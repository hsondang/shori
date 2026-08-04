import asyncio
import csv
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.config import EXPORT_DIR
from app.models.pipeline import (
    NodeExecutionResult,
    NodeStatus,
    NodeType,
    PipelineDefinition,
)
from app.services.cache_keys import compute_cache_keys
from app.services.csv_service import (
    CSV_PREVIEW_LIMIT,
    preview_csv_text,
    preview_preprocessed_csv_text,
)
from app.services.database_export import (
    DatabaseExportError,
    ExportAborted,
    append_query_to_table,
    effective_export_sql,
    export_connection_config,
    parse_target_table,
    resolve_export_connection,
    validate_export,
)
from app.services.duckdb_manager import is_reserved_table_name
from app.services.execution_registry import ExecutionCancelled
from app.services.export_service import export_table_to_ai_workspace, export_table_to_local
from app.services.oracle_service import OracleService
from app.services.pipeline_graph import resolve_direct_upstreams, upstream_table_name
from app.storage.pipeline_store import PipelineStore

router = APIRouter()


class ExportToPathRequest(BaseModel):
    table_name: str
    output_path: str
    format: str = "csv"


class ExportToAiRequest(BaseModel):
    table_name: str
    source_node_id: str | None = None


class ExportToDatabaseRequest(BaseModel):
    """The whole pipeline comes along so the export reads the same copy of each
    upstream the preview would (see resolve_direct_upstreams)."""

    pipeline: PipelineDefinition
    node_id: str


class CsvSourcePreviewRequest(BaseModel):
    file_path: str
    limit: int = Field(default=CSV_PREVIEW_LIMIT, ge=1, le=CSV_PREVIEW_LIMIT)


class CsvSourcePreprocessedPreviewRequest(CsvSourcePreviewRequest):
    node_id: str
    preprocessing: dict


@router.post("/preview/csv-source")
def preview_csv_source(payload: CsvSourcePreviewRequest):
    if not Path(payload.file_path).exists():
        raise HTTPException(status_code=404, detail=f"CSV file '{payload.file_path}' not found")
    try:
        return preview_csv_text(payload.file_path, limit=payload.limit, stage="raw")
    except (csv.Error, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"Unable to preview CSV: {exc}") from exc


@router.post("/preview/csv-source/preprocessed")
def preview_preprocessed_csv_source(payload: CsvSourcePreprocessedPreviewRequest, request: Request):
    if not Path(payload.file_path).exists():
        raise HTTPException(status_code=404, detail=f"CSV file '{payload.file_path}' not found")
    try:
        return preview_preprocessed_csv_text(
            request.app.state.csv_preprocess_artifacts,
            payload.node_id,
            payload.file_path,
            payload.preprocessing,
            limit=payload.limit,
        )
    except (csv.Error, ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=f"Unable to preview CSV: {exc}") from exc


@router.delete("/preview/csv-source/preprocessed/{node_id}")
def delete_preprocessed_csv_source(node_id: str, request: Request):
    deleted = request.app.state.csv_preprocess_artifacts.invalidate(node_id)
    return {"deleted": deleted}


def _get_project_db(request: Request, project_id: str):
    try:
        return request.app.state.project_dbs.get(project_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _require_user_table(db, table_name: str):
    if is_reserved_table_name(table_name) or not db.table_exists(table_name):
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")


@router.get("/{project_id}/preview/{table_name}")
def preview_data(project_id: str, table_name: str, request: Request, offset: int = 0, limit: int = 100):
    db = _get_project_db(request, project_id)
    _require_user_table(db, table_name)
    return db.preview(table_name, offset=offset, limit=limit)


@router.get("/{project_id}/export/{table_name}")
def export_data(project_id: str, table_name: str, request: Request):
    db = _get_project_db(request, project_id)
    _require_user_table(db, table_name)

    output_path = EXPORT_DIR / f"{table_name}.csv"
    db.export_to_csv(table_name, str(output_path))
    return FileResponse(
        path=str(output_path),
        filename=f"{table_name}.csv",
        media_type="text/csv",
    )


@router.post("/{project_id}/export-to-path")
def export_to_path(project_id: str, payload: ExportToPathRequest, request: Request):
    db = _get_project_db(request, project_id)
    try:
        return export_table_to_local(db, payload.table_name, payload.output_path, payload.format)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{project_id}/export-to-ai")
def export_to_ai(project_id: str, payload: ExportToAiRequest, request: Request):
    # AI workspace touchpoint #2 (docs/ai-workspace-model.md §5): drop a
    # parquet clone in the spool; the /ai sub-app ingests it independently.
    db = _get_project_db(request, project_id)
    try:
        return export_table_to_ai_workspace(db, project_id, payload.table_name, payload.source_node_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _prepare_database_export(payload: ExportToDatabaseRequest, request: Request):
    """Everything both the export and the validate endpoint need up front.

    Returns (node, sql, resolution, connection_config, schema, table). Raises
    HTTPException for anything the user has to fix.
    """
    pipeline = payload.pipeline
    node = next((n for n in pipeline.nodes if n.id == payload.node_id), None)
    if node is None:
        raise HTTPException(status_code=404, detail="Node not found in pipeline")
    config = node.config or {}
    if node.type != NodeType.EXPORT or config.get("destination") != "database":
        raise HTTPException(
            status_code=400, detail="This node is not configured to export to a database"
        )

    try:
        # Permission is re-checked here, not just in the UI: a node config is
        # persisted JSON, so a stale or hand-edited one must not be able to
        # write to a database whose approval was never granted or was revoked.
        connection = resolve_export_connection(config.get("connection_source_id"), PipelineStore())
        schema, table = parse_target_table(config.get("target_table"))
        sql = effective_export_sql(node, upstream_table_name(pipeline, node.id))
    except DatabaseExportError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    manager = _get_project_db(request, pipeline.id)
    cache_keys = compute_cache_keys(pipeline)
    resolution, missing = await asyncio.to_thread(
        resolve_direct_upstreams, pipeline, node.id, cache_keys, manager
    )
    if missing:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "upstreams_unavailable",
                "message": "Some upstream tables are not loaded or materialized yet.",
                "missing_tables": missing,
            },
        )

    return node, sql, resolution, export_connection_config(connection), schema, table, manager


@router.post("/{project_id}/export-to-database/validate")
async def validate_export_to_database(
    project_id: str, payload: ExportToDatabaseRequest, request: Request
):
    """Compare the export query's columns against the live target table.

    This is the only preview path that touches the destination database; the
    plain row preview stays inside DuckDB.
    """
    node, sql, resolution, conn_config, schema, table, manager = await _prepare_database_export(
        payload, request
    )
    settings = payload.pipeline.settings
    connection, release = await request.app.state.connection_pools.acquire(
        "oracle", conn_config, settings.max_connections_per_database
    )
    try:
        return await validate_export(manager, connection, sql, resolution, schema, table)
    except DatabaseExportError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Unable to validate export: {exc}") from exc
    finally:
        await release()


@router.post("/{project_id}/export-to-database")
async def export_to_database(
    project_id: str, payload: ExportToDatabaseRequest, request: Request
):
    """Append the export query's rows to the target table as a tracked run.

    Returns an execution-run snapshot immediately; the frontend polls
    /api/execute/runs/{id} and aborts through the usual endpoint, so a long
    load neither blocks a request nor dies with the config panel.
    """
    node, sql, resolution, conn_config, schema, table, manager = await _prepare_database_export(
        payload, request
    )
    settings = payload.pipeline.settings
    registry = request.app.state.execution_registry
    run = registry.create_run("node", [node.id])
    controller = registry.create_controller(run.execution_id)

    async def runner() -> None:
        started_at = _utc_now_iso()
        registry.mark_node_running(run.execution_id, node.id, started_at)
        connection = None
        release = None
        try:
            connection, release = await request.app.state.connection_pools.acquire(
                "oracle", conn_config, settings.max_connections_per_database
            )
            controller.raise_if_cancelled()
            # Abort has to reach the in-flight INSERT, not just the batch loop:
            # cancel() interrupts the round trip, then append_rows rolls back.
            controller.set_abort_callback(node.id, lambda: OracleService().abort_query(connection))

            def on_progress(rows_written: int) -> None:
                # Thread-safe: the registry is lock-guarded, so the export
                # thread can report straight into the run snapshot.
                registry.update_node_result(
                    run.execution_id,
                    NodeExecutionResult(
                        node_id=node.id,
                        status=NodeStatus.RUNNING,
                        row_count=rows_written,
                        started_at=started_at,
                    ),
                )

            result = await append_query_to_table(
                manager,
                connection,
                sql,
                resolution,
                schema,
                table,
                on_progress=on_progress,
                should_abort=controller.is_cancelled,
            )
            registry.set_node_result(
                run.execution_id,
                NodeExecutionResult(
                    node_id=node.id,
                    status=NodeStatus.SUCCESS,
                    row_count=result["row_count"],
                    column_count=len(result["columns"]),
                    columns=result["columns"],
                    started_at=started_at,
                    finished_at=_utc_now_iso(),
                ),
            )
            registry.finalize_run(run.execution_id)
        except (asyncio.CancelledError, ExecutionCancelled, ExportAborted):
            pass
        except Exception as exc:
            registry.set_node_result(
                run.execution_id,
                NodeExecutionResult(
                    node_id=node.id,
                    status=NodeStatus.ERROR,
                    error=str(exc),
                    started_at=started_at,
                    finished_at=_utc_now_iso(),
                ),
            )
            registry.fail_run(run.execution_id, str(exc))
        finally:
            controller.clear_abort_callback(node.id)
            if release is not None:
                await release()

    task = asyncio.create_task(runner())
    registry.attach_task(run.execution_id, task)
    await asyncio.sleep(0)

    snapshot = registry.get_run(run.execution_id)
    if snapshot is None:
        raise HTTPException(status_code=500, detail="Export run disappeared before it could be tracked")
    return snapshot


@router.get("/{project_id}/schema/{table_name}")
def get_schema(project_id: str, table_name: str, request: Request):
    db = _get_project_db(request, project_id)
    _require_user_table(db, table_name)

    preview = db.preview(table_name, offset=0, limit=0)
    return {
        "table_name": table_name,
        "columns": preview["columns"],
        "column_types": preview["column_types"],
        "total_rows": preview["total_rows"],
    }


@router.delete("/{project_id}/table/{table_name}")
def delete_table(project_id: str, table_name: str, request: Request):
    db = _get_project_db(request, project_id)
    if is_reserved_table_name(table_name):
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    existed = db.table_exists(table_name)
    db.drop_table(table_name)
    return {"deleted": existed}
