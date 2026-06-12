import asyncio
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app.models.pipeline import (
    NodeExecutionResult,
    NodeStatus,
    NodeType,
    PipelineDefinition,
)
from app.services.cache_keys import compute_cache_keys
from app.services.connection_resolution import resolve_pipeline_connections
from app.services.execution_registry import ExecutionCancelled
from app.services.preview_sessions import PreviewSessionNotFound
from app.storage.pipeline_store import PipelineStore

router = APIRouter()


class PreviewSessionStartRequest(BaseModel):
    pipeline: PipelineDefinition
    node_id: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_sessions(request: Request):
    return request.app.state.preview_sessions


@router.post("/start")
async def start_preview_session(payload: PreviewSessionStartRequest, request: Request):
    store = PipelineStore()
    pipeline = resolve_pipeline_connections(payload.pipeline, store)
    node_map = {n.id: n for n in pipeline.nodes}
    node = node_map.get(payload.node_id)
    if node is None:
        raise HTTPException(status_code=404, detail="Node not found in pipeline")
    if node.type != NodeType.DB_SOURCE:
        raise HTTPException(status_code=400, detail="Live preview is only available for database source nodes")

    cache_key = compute_cache_keys(pipeline).get(node.id)
    settings = pipeline.settings
    # Touch the project db so the project id is validated before we connect.
    request.app.state.project_dbs.get(pipeline.id)
    try:
        return await _get_sessions(request).start(
            project_id=pipeline.id,
            node=node,
            cache_key=cache_key,
            chunk_rows=settings.preview_chunk_rows,
            max_buffer_rows=settings.preview_max_buffer_rows,
            ttl_seconds=settings.preview_session_ttl_seconds,
            max_connections_per_database=settings.max_connections_per_database,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Unable to start preview: {exc}") from exc


@router.post("/{session_id}/fetch")
async def fetch_preview_rows(session_id: str, request: Request):
    try:
        return await _get_sessions(request).fetch_more(session_id)
    except PreviewSessionNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Unable to fetch preview rows: {exc}") from exc


@router.post("/{session_id}/materialize")
async def materialize_preview_session(session_id: str, request: Request):
    """Turn the live preview into the node's persisted table: buffered rows
    stream in first, then the open cursor is drained to completion. Returns
    an execution-run snapshot so the node badge tracks progress."""
    sessions = _get_sessions(request)
    try:
        session = await sessions.get_session(session_id)
    except PreviewSessionNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    manager = request.app.state.project_dbs.get(session.project_id)
    registry = request.app.state.execution_registry
    node = session.node
    run = registry.create_run("node", [node.id])
    controller = registry.create_controller(run.execution_id)

    def register_interrupt(interrupt):
        controller.set_abort_callback(node.id, interrupt)

    async def runner() -> None:
        started_at = _utc_now_iso()
        registry.mark_node_running(run.execution_id, node.id, started_at)
        try:
            stats = await sessions.materialize(
                session_id,
                manager,
                register_interrupt=register_interrupt,
            )
            registry.set_node_result(
                run.execution_id,
                NodeExecutionResult(
                    node_id=node.id,
                    status=NodeStatus.SUCCESS,
                    row_count=stats["row_count"],
                    column_count=stats["column_count"],
                    columns=stats["columns"],
                    started_at=started_at,
                    finished_at=_utc_now_iso(),
                ),
            )
            registry.finalize_run(run.execution_id)
        except (asyncio.CancelledError, ExecutionCancelled):
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

    task = asyncio.create_task(runner())
    registry.attach_task(run.execution_id, task)
    await asyncio.sleep(0)

    snapshot = registry.get_run(run.execution_id)
    if snapshot is None:
        raise HTTPException(status_code=500, detail="Materialize run disappeared before it could be tracked")
    return snapshot


@router.delete("/{session_id}")
async def close_preview_session(session_id: str, request: Request):
    closed = await _get_sessions(request).close(session_id)
    return {"closed": closed}
