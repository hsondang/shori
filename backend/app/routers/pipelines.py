import logging

from fastapi import APIRouter, HTTPException, Request

from app.models.pipeline import NodeType, PipelineDefinition, ProjectStarUpdate
from app.services.duckdb_manager import ProjectBusyError, validate_user_table_name
from app.storage.pipeline_store import PipelineStore

logger = logging.getLogger(__name__)

router = APIRouter()


def get_store() -> PipelineStore:
    return PipelineStore()


def _validate_pipeline_tables(pipeline: PipelineDefinition) -> None:
    seen: dict[str, str] = {}
    for node in pipeline.nodes:
        if node.type == NodeType.EXPORT:
            continue
        try:
            validate_user_table_name(node.table_name)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        other = seen.get(node.table_name)
        if other is not None:
            raise HTTPException(
                status_code=400,
                detail=f"Nodes '{other}' and '{node.id}' both use table name "
                f"'{node.table_name}'; table names must be unique within a project.",
            )
        seen[node.table_name] = node.id


def _reconcile_project_storage(request: Request, pipeline: PipelineDefinition) -> None:
    """Sync the project's DuckDB with the saved definition: drop tables for
    nodes that no longer exist (eager drop semantics) and rename tables for
    nodes whose table_name changed, preserving their cached data."""
    try:
        manager = request.app.state.project_dbs.get(pipeline.id)
    except ValueError:
        return
    current_nodes = {node.id: node for node in pipeline.nodes}
    for node_id, meta in manager.all_node_meta().items():
        node = current_nodes.get(node_id)
        if node is None:
            manager.drop_node(node_id)
            continue
        if node.type != NodeType.EXPORT and meta["table_name"] != node.table_name:
            try:
                manager.rename_node_table(node_id, node.table_name)
            except Exception:
                # A failed rename (e.g. name collision) must not leave a
                # cache entry pointing at the wrong table.
                logger.warning(
                    "Failed to rename table for node %s; dropping its cache.",
                    node_id,
                    exc_info=True,
                )
                manager.drop_node(node_id)


@router.get("")
def list_pipelines():
    return get_store().list_all()


@router.get("/{pipeline_id}")
def get_pipeline(pipeline_id: str):
    try:
        return get_store().load(pipeline_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Pipeline not found")


@router.post("")
def create_pipeline(pipeline: PipelineDefinition, request: Request):
    _validate_pipeline_tables(pipeline)
    get_store().save(pipeline)
    _reconcile_project_storage(request, pipeline)
    return {"id": pipeline.id}


@router.put("/{pipeline_id}")
def update_pipeline(pipeline_id: str, pipeline: PipelineDefinition, request: Request):
    pipeline.id = pipeline_id
    _validate_pipeline_tables(pipeline)
    get_store().save(pipeline)
    _reconcile_project_storage(request, pipeline)
    return {"id": pipeline.id}


@router.delete("/{pipeline_id}")
def delete_pipeline(pipeline_id: str, request: Request):
    get_store().delete(pipeline_id)
    try:
        request.app.state.project_dbs.close_and_delete(pipeline_id)
    except ValueError:
        pass  # invalid id can't have a project dir
    return {"ok": True}


@router.patch("/{pipeline_id}/star")
def update_pipeline_star(pipeline_id: str, payload: ProjectStarUpdate):
    updated = get_store().update_star(pipeline_id, payload.starred)
    if not updated:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return {"id": pipeline_id, "starred": payload.starred}


@router.get("/{pipeline_id}/storage")
def get_project_storage(pipeline_id: str, request: Request):
    try:
        manager = request.app.state.project_dbs.get(pipeline_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return manager.storage_info()


@router.post("/{pipeline_id}/compact")
def compact_project_storage(pipeline_id: str, request: Request):
    try:
        manager = request.app.state.project_dbs.get(pipeline_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        return manager.compact()
    except ProjectBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
