import asyncio

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app.models.pipeline import (
    DatabaseConnectionDefinition,
    NodeDefinition,
    NodeType,
    PipelineDefinition,
)
from app.services.cache_keys import compute_cache_keys
from app.services.execution_registry import ExecutionCancelled
from app.services.pipeline_engine import PipelineEngine
from app.storage.pipeline_store import PipelineStore

router = APIRouter()


def get_store() -> PipelineStore:
    return PipelineStore()


def _get_engine(request: Request, project_id: str) -> PipelineEngine:
    manager = request.app.state.project_dbs.get(project_id)
    return PipelineEngine(
        manager,
        request.app.state.csv_preprocess_artifacts,
    )


def _get_registry(request: Request):
    return request.app.state.execution_registry


class NodeExecutionRequest(BaseModel):
    pipeline: PipelineDefinition
    node_id: str
    force: bool = False


def _saved_connection_to_config(connection: DatabaseConnectionDefinition) -> dict:
    if connection.db_type == "oracle":
        return {
            "host": connection.host,
            "port": connection.port,
            "service_name": connection.service_name,
            "user": connection.user,
            "password": connection.password,
        }

    return {
        "host": connection.host,
        "port": connection.port,
        "database": connection.database,
        "user": connection.user,
        "password": connection.password,
    }


def _resolve_node_connections(node: NodeDefinition, store: PipelineStore) -> NodeDefinition:
    config = dict(node.config)
    if node.type != "db_source" or config.get("connection_mode") != "global":
        return node

    connection_source_id = config.get("connection_source_id")
    if not isinstance(connection_source_id, str) or not connection_source_id:
        raise HTTPException(status_code=400, detail="Global database source is missing connection_source_id")

    try:
        connection = store.load_global_connection(connection_source_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Global database connection not found")

    return node.model_copy(update={
        "config": {
            **config,
            "db_type": connection.db_type,
            "connection": _saved_connection_to_config(connection),
        }
    })


def _resolve_pipeline_connections(pipeline: PipelineDefinition, store: PipelineStore) -> PipelineDefinition:
    return pipeline.model_copy(update={
        "nodes": [_resolve_node_connections(node, store) for node in pipeline.nodes],
    })


def _resolved_node_and_key(
    payload: NodeExecutionRequest, store: PipelineStore
) -> tuple[NodeDefinition, str | None]:
    """Resolve connections across the pipeline and compute the target node's
    cache key (transforms need their upstreams' keys)."""
    pipeline = _resolve_pipeline_connections(payload.pipeline, store)
    node_map = {n.id: n for n in pipeline.nodes}
    node = node_map.get(payload.node_id)
    if node is None:
        raise HTTPException(status_code=404, detail="Node not found in pipeline")
    cache_keys = compute_cache_keys(pipeline)
    return node, cache_keys.get(payload.node_id)


@router.post("/pipeline/start")
async def start_pipeline_execution(pipeline: PipelineDefinition, request: Request, force: bool = False):
    store = get_store()
    pipeline = _resolve_pipeline_connections(pipeline, store)
    engine = _get_engine(request, pipeline.id)
    registry = _get_registry(request)
    run = registry.create_run("pipeline", [node.id for node in pipeline.nodes])
    execution_controller = registry.create_controller(run.execution_id)
    started = asyncio.Event()

    def on_node_start(node_id: str, started_at: str) -> None:
        registry.mark_node_running(run.execution_id, node_id, started_at)
        if not started.is_set():
            started.set()

    def on_node_finish(result) -> None:
        registry.set_node_result(run.execution_id, result)

    def on_node_update(result) -> None:
        registry.update_node_result(run.execution_id, result)

    async def runner() -> None:
        try:
            await engine.execute_pipeline(
                pipeline,
                force_refresh=force,
                on_node_start=on_node_start,
                on_node_finish=on_node_finish,
                on_node_update=on_node_update,
                execution_controller=execution_controller,
            )
            registry.finalize_run(run.execution_id)
        except (asyncio.CancelledError, ExecutionCancelled):
            pass
        except Exception as exc:
            registry.fail_run(run.execution_id, str(exc))
        finally:
            if not started.is_set():
                started.set()

    task = asyncio.create_task(runner())
    registry.attach_task(run.execution_id, task)
    await started.wait()
    await asyncio.sleep(0)

    snapshot = registry.get_run(run.execution_id)
    if snapshot is None:
        raise HTTPException(status_code=500, detail="Execution run disappeared before it could be tracked")
    return snapshot


@router.post("/node/start")
async def start_node_execution(payload: NodeExecutionRequest, request: Request):
    store = get_store()
    node, cache_key = _resolved_node_and_key(payload, store)
    engine = _get_engine(request, payload.pipeline.id)
    registry = _get_registry(request)
    run = registry.create_run("node", [node.id])
    execution_controller = registry.create_controller(run.execution_id)
    started = asyncio.Event()

    def on_node_start(node_id: str, started_at: str) -> None:
        registry.mark_node_running(run.execution_id, node_id, started_at)
        if not started.is_set():
            started.set()

    def on_node_finish(result) -> None:
        registry.set_node_result(run.execution_id, result)

    def on_node_update(result) -> None:
        registry.update_node_result(run.execution_id, result)

    async def runner() -> None:
        try:
            await engine.execute_single_node(
                node,
                cache_key=cache_key,
                force_refresh=payload.force,
                on_node_start=on_node_start,
                on_node_finish=on_node_finish,
                on_node_update=on_node_update,
                execution_controller=execution_controller,
            )
            registry.finalize_run(run.execution_id)
        except (asyncio.CancelledError, ExecutionCancelled):
            pass
        except Exception as exc:
            registry.fail_run(run.execution_id, str(exc))
        finally:
            if not started.is_set():
                started.set()

    task = asyncio.create_task(runner())
    registry.attach_task(run.execution_id, task)
    await started.wait()
    await asyncio.sleep(0)

    snapshot = registry.get_run(run.execution_id)
    if snapshot is None:
        raise HTTPException(status_code=500, detail="Execution run disappeared before it could be tracked")
    return snapshot


@router.post("/cache-status")
async def get_cache_status(pipeline: PipelineDefinition, request: Request):
    """Per-node cache state for the given pipeline definition: does the
    persisted table still match what the node would produce today?"""
    store = get_store()
    manager = request.app.state.project_dbs.get(pipeline.id)
    metas = manager.all_node_meta()

    resolved_nodes = []
    unresolvable: set[str] = set()
    for node in pipeline.nodes:
        try:
            resolved_nodes.append(_resolve_node_connections(node, store))
        except HTTPException:
            # A missing global connection shouldn't fail the whole status
            # call; the node simply can't be fresh.
            unresolvable.add(node.id)
            resolved_nodes.append(node)
    resolved = pipeline.model_copy(update={"nodes": resolved_nodes})
    cache_keys = compute_cache_keys(resolved)

    statuses: dict[str, dict] = {}
    for node in resolved.nodes:
        if node.type == NodeType.EXPORT:
            continue
        meta = metas.get(node.id)
        if meta is None:
            state = "missing"
        elif node.id in unresolvable:
            state = "stale"
        elif meta["status"] == "loading":
            state = "loading"
        elif meta["status"] == "failed":
            state = "failed"
        elif meta["cache_key"] == cache_keys.get(node.id):
            state = "fresh"
        else:
            state = "stale"
        statuses[node.id] = {
            "state": state,
            "row_count": meta["row_count"] if meta else None,
            "column_count": meta["column_count"] if meta else None,
            "finished_at": meta["finished_at"] if meta else None,
            "error": meta["error"] if meta else None,
        }
    return {"nodes": statuses}


@router.post("/pipeline/{pipeline_id}")
async def execute_pipeline(pipeline_id: str, request: Request, force: bool = False):
    try:
        store = get_store()
        pipeline = store.load(pipeline_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline = _resolve_pipeline_connections(pipeline, store)
    engine = _get_engine(request, pipeline.id)
    results = await engine.execute_pipeline(pipeline, force_refresh=force)
    return results


@router.post("/pipeline")
async def execute_pipeline_inline(pipeline: PipelineDefinition, request: Request, force: bool = False):
    pipeline = _resolve_pipeline_connections(pipeline, get_store())
    engine = _get_engine(request, pipeline.id)
    results = await engine.execute_pipeline(pipeline, force_refresh=force)
    return results


@router.post("/node")
async def execute_node(payload: NodeExecutionRequest, request: Request):
    store = get_store()
    node, cache_key = _resolved_node_and_key(payload, store)
    engine = _get_engine(request, payload.pipeline.id)
    result = await engine.execute_single_node(
        node, cache_key=cache_key, force_refresh=payload.force
    )
    return result


@router.get("/runs/{execution_id}")
async def get_execution_run_status(execution_id: str, request: Request):
    run = _get_registry(request).get_run(execution_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Execution run not found")
    return run


@router.post("/runs/{execution_id}/abort")
async def abort_execution_run(execution_id: str, request: Request):
    run = _get_registry(request).abort_run(execution_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Execution run not found")
    return run
