import asyncio

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app.models.pipeline import (
    NodeDefinition,
    NodeType,
    PipelineDefinition,
)
from app.services.cache_keys import compute_cache_keys
from app.services.connection_resolution import (
    resolve_node_connections as _resolve_node_connections,
    resolve_pipeline_connections as _resolve_pipeline_connections,
)
from app.services.execution_registry import ExecutionCancelled
from app.services.pipeline_engine import PipelineEngine, compute_upstream_resolution
from app.storage.pipeline_store import PipelineStore

router = APIRouter()


def get_store() -> PipelineStore:
    return PipelineStore()


def _get_engine(request: Request, pipeline: PipelineDefinition) -> PipelineEngine:
    settings = pipeline.settings
    manager = request.app.state.project_dbs.get(
        pipeline.id,
        settings={"duckdb_memory_limit": settings.duckdb_memory_limit},
    )
    return PipelineEngine(
        manager,
        request.app.state.csv_preprocess_artifacts,
        connection_pools=getattr(request.app.state, "connection_pools", None),
        max_concurrent_nodes=settings.max_concurrent_nodes,
        max_connections_per_database=settings.max_connections_per_database,
        use_postgres_attach=True,
    )


def _get_registry(request: Request):
    return request.app.state.execution_registry


class NodeExecutionRequest(BaseModel):
    pipeline: PipelineDefinition
    node_id: str
    force: bool = False


def _resolved_node_and_key(
    payload: NodeExecutionRequest, store: PipelineStore
) -> tuple[NodeDefinition, str | None, PipelineDefinition, dict[str, str]]:
    """Resolve connections across the pipeline and compute the target node's
    cache key (transforms need their upstreams' keys). Also returns the resolved
    pipeline and full cache-key map so callers can resolve upstream reads."""
    pipeline = _resolve_pipeline_connections(payload.pipeline, store)
    node_map = {n.id: n for n in pipeline.nodes}
    node = node_map.get(payload.node_id)
    if node is None:
        raise HTTPException(status_code=404, detail="Node not found in pipeline")
    cache_keys = compute_cache_keys(pipeline)
    return node, cache_keys.get(payload.node_id), pipeline, cache_keys


@router.post("/pipeline/start")
async def start_pipeline_execution(pipeline: PipelineDefinition, request: Request, force: bool = False):
    store = get_store()
    pipeline = _resolve_pipeline_connections(pipeline, store)
    engine = _get_engine(request, pipeline)
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
    node, cache_key, resolved_pipeline, cache_keys = _resolved_node_and_key(payload, store)
    engine = _get_engine(request, payload.pipeline)
    upstream_resolution = await asyncio.to_thread(
        compute_upstream_resolution, resolved_pipeline, node.id, cache_keys, engine.duckdb
    )
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
                upstream_resolution=upstream_resolution,
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
    """Per-node, per-location cache state: does each persisted copy
    (in_memory / materialized) still match what the node would produce today?
    A node can hold both copies, tracked independently."""
    store = get_store()
    manager = request.app.state.project_dbs.get(pipeline.id)
    all_locations = manager.all_node_locations()

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
        per_location = {
            location: _location_status(
                meta, manager, cache_keys.get(node.id), node.id in unresolvable
            )
            for location, meta in all_locations.get(node.id, {}).items()
        }
        preferred = _preferred_present(per_location)
        summary = per_location.get(preferred) if preferred else None
        statuses[node.id] = {
            "locations": per_location,
            "lifecycle": _derive_lifecycle(per_location),
            # Back-compat single-copy fields still read by the canvas node chip.
            "state": summary["state"] if summary else "missing",
            "location": preferred if summary and summary["present"] else None,
            "row_count": summary["row_count"] if summary else None,
            "column_count": summary["column_count"] if summary else None,
            "finished_at": summary["finished_at"] if summary else None,
            "error": next(
                (s["error"] for s in per_location.values() if s.get("error")), None
            ),
        }
    return {"nodes": statuses}


def _location_status(meta: dict, manager, node_cache_key: str | None, unresolvable: bool) -> dict:
    """Presence + freshness of one (node, location) copy. An in-memory table is
    gone after a restart (scratch is RAM-only), so a 'complete' row doesn't
    guarantee the table is still there."""
    location = meta["location"]
    table_present = bool(
        meta["status"] == "complete"
        and manager.table_exists(meta["table_name"], location=location)
    )
    if meta["status"] == "loading":
        state = "loading"
    elif meta["status"] == "failed":
        state = "failed"
    elif not table_present:
        state = "missing"
    elif unresolvable:
        state = "stale"
    elif meta["cache_key"] == node_cache_key:
        state = "fresh"
    else:
        state = "stale"
    return {
        "present": table_present,
        "state": state,
        "row_count": meta["row_count"],
        "column_count": meta["column_count"],
        "finished_at": meta["finished_at"],
        "error": meta["error"],
    }


def _preferred_present(per_location: dict[str, dict]) -> str | None:
    """The location the single-value summary reports: a present copy wins
    (in_memory over materialized); else any recorded location."""
    for location in ("in_memory", "materialized"):
        status = per_location.get(location)
        if status and status["present"]:
            return location
    for location in ("in_memory", "materialized"):
        if location in per_location:
            return location
    return None


def _derive_lifecycle(per_location: dict[str, dict]) -> str:
    """Single compact card label projected over a node's location copies."""
    if not per_location:
        return "new"
    states = [status["state"] for status in per_location.values()]
    if "loading" in states:
        return "running"
    for location in ("in_memory", "materialized"):
        status = per_location.get(location)
        if status and status["present"]:
            return location
    if states and all(state == "failed" for state in states):
        return "error"
    return "idle"


@router.post("/pipeline/{pipeline_id}")
async def execute_pipeline(pipeline_id: str, request: Request, force: bool = False):
    try:
        store = get_store()
        pipeline = store.load(pipeline_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline = _resolve_pipeline_connections(pipeline, store)
    engine = _get_engine(request, pipeline)
    results = await engine.execute_pipeline(pipeline, force_refresh=force)
    return results


@router.post("/pipeline")
async def execute_pipeline_inline(pipeline: PipelineDefinition, request: Request, force: bool = False):
    pipeline = _resolve_pipeline_connections(pipeline, get_store())
    engine = _get_engine(request, pipeline)
    results = await engine.execute_pipeline(pipeline, force_refresh=force)
    return results


@router.post("/node")
async def execute_node(payload: NodeExecutionRequest, request: Request):
    store = get_store()
    node, cache_key, resolved_pipeline, cache_keys = _resolved_node_and_key(payload, store)
    engine = _get_engine(request, payload.pipeline)
    upstream_resolution = await asyncio.to_thread(
        compute_upstream_resolution, resolved_pipeline, node.id, cache_keys, engine.duckdb
    )
    result = await engine.execute_single_node(
        node, cache_key=cache_key, force_refresh=payload.force, upstream_resolution=upstream_resolution
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
