import asyncio

from fastapi import APIRouter, HTTPException, Request

from app.models.pipeline import NodeDefinition, PipelineDefinition
from app.services.execution_registry import ExecutionCancelled
from app.services.pipeline_engine import PipelineEngine
from app.storage.pipeline_store import PipelineStore

router = APIRouter()


def get_store() -> PipelineStore:
    return PipelineStore()


def _get_engine(request: Request) -> PipelineEngine:
    return PipelineEngine(
        request.app.state.duckdb,
        request.app.state.csv_preprocess_artifacts,
    )


def _get_registry(request: Request):
    return request.app.state.execution_registry


@router.post("/pipeline/start")
async def start_pipeline_execution(pipeline: PipelineDefinition, request: Request, force: bool = False):
    engine = _get_engine(request)
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
async def start_node_execution(node: NodeDefinition, request: Request):
    engine = _get_engine(request)
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


@router.post("/pipeline/{pipeline_id}")
async def execute_pipeline(pipeline_id: str, request: Request, force: bool = False):
    try:
        pipeline = get_store().load(pipeline_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    engine = _get_engine(request)
    results = await engine.execute_pipeline(pipeline, force_refresh=force)
    return results


@router.post("/pipeline")
async def execute_pipeline_inline(pipeline: PipelineDefinition, request: Request, force: bool = False):
    engine = _get_engine(request)
    results = await engine.execute_pipeline(pipeline, force_refresh=force)
    return results


@router.post("/node")
async def execute_node(node: NodeDefinition, request: Request):
    engine = _get_engine(request)
    result = await engine.execute_single_node(node)
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
