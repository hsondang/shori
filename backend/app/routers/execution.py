from fastapi import APIRouter, HTTPException, Request

from app.models.pipeline import NodeDefinition, PipelineDefinition
from app.services.pipeline_engine import PipelineEngine
from app.storage.pipeline_store import PipelineStore

router = APIRouter()
store = PipelineStore()


def _get_engine(request: Request) -> PipelineEngine:
    return PipelineEngine(
        request.app.state.duckdb,
        request.app.state.csv_preprocess_artifacts,
    )


@router.post("/pipeline/{pipeline_id}")
async def execute_pipeline(pipeline_id: str, request: Request, force: bool = False):
    try:
        pipeline = store.load(pipeline_id)
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
