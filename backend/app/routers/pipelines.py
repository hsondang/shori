from fastapi import APIRouter, HTTPException

from app.models.pipeline import PipelineDefinition
from app.storage.pipeline_store import PipelineStore

router = APIRouter()
store = PipelineStore()


@router.get("")
def list_pipelines():
    return store.list_all()


@router.get("/{pipeline_id}")
def get_pipeline(pipeline_id: str):
    try:
        return store.load(pipeline_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Pipeline not found")


@router.post("")
def create_pipeline(pipeline: PipelineDefinition):
    store.save(pipeline)
    return {"id": pipeline.id}


@router.put("/{pipeline_id}")
def update_pipeline(pipeline_id: str, pipeline: PipelineDefinition):
    pipeline.id = pipeline_id
    store.save(pipeline)
    return {"id": pipeline.id}


@router.delete("/{pipeline_id}")
def delete_pipeline(pipeline_id: str):
    store.delete(pipeline_id)
    return {"ok": True}
