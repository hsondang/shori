from fastapi import APIRouter, HTTPException

from app.models.pipeline import PipelineDefinition, ProjectStarUpdate
from app.storage.pipeline_store import PipelineStore

router = APIRouter()


def get_store() -> PipelineStore:
    return PipelineStore()


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
def create_pipeline(pipeline: PipelineDefinition):
    get_store().save(pipeline)
    return {"id": pipeline.id}


@router.put("/{pipeline_id}")
def update_pipeline(pipeline_id: str, pipeline: PipelineDefinition):
    pipeline.id = pipeline_id
    get_store().save(pipeline)
    return {"id": pipeline.id}


@router.delete("/{pipeline_id}")
def delete_pipeline(pipeline_id: str):
    get_store().delete(pipeline_id)
    return {"ok": True}


@router.patch("/{pipeline_id}/star")
def update_pipeline_star(pipeline_id: str, payload: ProjectStarUpdate):
    updated = get_store().update_star(pipeline_id, payload.starred)
    if not updated:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return {"id": pipeline_id, "starred": payload.starred}
