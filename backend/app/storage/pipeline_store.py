import json
from pathlib import Path

from app.config import PIPELINE_DIR
from app.models.pipeline import PipelineDefinition


class PipelineStore:
    def save(self, pipeline: PipelineDefinition):
        path = PIPELINE_DIR / f"{pipeline.id}.json"
        path.write_text(pipeline.model_dump_json(indent=2))

    def load(self, pipeline_id: str) -> PipelineDefinition:
        path = PIPELINE_DIR / f"{pipeline_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"Pipeline {pipeline_id} not found")
        return PipelineDefinition.model_validate_json(path.read_text())

    def list_all(self) -> list[dict]:
        results = []
        for p in PIPELINE_DIR.glob("*.json"):
            data = json.loads(p.read_text())
            results.append({"id": data["id"], "name": data["name"]})
        return results

    def delete(self, pipeline_id: str):
        path = PIPELINE_DIR / f"{pipeline_id}.json"
        path.unlink(missing_ok=True)
