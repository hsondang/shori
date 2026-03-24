import sqlite3
from datetime import datetime, timezone

from app.config import PROJECT_DB_PATH
from app.models.pipeline import PipelineDefinition, ProjectSummary


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class PipelineStore:
    def __init__(self):
        self.db_path = PROJECT_DB_PATH
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS projects (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    pipeline_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

    def save(self, pipeline: PipelineDefinition):
        now = _utc_now()
        pipeline_json = pipeline.model_dump_json(indent=2)

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO projects (id, name, pipeline_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    pipeline_json = excluded.pipeline_json,
                    updated_at = excluded.updated_at
                """,
                (pipeline.id, pipeline.name, pipeline_json, now, now),
            )

    def load(self, pipeline_id: str) -> PipelineDefinition:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT pipeline_json FROM projects WHERE id = ?",
                (pipeline_id,),
            ).fetchone()

        if row is None:
            raise FileNotFoundError(f"Pipeline {pipeline_id} not found")

        return PipelineDefinition.model_validate_json(row["pipeline_json"])

    def list_all(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, name, created_at, updated_at
                FROM projects
                ORDER BY updated_at DESC, id ASC
                """
            ).fetchall()

        return [ProjectSummary.model_validate(dict(row)).model_dump() for row in rows]

    def delete(self, pipeline_id: str):
        with self._connect() as conn:
            conn.execute("DELETE FROM projects WHERE id = ?", (pipeline_id,))
