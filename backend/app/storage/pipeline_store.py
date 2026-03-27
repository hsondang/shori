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
                    starred INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(projects)").fetchall()
            }
            if "starred" not in columns:
                conn.execute(
                    "ALTER TABLE projects ADD COLUMN starred INTEGER NOT NULL DEFAULT 0"
                )

    def save(self, pipeline: PipelineDefinition):
        now = _utc_now()
        pipeline_json = pipeline.model_dump_json(indent=2)

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO projects (id, name, pipeline_json, starred, created_at, updated_at)
                VALUES (?, ?, ?, 0, ?, ?)
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
                SELECT id, name, starred, created_at, updated_at
                FROM projects
                ORDER BY starred DESC, updated_at DESC, id ASC
                """
            ).fetchall()

        return [ProjectSummary.model_validate(dict(row)).model_dump() for row in rows]

    def delete(self, pipeline_id: str):
        with self._connect() as conn:
            conn.execute("DELETE FROM projects WHERE id = ?", (pipeline_id,))

    def update_star(self, pipeline_id: str, starred: bool) -> bool:
        with self._connect() as conn:
            result = conn.execute(
                "UPDATE projects SET starred = ? WHERE id = ?",
                (1 if starred else 0, pipeline_id),
            )
        return result.rowcount > 0
