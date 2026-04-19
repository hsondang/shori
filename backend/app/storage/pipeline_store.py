import sqlite3
import json
from datetime import datetime, timezone
from uuid import uuid4

from app.config import PROJECT_DB_PATH
from app.models.pipeline import (
    DatabaseConnectionDefinition,
    DatabaseConnectionInputDefinition,
    PipelineDefinition,
    ProjectSummary,
)
from pydantic import TypeAdapter


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class SavedConnectionNameConflictError(ValueError):
    pass


class GlobalConnectionInUseError(ValueError):
    def __init__(self, project_names: list[str]):
        self.project_names = project_names
        joined = ", ".join(project_names[:3])
        suffix = "..." if len(project_names) > 3 else ""
        super().__init__(
            f"Global database connection is still used by project(s): {joined}{suffix}"
        )


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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS global_database_connections (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL COLLATE NOCASE UNIQUE,
                    db_type TEXT NOT NULL,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    database_name TEXT,
                    service_name TEXT,
                    user TEXT NOT NULL,
                    password TEXT NOT NULL,
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

    def _saved_connection_row_to_model(
        self, row: sqlite3.Row
    ) -> DatabaseConnectionDefinition:
        data = {
            "id": row["id"],
            "name": row["name"],
            "db_type": row["db_type"],
            "host": row["host"],
            "port": row["port"],
            "user": row["user"],
            "password": row["password"],
        }
        if row["db_type"] == "oracle":
            data["service_name"] = row["service_name"]
        else:
            data["database"] = row["database_name"]
        return TypeAdapter(DatabaseConnectionDefinition).validate_python(data)

    def _normalize_saved_connection_input(
        self, connection: DatabaseConnectionInputDefinition | dict
    ) -> DatabaseConnectionInputDefinition:
        return TypeAdapter(DatabaseConnectionInputDefinition).validate_python(connection)

    def _saved_connection_params(
        self, connection: DatabaseConnectionInputDefinition
    ) -> tuple[str | None, str | None]:
        if connection.db_type == "oracle":
            return None, connection.service_name
        return connection.database, None

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

    def list_global_connections(self) -> list[DatabaseConnectionDefinition]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, name, db_type, host, port, database_name, service_name, user, password
                FROM global_database_connections
                ORDER BY name COLLATE NOCASE ASC, id ASC
                """
            ).fetchall()
        return [self._saved_connection_row_to_model(row) for row in rows]

    def load_global_connection(self, connection_id: str) -> DatabaseConnectionDefinition:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, name, db_type, host, port, database_name, service_name, user, password
                FROM global_database_connections
                WHERE id = ?
                """,
                (connection_id,),
            ).fetchone()

        if row is None:
            raise FileNotFoundError(f"Global database connection {connection_id} not found")

        return self._saved_connection_row_to_model(row)

    def create_global_connection(
        self, connection: DatabaseConnectionInputDefinition | dict
    ) -> DatabaseConnectionDefinition:
        connection = self._normalize_saved_connection_input(connection)
        connection_id = uuid4().hex
        now = _utc_now()
        database_name, service_name = self._saved_connection_params(connection)

        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO global_database_connections (
                        id, name, db_type, host, port, database_name, service_name, user, password, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        connection_id,
                        connection.name,
                        connection.db_type,
                        connection.host,
                        connection.port,
                        database_name,
                        service_name,
                        connection.user,
                        connection.password,
                        now,
                        now,
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise SavedConnectionNameConflictError(
                f'Global database connection name "{connection.name}" is already in use'
            ) from exc

        return self.load_global_connection(connection_id)

    def update_global_connection(
        self, connection_id: str, connection: DatabaseConnectionInputDefinition | dict
    ) -> DatabaseConnectionDefinition:
        connection = self._normalize_saved_connection_input(connection)
        now = _utc_now()
        database_name, service_name = self._saved_connection_params(connection)

        try:
            with self._connect() as conn:
                result = conn.execute(
                    """
                    UPDATE global_database_connections
                    SET name = ?, db_type = ?, host = ?, port = ?, database_name = ?, service_name = ?, user = ?, password = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        connection.name,
                        connection.db_type,
                        connection.host,
                        connection.port,
                        database_name,
                        service_name,
                        connection.user,
                        connection.password,
                        now,
                        connection_id,
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise SavedConnectionNameConflictError(
                f'Global database connection name "{connection.name}" is already in use'
            ) from exc

        if result.rowcount == 0:
            raise FileNotFoundError(f"Global database connection {connection_id} not found")

        return self.load_global_connection(connection_id)

    def _projects_using_global_connection(self, connection_id: str) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT name, pipeline_json FROM projects ORDER BY name COLLATE NOCASE ASC, id ASC"
            ).fetchall()

        project_names: list[str] = []
        for row in rows:
            try:
                payload = json.loads(row["pipeline_json"])
            except json.JSONDecodeError:
                continue
            nodes = payload.get("nodes", [])
            if not isinstance(nodes, list):
                continue
            if any(
                isinstance(node, dict)
                and isinstance(node.get("config"), dict)
                and node["config"].get("connection_mode") == "global"
                and node["config"].get("connection_source_id") == connection_id
                for node in nodes
            ):
                project_names.append(row["name"])
        return project_names

    def delete_global_connection(self, connection_id: str) -> bool:
        project_names = self._projects_using_global_connection(connection_id)
        if project_names:
            raise GlobalConnectionInUseError(project_names)

        with self._connect() as conn:
            result = conn.execute(
                "DELETE FROM global_database_connections WHERE id = ?",
                (connection_id,),
            )
        return result.rowcount > 0
