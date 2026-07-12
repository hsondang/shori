"""Per-project AI workspace: sandboxed DuckDB + clone metadata + inbox sweep.

The workspace DuckDB is opened with DuckDB's own sandbox switches
(docs/ai-workspace-model.md §2):

- ``enable_external_access = false`` — SQL on this instance cannot ATTACH other
  databases, read or write files, or load extensions. This is what makes it
  safe to allow full SQL inside the workspace later: escaping the workspace is
  impossible at the database level, not merely discouraged.
- ``lock_configuration = true`` — SQL cannot flip the switch back.

Because those settings are instance-global and locked, ingestion cannot use SQL
file reads (``FROM 'x.parquet'`` is exactly what external access blocks).
Clones are therefore read with pyarrow and handed to DuckDB as in-memory Arrow
tables.

Clone metadata (provenance, cloned_at) lives in a sidecar ``meta.sqlite3``, not
inside the workspace DuckDB — the agent's SQL can touch every table in the
workspace, and audit records must not be agent-writable.
"""

import json
import logging
import re
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow.parquet

logger = logging.getLogger(__name__)

# Applied at connect time; lock_configuration makes them immutable afterwards.
WORKSPACE_DB_CONFIG = {
    "memory_limit": "2GB",
    "enable_external_access": "false",
    "lock_configuration": "true",
}

# Same shape the main app enforces for user table names; independently stated
# here because spool file names become SQL identifiers. Underscore-prefixed
# names are reserved for internal use.
_SAFE_TABLE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class AIWorkspace:
    """One project's AI workspace. All operations serialize on a single lock —
    correctness first; concurrency is not a Phase 1 concern."""

    def __init__(self, root: Path):
        self.root = root  # data/ai/<project_id>/
        self.inbox = root / "inbox"
        self.db_path = root / "workspace.duckdb"
        self.meta_path = root / "meta.sqlite3"
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._lock = threading.Lock()

    # -- lifecycle -----------------------------------------------------------

    def exists(self) -> bool:
        """True once anything has ever been exported to this workspace."""
        return self.db_path.exists() or any(self.inbox.glob("*.json"))

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _connection(self) -> duckdb.DuckDBPyConnection:
        """Open lazily so projects that never export get no workspace files."""
        if self._conn is None:
            self.root.mkdir(parents=True, exist_ok=True)
            self._conn = duckdb.connect(str(self.db_path), config=WORKSPACE_DB_CONFIG)
        return self._conn

    def _meta(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.meta_path)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tables (
                name TEXT PRIMARY KEY,
                source_node_id TEXT,
                source_table TEXT,
                exported_at TEXT,
                cloned_at TEXT NOT NULL,
                row_count INTEGER
            )
            """
        )
        return conn

    # -- ingestion -----------------------------------------------------------

    def sweep_inbox(self) -> int:
        """Ingest pending clones from the spool. Safe to call any time.

        The sidecar JSON is the commit marker (written last by the exporter),
        so files are keyed off sidecars. A file that fails to ingest is left in
        place for the next sweep rather than deleted.
        """
        if not self.inbox.is_dir():
            return 0
        sidecars = sorted(self.inbox.glob("*.json"))
        if not sidecars:
            return 0
        ingested = 0
        with self._lock:
            for sidecar in sidecars:
                try:
                    self._ingest_one(sidecar)
                    ingested += 1
                except Exception:
                    logger.exception("AI workspace: failed to ingest %s; will retry", sidecar)
        return ingested

    def _ingest_one(self, sidecar: Path) -> None:
        meta = json.loads(sidecar.read_text())
        table_name = str(meta.get("table_name", ""))
        parquet = self.inbox / f"{table_name}.parquet"
        if not _SAFE_TABLE_NAME.fullmatch(table_name):
            raise ValueError(f"Refusing to ingest unsafe table name {table_name!r}")
        if not parquet.exists():
            raise FileNotFoundError(f"Sidecar without parquet: {sidecar.name}")

        arrow_table = pyarrow.parquet.read_table(parquet)
        conn = self._connection()
        conn.register("_shori_inbox_batch", arrow_table)
        try:
            conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM _shori_inbox_batch')
        finally:
            conn.unregister("_shori_inbox_batch")

        with self._meta() as meta_conn:
            meta_conn.execute(
                """
                INSERT INTO tables (name, source_node_id, source_table, exported_at, cloned_at, row_count)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    source_node_id = excluded.source_node_id,
                    source_table = excluded.source_table,
                    exported_at = excluded.exported_at,
                    cloned_at = excluded.cloned_at,
                    row_count = excluded.row_count
                """,
                (
                    table_name,
                    meta.get("source_node_id"),
                    meta.get("source_table"),
                    meta.get("exported_at"),
                    _utc_now_iso(),
                    arrow_table.num_rows,
                ),
            )
        parquet.unlink()
        sidecar.unlink()

    # -- reads ---------------------------------------------------------------

    def list_tables(self) -> list[dict]:
        if not self.meta_path.exists():
            return []
        with self._lock:
            with self._meta() as meta_conn:
                rows = meta_conn.execute(
                    "SELECT name, source_node_id, source_table, exported_at, cloned_at, row_count "
                    "FROM tables ORDER BY name"
                ).fetchall()
            tables = []
            for name, source_node_id, source_table, exported_at, cloned_at, row_count in rows:
                columns = self._connection().execute(
                    "SELECT count(*) FROM information_schema.columns "
                    "WHERE table_name = ? AND table_schema = 'main'",
                    [name],
                ).fetchone()[0]
                tables.append(
                    {
                        "name": name,
                        "column_count": columns,
                        "row_count": row_count,
                        "cloned_from": {"node_id": source_node_id, "table": source_table},
                        "cloned_at": cloned_at,
                    }
                )
            return tables

    def get_table_schema(self, table_name: str) -> dict:
        with self._lock:
            with self._meta() as meta_conn:
                row = meta_conn.execute(
                    "SELECT cloned_at FROM tables WHERE name = ?", (table_name,)
                ).fetchone()
            if row is None or not _SAFE_TABLE_NAME.fullmatch(table_name):
                raise KeyError(table_name)
            described = self._connection().execute(f'DESCRIBE "{table_name}"').fetchall()
            return {
                "table": table_name,
                "cloned_at": row[0],
                "columns": [{"name": col[0], "type": col[1]} for col in described],
            }


class AIWorkspaceRegistry:
    """project_id -> AIWorkspace, created lazily under data_dir/ai/."""

    def __init__(self, data_dir: Path):
        self._data_dir = data_dir
        self._workspaces: dict[str, AIWorkspace] = {}
        self._lock = threading.Lock()

    def get(self, project_id: str) -> AIWorkspace:
        with self._lock:
            workspace = self._workspaces.get(project_id)
            if workspace is None:
                workspace = AIWorkspace(self._data_dir / "ai" / project_id)
                self._workspaces[project_id] = workspace
            return workspace

    def close_all(self) -> None:
        with self._lock:
            for workspace in self._workspaces.values():
                workspace.close()
            self._workspaces.clear()
