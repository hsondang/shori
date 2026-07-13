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
        """True once anything has ever been exported to or drafted in this workspace."""
        return (
            self.db_path.exists()
            or self.meta_path.exists()
            or any(self.inbox.glob("*.json"))
        )

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
        self.root.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.meta_path)
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tables (
                name TEXT PRIMARY KEY,
                source_node_id TEXT,
                source_table TEXT,
                exported_at TEXT,
                cloned_at TEXT NOT NULL,
                row_count INTEGER
            );
            CREATE TABLE IF NOT EXISTS editor (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                sql TEXT NOT NULL,
                last_editor TEXT NOT NULL,   -- 'user' | 'agent'
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS drafts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sql TEXT NOT NULL,
                note TEXT,
                status TEXT NOT NULL,        -- 'staged' | 'loaded' | 'superseded'
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                tool TEXT NOT NULL,
                detail TEXT NOT NULL,        -- JSON
                decision TEXT NOT NULL       -- 'allowed' | 'pending' | 'denied' | 'error'
            );
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

    # -- shared editor (docs/ai-workspace-model.md §6b, §7) --------------------

    def get_editor(self) -> dict:
        with self._lock:
            return self._editor_snapshot()

    def _editor_snapshot(self) -> dict:
        if not self.meta_path.exists():
            return {"sql": "", "last_editor": None, "updated_at": None, "staged_draft": None}
        with self._meta() as meta_conn:
            row = meta_conn.execute(
                "SELECT sql, last_editor, updated_at FROM editor WHERE id = 1"
            ).fetchone()
            draft = meta_conn.execute(
                "SELECT id, sql, note, created_at FROM drafts WHERE status = 'staged' "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return {
            "sql": row[0] if row else "",
            "last_editor": row[1] if row else None,
            "updated_at": row[2] if row else None,
            "staged_draft": (
                {"id": draft[0], "sql": draft[1], "note": draft[2], "created_at": draft[3]}
                if draft
                else None
            ),
        }

    def _set_editor(self, meta_conn: sqlite3.Connection, sql: str, editor: str) -> None:
        meta_conn.execute(
            """
            INSERT INTO editor (id, sql, last_editor, updated_at) VALUES (1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                sql = excluded.sql,
                last_editor = excluded.last_editor,
                updated_at = excluded.updated_at
            """,
            (sql, editor, _utc_now_iso()),
        )

    def user_set_editor(self, sql: str) -> dict:
        with self._lock:
            with self._meta() as meta_conn:
                self._set_editor(meta_conn, sql, "user")
            return self._editor_snapshot()

    def agent_write_editor(self, sql: str, note: str | None = None) -> dict:
        """Direct write when the editor is clean (empty or agent-owned); otherwise
        stage a draft so the agent never clobbers the user's typing."""
        with self._lock:
            with self._meta() as meta_conn:
                row = meta_conn.execute(
                    "SELECT sql, last_editor FROM editor WHERE id = 1"
                ).fetchone()
                editor_clean = row is None or not row[0].strip() or row[1] == "agent"
                if editor_clean:
                    self._set_editor(meta_conn, sql, "agent")
                    return {"mode": "written"}
                meta_conn.execute("UPDATE drafts SET status = 'superseded' WHERE status = 'staged'")
                cursor = meta_conn.execute(
                    "INSERT INTO drafts (sql, note, status, created_at) VALUES (?, ?, 'staged', ?)",
                    (sql, note, _utc_now_iso()),
                )
                return {"mode": "staged", "draft_id": cursor.lastrowid}

    def load_draft(self, draft_id: int) -> dict:
        """User accepted a staged draft: it becomes the editor content, owned by
        the agent (so follow-up agent writes are direct until the user types)."""
        with self._lock:
            with self._meta() as meta_conn:
                row = meta_conn.execute(
                    "SELECT sql FROM drafts WHERE id = ? AND status = 'staged'", (draft_id,)
                ).fetchone()
                if row is None:
                    raise KeyError(draft_id)
                self._set_editor(meta_conn, row[0], "agent")
                meta_conn.execute("UPDATE drafts SET status = 'loaded' WHERE id = ?", (draft_id,))
            return self._editor_snapshot()

    # -- validation ------------------------------------------------------------

    def validate_sql(self, sql: str) -> dict:
        """Bind-only validation on the sandboxed connection: never executes.

        DESCRIBE runs the binder and yields output columns for query-shaped
        statements; EXPLAIN covers the rest (DDL/DML) without executing either.
        The multi-statement guard matters: a second statement appended after the
        one being validated would otherwise be *executed* by conn.execute().
        """
        stripped = sql.strip().rstrip(";").strip()
        if not stripped:
            return {"valid": False, "error": "Empty SQL"}
        with self._lock:
            conn = self._connection()
            try:
                if len(conn.extract_statements(stripped)) != 1:
                    return {
                        "valid": False,
                        "error": "Validate one statement at a time (multiple statements found).",
                    }
            except Exception as exc:  # parse error
                return {"valid": False, "error": str(exc)}
            try:
                described = conn.execute(f"DESCRIBE {stripped}").fetchall()
                return {
                    "valid": True,
                    "columns": [{"name": col[0], "type": col[1]} for col in described],
                }
            except Exception as describe_error:
                try:
                    conn.execute(f"EXPLAIN {stripped}").fetchall()
                    return {"valid": True, "columns": []}
                except Exception:
                    return {"valid": False, "error": str(describe_error)}

    # -- settings, activity, state ---------------------------------------------

    SETTING_DEFAULTS = {"autonomous_execute": False, "auto_share_results": False}

    def get_settings(self) -> dict:
        values = dict(self.SETTING_DEFAULTS)
        if self.meta_path.exists():
            with self._lock, self._meta() as meta_conn:
                for key, value in meta_conn.execute("SELECT key, value FROM settings"):
                    if key in values:
                        values[key] = bool(value)
        return values

    def log_activity(self, tool: str, detail: dict, decision: str = "allowed") -> None:
        with self._lock, self._meta() as meta_conn:
            meta_conn.execute(
                "INSERT INTO activity (ts, tool, detail, decision) VALUES (?, ?, ?, ?)",
                (_utc_now_iso(), tool, json.dumps(detail)[:4000], decision),
            )

    def list_activity(self, limit: int = 50) -> list[dict]:
        if not self.meta_path.exists():
            return []
        with self._lock, self._meta() as meta_conn:
            rows = meta_conn.execute(
                "SELECT ts, tool, detail, decision FROM activity ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            {"ts": ts, "tool": tool, "detail": json.loads(detail), "decision": decision}
            for ts, tool, detail, decision in rows
        ]

    def workspace_state(self) -> dict:
        return {
            "settings": self.get_settings(),
            "editor": self.get_editor(),
            "latest_result": None,  # Phase 3
            "pending_requests": [],  # Phases 3-4
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
