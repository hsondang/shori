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

# Wall-clock ceiling for a single execution, enforced via conn.interrupt().
EXECUTION_TIMEOUT_S = 30


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
        self.results_dir = root / "results"
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
            CREATE TABLE IF NOT EXISTS execution_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sql TEXT NOT NULL,           -- editor snapshot at request time
                status TEXT NOT NULL,        -- 'pending' | 'approved'
                created_at TEXT NOT NULL,
                resolved_at TEXT,
                result_id INTEGER
            );
            CREATE TABLE IF NOT EXISTS results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sql TEXT NOT NULL,
                status TEXT NOT NULL,        -- 'ok' | 'error'
                error TEXT,
                row_count INTEGER,           -- for the UI; never shown to the agent
                columns TEXT,                -- JSON; UI only
                initiated_by TEXT NOT NULL,  -- 'agent' | 'user'
                request_id INTEGER,          -- links to an approved execution_request
                shared INTEGER NOT NULL DEFAULT 0,  -- Phase 4: disclosed to the agent
                ran_at TEXT NOT NULL
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

    # -- execution (docs/ai-workspace-model.md §4, §6b) ------------------------

    def execute_editor(self, initiated_by: str) -> dict:
        """Run the editor's current content on the sandboxed connection.

        The audit invariant: only SQL sitting in the editor can ever run — this
        method takes no SQL argument. Full SQL is permitted (the §2 sandbox
        contains the blast radius); a wall-clock timeout guards runaway queries.
        Result rows are captured to a parquet file under results/, which the
        agent's SQL cannot read (external access is disabled) — so a stored
        result is never queryable back through the workspace. Returns UI-shaped
        metadata; the server redacts row_count/columns for the agent.
        """
        with self._lock:
            sql = self._editor_snapshot()["sql"]
            stripped = sql.strip().rstrip(";").strip()
            if not stripped:
                raise ValueError("The editor is empty — write a query first.")
            try:
                arrow, columns, row_count = self._run_and_capture(stripped)
                status, error = "ok", None
            except Exception as exc:  # DuckDB binder/runtime/interrupt errors
                arrow, columns, row_count = None, [], None
                status = "error"
                error = (
                    f"Query exceeded the {EXECUTION_TIMEOUT_S}s time limit and was cancelled."
                    if "INTERRUPT" in str(exc).upper()
                    else str(exc)
                )
            return self._store_result(
                stripped, status, error, columns, row_count, arrow, initiated_by
            )

    def _run_and_capture(self, sql: str):
        """Execute under a timeout; return (arrow|None, columns, row_count).

        DuckDB reports affected rows for DDL/DML as a single-column "Count"
        result — classified here as "no result set" so the UI shows a status,
        not a bogus one-cell table."""
        conn = self._connection()
        timer = threading.Timer(EXECUTION_TIMEOUT_S, conn.interrupt)
        timer.start()
        try:
            cursor = conn.execute(sql)
            arrow_table = cursor.to_arrow_table()
        finally:
            timer.cancel()
        schema = arrow_table.schema
        if len(schema) == 1 and schema.names[0] == "Count":
            affected = arrow_table.column(0)[0].as_py() if arrow_table.num_rows else 0
            return None, [], affected
        columns = [{"name": field.name, "type": str(field.type)} for field in schema]
        return arrow_table, columns, arrow_table.num_rows

    def _store_result(self, sql, status, error, columns, row_count, arrow, initiated_by) -> dict:
        with self._meta() as meta_conn:
            cursor = meta_conn.execute(
                "INSERT INTO results (sql, status, error, row_count, columns, initiated_by, ran_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (sql, status, error, row_count, json.dumps(columns), initiated_by, _utc_now_iso()),
            )
            result_id = cursor.lastrowid
        if arrow is not None:
            self.results_dir.mkdir(parents=True, exist_ok=True)
            pyarrow.parquet.write_table(arrow, self.results_dir / f"{result_id}.parquet")
        return {
            "result_id": result_id,
            "status": status,
            "error": error,
            "row_count": row_count,
            "columns": columns,
        }

    def read_result_rows(self, result_id: int, limit: int = 100) -> dict:
        """Rows of a stored result (for the UI, and — Phase 4 — gated agent access)."""
        with self._lock:
            with self._meta() as meta_conn:
                row = meta_conn.execute(
                    "SELECT status, error, row_count, columns FROM results WHERE id = ?",
                    (result_id,),
                ).fetchone()
            if row is None:
                raise KeyError(result_id)
            status, error, row_count, columns_json = row
            columns = json.loads(columns_json)
            path = self.results_dir / f"{result_id}.parquet"
            if not path.exists():
                return {
                    "columns": columns, "rows": [], "row_count": row_count,
                    "truncated": False, "status": status, "error": error,
                }
            table = pyarrow.parquet.read_table(path)
            total = table.num_rows
            rows = table.slice(0, max(0, limit)).to_pylist()
            return {
                "columns": columns, "rows": rows, "row_count": total,
                "truncated": total > limit, "status": status, "error": error,
            }

    # -- execution requests (JIT approval, docs §4) ----------------------------

    def create_execution_request(self, sql: str) -> int:
        with self._lock, self._meta() as meta_conn:
            cursor = meta_conn.execute(
                "INSERT INTO execution_requests (sql, status, created_at) VALUES (?, 'pending', ?)",
                (sql, _utc_now_iso()),
            )
            return cursor.lastrowid

    def resolve_pending_execute(self, result_id: int) -> int | None:
        """Link a just-produced result to the oldest pending request (the user
        clicking Run is the approval). No pending request → nothing to link."""
        with self._lock, self._meta() as meta_conn:
            row = meta_conn.execute(
                "SELECT id FROM execution_requests WHERE status = 'pending' ORDER BY id LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            meta_conn.execute(
                "UPDATE execution_requests SET status = 'approved', resolved_at = ?, result_id = ? "
                "WHERE id = ?",
                (_utc_now_iso(), result_id, row[0]),
            )
            return row[0]

    def _pending_requests(self) -> list[dict]:
        if not self.meta_path.exists():
            return []
        with self._meta() as meta_conn:
            rows = meta_conn.execute(
                "SELECT id, sql, created_at FROM execution_requests WHERE status = 'pending' "
                "ORDER BY id"
            ).fetchall()
        return [
            {"id": rid, "kind": "execute", "sql": sql, "created_at": created}
            for rid, sql, created in rows
        ]

    def _latest_result(self) -> dict | None:
        if not self.meta_path.exists():
            return None
        with self._meta() as meta_conn:
            row = meta_conn.execute(
                "SELECT id, status, error, row_count, columns, initiated_by, request_id, shared, ran_at "
                "FROM results ORDER BY id DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        rid, status, error, row_count, columns, initiated_by, request_id, shared, ran_at = row
        return {
            "result_id": rid,
            "status": status,
            "error": error,
            "row_count": row_count,
            "columns": json.loads(columns),
            "initiated_by": initiated_by,
            "request_id": request_id,
            "shared": bool(shared),
            "ran_at": ran_at,
        }

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

    def set_setting(self, key: str, value: bool) -> None:
        if key not in self.SETTING_DEFAULTS:
            raise KeyError(key)
        with self._lock, self._meta() as meta_conn:
            meta_conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, int(bool(value))),
            )

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
            "latest_result": self._latest_result(),
            "pending_requests": self._pending_requests(),
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
