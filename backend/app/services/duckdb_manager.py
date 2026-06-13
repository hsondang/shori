from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
import json
import logging
import math
import os
from pathlib import Path
import threading

import duckdb

logger = logging.getLogger(__name__)

RESERVED_TABLE_PREFIX = "_shori_"
STAGING_SUFFIX = "__staging"
META_TABLE = "_shori_node_meta"

META_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {META_TABLE} (
    node_id VARCHAR PRIMARY KEY,
    table_name VARCHAR NOT NULL,
    cache_key VARCHAR,
    status VARCHAR NOT NULL,
    row_count BIGINT,
    column_count INTEGER,
    columns_json VARCHAR,
    error VARCHAR,
    started_at VARCHAR,
    finished_at VARCHAR,
    duration_ms DOUBLE
)
"""

META_COLUMNS = [
    "node_id",
    "table_name",
    "cache_key",
    "status",
    "row_count",
    "column_count",
    "columns_json",
    "error",
    "started_at",
    "finished_at",
    "duration_ms",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_reserved_table_name(table_name: str) -> bool:
    return table_name.startswith(RESERVED_TABLE_PREFIX) or table_name.endswith(STAGING_SUFFIX)


def validate_user_table_name(table_name: str) -> str:
    if not table_name:
        raise ValueError("Table name must not be empty")
    if is_reserved_table_name(table_name):
        raise ValueError(
            f"Table name '{table_name}' is reserved (prefix '{RESERVED_TABLE_PREFIX}' "
            f"and suffix '{STAGING_SUFFIX}' are internal)"
        )
    return table_name


class ProjectBusyError(RuntimeError):
    pass


class DuckDBManager:
    """Storage for one project: a persistent DuckDB file plus node metadata.

    Concurrency model: one process-wide connection per file; every operation
    runs on its own cursor (a child connection), so independent node loads can
    write different tables in parallel under DuckDB's MVCC. The invariant that
    keeps this conflict-free is one writer per table at a time, which the
    one-table-per-node model provides.
    """

    def __init__(
        self,
        db_path: str | os.PathLike = ":memory:",
        *,
        memory_limit: str | None = None,
        temp_directory: str | os.PathLike | None = None,
    ):
        self.db_path = str(db_path)
        self._is_file_backed = self.db_path != ":memory:"
        if self._is_file_backed:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._memory_limit = memory_limit
        self._temp_directory = str(temp_directory) if temp_directory else None
        self._active_ops = 0
        self._active_lock = threading.Lock()
        self._closed = False
        self._postgres_extension_ready: bool | None = None
        self.conn = duckdb.connect(self.db_path)
        self._apply_settings()
        self._ensure_meta_schema()
        self._recover_interrupted_loads()

    def _apply_settings(self):
        if self._memory_limit:
            self.conn.execute(f"SET memory_limit = '{self._memory_limit}'")
        if self._temp_directory and self._is_file_backed:
            Path(self._temp_directory).mkdir(parents=True, exist_ok=True)
            self.conn.execute(f"SET temp_directory = '{self._temp_directory}'")

    def _ensure_meta_schema(self):
        self.conn.execute(META_SCHEMA)

    def _recover_interrupted_loads(self):
        """Drop leftover staging tables and fail metadata rows stuck in 'loading'."""
        staging_tables = self.conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name LIKE ?",
            [f"%{STAGING_SUFFIX}"],
        ).fetchall()
        for (name,) in staging_tables:
            logger.warning("Dropping leftover staging table %s from interrupted load", name)
            self.conn.execute(f"DROP TABLE IF EXISTS {_quote_identifier(name)}")
        self.conn.execute(
            f"UPDATE {META_TABLE} SET status = 'failed', "
            "error = 'Load was interrupted (backend stopped mid-load)', "
            "finished_at = ? WHERE status = 'loading'",
            [utc_now_iso()],
        )

    @contextmanager
    def _cursor(self):
        with self._track():
            cur = self.conn.cursor()
            try:
                yield cur
            finally:
                cur.close()

    @contextmanager
    def _track(self):
        with self._active_lock:
            if self._closed:
                raise RuntimeError("DuckDBManager is closed")
            self._active_ops += 1
        try:
            yield
        finally:
            with self._active_lock:
                self._active_ops -= 1

    # ------------------------------------------------------------------
    # Node loads (staging + atomic swap)
    # ------------------------------------------------------------------

    def begin_load(self, node_id: str, table_name: str, cache_key: str | None = None) -> "StagingLoad":
        return StagingLoad(self, node_id, table_name, cache_key)

    def register_csv(
        self,
        table_name: str,
        file_path: str,
        *,
        node_id: str | None = None,
        cache_key: str | None = None,
        register_interrupt=None,
    ) -> dict:
        load = self.begin_load(node_id or table_name, table_name, cache_key)
        try:
            if node_id is not None:
                load.mark_loading()
            if register_interrupt is not None:
                register_interrupt(load.interrupt)
            # Scan the full CSV before inferring types so late mixed-type IDs stay text.
            load.create_staging_as(
                "SELECT * FROM read_csv_auto(?, sample_size=-1)", [file_path]
            )
            return load.commit(record_meta=node_id is not None)
        except BaseException as exc:
            load.abort(str(exc), record_meta=node_id is not None)
            raise

    def register_dataframe(
        self,
        table_name: str,
        df,
        *,
        node_id: str | None = None,
        cache_key: str | None = None,
    ) -> dict:
        load = self.begin_load(node_id or table_name, table_name, cache_key)
        try:
            if node_id is not None:
                load.mark_loading()
            load.append(df)
            return load.commit(record_meta=node_id is not None)
        except BaseException as exc:
            load.abort(str(exc), record_meta=node_id is not None)
            raise

    def execute_transform(
        self,
        table_name: str,
        sql: str,
        *,
        node_id: str | None = None,
        cache_key: str | None = None,
        register_interrupt=None,
    ) -> dict:
        load = self.begin_load(node_id or table_name, table_name, cache_key)
        try:
            if node_id is not None:
                load.mark_loading()
            if register_interrupt is not None:
                register_interrupt(load.interrupt)
            load.create_staging_as(f"({sql})")
            return load.commit(record_meta=node_id is not None)
        except BaseException as exc:
            load.abort(str(exc), record_meta=node_id is not None)
            raise

    def append_dataframe(self, table_name: str, df) -> dict:
        with self._cursor() as cur:
            quoted = _quote_identifier(table_name)
            cur.register("_shori_append_src", df)
            try:
                cur.execute(f"INSERT INTO {quoted} SELECT * FROM _shori_append_src")
            finally:
                cur.unregister("_shori_append_src")
            return _table_stats(cur, table_name)

    # ------------------------------------------------------------------
    # Node metadata
    # ------------------------------------------------------------------

    def upsert_node_meta(self, **fields) -> None:
        with self._cursor() as cur:
            _upsert_meta(cur, fields)

    def get_node_meta(self, node_id: str) -> dict | None:
        with self._cursor() as cur:
            row = cur.execute(
                f"SELECT {', '.join(META_COLUMNS)} FROM {META_TABLE} WHERE node_id = ?",
                [node_id],
            ).fetchone()
        return _meta_row_to_dict(row) if row else None

    def all_node_meta(self) -> dict[str, dict]:
        with self._cursor() as cur:
            rows = cur.execute(
                f"SELECT {', '.join(META_COLUMNS)} FROM {META_TABLE}"
            ).fetchall()
        metas = (_meta_row_to_dict(row) for row in rows)
        return {meta["node_id"]: meta for meta in metas}

    def drop_node(self, node_id: str) -> bool:
        """Drop a node's table and metadata row. Returns True if anything existed."""
        meta = self.get_node_meta(node_id)
        existed = meta is not None
        with self._cursor() as cur:
            if meta is not None:
                cur.execute(f"DROP TABLE IF EXISTS {_quote_identifier(meta['table_name'])}")
                cur.execute(
                    f"DROP TABLE IF EXISTS {_quote_identifier(meta['table_name'] + STAGING_SUFFIX)}"
                )
            cur.execute(f"DELETE FROM {META_TABLE} WHERE node_id = ?", [node_id])
        return existed

    def rename_node_table(self, node_id: str, new_table_name: str) -> bool:
        """Rename a cached node table, preserving its data and cache validity."""
        validate_user_table_name(new_table_name)
        meta = self.get_node_meta(node_id)
        if meta is None or meta["table_name"] == new_table_name:
            return False
        with self._cursor() as cur:
            cur.execute("BEGIN TRANSACTION")
            try:
                if _table_exists(cur, meta["table_name"]):
                    cur.execute(f"DROP TABLE IF EXISTS {_quote_identifier(new_table_name)}")
                    cur.execute(
                        f"ALTER TABLE {_quote_identifier(meta['table_name'])} "
                        f"RENAME TO {_quote_identifier(new_table_name)}"
                    )
                cur.execute(
                    f"UPDATE {META_TABLE} SET table_name = ? WHERE node_id = ?",
                    [new_table_name, node_id],
                )
                cur.execute("COMMIT")
            except BaseException:
                cur.execute("ROLLBACK")
                raise
        return True

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def preview(self, table_name: str, offset: int = 0, limit: int = 100) -> dict:
        with self._cursor() as cur:
            quoted_table_name = _quote_identifier(table_name)
            cols_result = cur.execute(f"DESCRIBE {quoted_table_name}").fetchall()
            columns = [row[0] for row in cols_result]
            col_types = [row[1] for row in cols_result]

            rows = self._fetch_preview_rows(cur, quoted_table_name, columns, offset, limit)

            total = cur.execute(
                f"SELECT COUNT(*) FROM {quoted_table_name}"
            ).fetchone()[0]

            return {
                "kind": "table",
                "columns": columns,
                "column_types": col_types,
                "rows": [[_json_safe_value(value) for value in row] for row in rows],
                "total_rows": total,
                "offset": offset,
                "limit": limit,
            }

    def export_to_csv(self, table_name: str, output_path: str):
        with self._cursor() as cur:
            cur.execute(
                f"COPY {_quote_identifier(table_name)} TO '{output_path}' (HEADER, DELIMITER ',')"
            )

    def drop_table(self, table_name: str):
        if table_name.startswith(RESERVED_TABLE_PREFIX):
            raise ValueError(f"Cannot drop internal table '{table_name}'")
        with self._cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {_quote_identifier(table_name)}")
            cur.execute(f"DELETE FROM {META_TABLE} WHERE table_name = ?", [table_name])

    def table_exists(self, table_name: str) -> bool:
        with self._cursor() as cur:
            return _table_exists(cur, table_name)

    def table_stats(self, table_name: str) -> dict:
        with self._cursor() as cur:
            return _table_stats(cur, table_name)

    # ------------------------------------------------------------------
    # File maintenance
    # ------------------------------------------------------------------

    def storage_info(self) -> dict:
        size = 0
        if self._is_file_backed:
            path = Path(self.db_path)
            if path.exists():
                size = path.stat().st_size
            wal = Path(self.db_path + ".wal")
            if wal.exists():
                size += wal.stat().st_size
        return {"file_size_bytes": size, "path": self.db_path}

    def compact(self) -> dict:
        """Rewrite the database file to reclaim space freed by dropped tables.

        DuckDB files never shrink in place; the only way to reclaim space is to
        copy the catalog into a fresh file and swap it in.
        """
        if not self._is_file_backed:
            return self.storage_info()
        with self._active_lock:
            if self._active_ops > 0:
                raise ProjectBusyError("Cannot compact while project operations are running")
            if self._closed:
                raise RuntimeError("DuckDBManager is closed")
            self._closed = True  # block new ops while we swap files
        try:
            tmp_path = self.db_path + ".compact"
            for leftover in (tmp_path, tmp_path + ".wal"):
                if os.path.exists(leftover):
                    os.remove(leftover)
            current_db = self.conn.execute("SELECT current_database()").fetchone()[0]
            self.conn.execute(f"ATTACH '{tmp_path}' AS _shori_compact_target")
            self.conn.execute(
                f"COPY FROM DATABASE {_quote_identifier(current_db)} TO _shori_compact_target"
            )
            self.conn.execute("DETACH _shori_compact_target")
            self.conn.close()
            os.replace(tmp_path, self.db_path)
            for leftover in (self.db_path + ".wal", tmp_path + ".wal"):
                if os.path.exists(leftover):
                    os.remove(leftover)
            self.conn = duckdb.connect(self.db_path)
            self._apply_settings()
        finally:
            with self._active_lock:
                self._closed = False
        return self.storage_info()

    def checkpoint(self):
        with self._cursor() as cur:
            cur.execute("CHECKPOINT")

    def ensure_postgres_extension(self) -> bool:
        """Install/load DuckDB's postgres extension once per manager.

        Returns False (cached) when unavailable — e.g. offline with no local
        copy — so callers can fall back to driver-based extraction.
        """
        if self._postgres_extension_ready is None:
            try:
                with self._cursor() as cur:
                    cur.execute("INSTALL postgres")
                    cur.execute("LOAD postgres")
                self._postgres_extension_ready = True
            except Exception:
                logger.warning(
                    "DuckDB postgres extension unavailable; postgres sources "
                    "will load through the driver instead.",
                    exc_info=True,
                )
                self._postgres_extension_ready = False
        return self._postgres_extension_ready

    def _fetch_preview_rows(
        self,
        cur,
        quoted_table_name: str,
        columns: list[str],
        offset: int,
        limit: int,
    ) -> list[tuple]:
        try:
            return cur.execute(
                f"SELECT * FROM {quoted_table_name} LIMIT ? OFFSET ?",
                [limit, offset],
            ).fetchall()
        except duckdb.Error:
            # Some column types (e.g. TIMESTAMP WITH TIME ZONE) cannot be fetched
            # natively into Python; retry with every column cast to text.
            logger.warning(
                "Native preview fetch for %s failed; retrying with all columns "
                "cast to VARCHAR.",
                quoted_table_name,
                exc_info=True,
            )
            select_list = ", ".join(
                f"CAST({_quote_identifier(column)} AS VARCHAR) AS {_quote_identifier(column)}"
                for column in columns
            )
            if not select_list:
                return []
            return cur.execute(
                f"SELECT {select_list} FROM {quoted_table_name} LIMIT ? OFFSET ?",
                [limit, offset],
            ).fetchall()

    def close(self):
        with self._active_lock:
            self._closed = True
        self.conn.close()


class StagingLoad:
    """A chunked load into `<table>__staging`, committed via an atomic swap.

    The real table name only ever points at a fully loaded result: a crash
    mid-load leaves junk in the staging table (cleaned up on next open) while
    the previous version of the table stays intact and queryable.
    """

    def __init__(self, manager: DuckDBManager, node_id: str, table_name: str, cache_key: str | None):
        validate_user_table_name(table_name)
        self.node_id = node_id
        self.table_name = table_name
        self.cache_key = cache_key
        self.staging_name = table_name + STAGING_SUFFIX
        self.started_at = utc_now_iso()
        self._manager = manager
        self._track = manager._track()
        self._track.__enter__()
        self._cur = manager.conn.cursor()
        self._created = False
        self._finished = False
        try:
            self._cur.execute(f"DROP TABLE IF EXISTS {_quote_identifier(self.staging_name)}")
        except BaseException:
            self._cleanup()
            raise

    def mark_loading(self):
        """Record the in-flight load in node metadata (engine paths only)."""
        _upsert_meta(
            self._cur,
            {
                "node_id": self.node_id,
                "table_name": self.table_name,
                "cache_key": self.cache_key,
                "status": "loading",
                "started_at": self.started_at,
            },
        )

    def append(self, data) -> None:
        """Append a chunk: a pandas DataFrame, Arrow table/batch, or any object
        DuckDB can scan (including Arrow PyCapsule streams)."""
        self._cur.register("_shori_chunk_src", data)
        try:
            if not self._created:
                self._cur.execute(
                    f"CREATE TABLE {_quote_identifier(self.staging_name)} "
                    "AS SELECT * FROM _shori_chunk_src"
                )
                self._created = True
            else:
                self._cur.execute(
                    f"INSERT INTO {_quote_identifier(self.staging_name)} "
                    "SELECT * FROM _shori_chunk_src"
                )
        finally:
            self._cur.unregister("_shori_chunk_src")

    def create_staging_as(self, select_sql: str, params: list | None = None) -> None:
        """Create the staging table directly from a SELECT (CSV scan, transform, ATTACH)."""
        if self._created:
            raise RuntimeError("Staging table already created")
        self._cur.execute(
            f"CREATE TABLE {_quote_identifier(self.staging_name)} AS {select_sql}",
            params or [],
        )
        self._created = True

    def execute(self, sql: str, params: list | None = None):
        """Run a setup statement on the load's cursor (e.g. LOAD/ATTACH)."""
        return self._cur.execute(sql, params or [])

    def interrupt(self):
        try:
            self._cur.interrupt()
        except Exception:
            logger.warning("Failed to interrupt staging load for %s", self.node_id, exc_info=True)

    def commit(self, record_meta: bool = True) -> dict:
        if self._finished:
            raise RuntimeError("StagingLoad already finished")
        if not self._created:
            raise RuntimeError("Cannot commit a load with no data appended")
        try:
            stats = _table_stats(self._cur, self.staging_name)
            finished_at = utc_now_iso()
            duration_ms = (
                datetime.fromisoformat(finished_at) - datetime.fromisoformat(self.started_at)
            ).total_seconds() * 1000
            self._cur.execute("BEGIN TRANSACTION")
            try:
                self._cur.execute(f"DROP TABLE IF EXISTS {_quote_identifier(self.table_name)}")
                self._cur.execute(
                    f"ALTER TABLE {_quote_identifier(self.staging_name)} "
                    f"RENAME TO {_quote_identifier(self.table_name)}"
                )
                if record_meta:
                    _upsert_meta(
                        self._cur,
                        {
                            "node_id": self.node_id,
                            "table_name": self.table_name,
                            "cache_key": self.cache_key,
                            "status": "complete",
                            "row_count": stats["row_count"],
                            "column_count": stats["column_count"],
                            "columns_json": json.dumps(stats["columns"]),
                            "started_at": self.started_at,
                            "finished_at": finished_at,
                            "duration_ms": duration_ms,
                        },
                    )
                self._cur.execute("COMMIT")
            except BaseException:
                self._cur.execute("ROLLBACK")
                raise
            return stats
        finally:
            self._finished = True
            self._cleanup()

    def abort(self, error: str | None = None, record_meta: bool = True) -> None:
        if self._finished:
            return
        self._finished = True
        try:
            self._cur.execute(f"DROP TABLE IF EXISTS {_quote_identifier(self.staging_name)}")
            if record_meta:
                _upsert_meta(
                    self._cur,
                    {
                        "node_id": self.node_id,
                        "table_name": self.table_name,
                        "cache_key": self.cache_key,
                        "status": "failed",
                        "error": error,
                        "started_at": self.started_at,
                        "finished_at": utc_now_iso(),
                    },
                )
        except Exception:
            logger.warning("Failed to clean up aborted load for %s", self.node_id, exc_info=True)
        finally:
            self._cleanup()

    def _cleanup(self):
        try:
            self._cur.close()
        except Exception:
            pass
        self._track.__exit__(None, None, None)


def _upsert_meta(cur, fields: dict) -> None:
    row = {column: None for column in META_COLUMNS}
    row.update({key: value for key, value in fields.items() if key in row})
    placeholders = ", ".join("?" for _ in META_COLUMNS)
    cur.execute(
        f"INSERT OR REPLACE INTO {META_TABLE} ({', '.join(META_COLUMNS)}) "
        f"VALUES ({placeholders})",
        [row[column] for column in META_COLUMNS],
    )


def _meta_row_to_dict(row: tuple) -> dict:
    meta = dict(zip(META_COLUMNS, row))
    meta["columns"] = json.loads(meta.pop("columns_json")) if meta.get("columns_json") else None
    return meta


def _table_exists(cur, table_name: str) -> bool:
    result = cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return result[0] > 0


def _table_stats(cur, table_name: str) -> dict:
    quoted_table_name = _quote_identifier(table_name)
    count = cur.execute(f"SELECT COUNT(*) FROM {quoted_table_name}").fetchone()[0]
    cols = cur.execute(f"DESCRIBE {quoted_table_name}").fetchall()
    return {
        "row_count": count,
        "column_count": len(cols),
        "columns": [c[0] for c in cols],
    }


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _json_safe_value(value):
    if value is None or isinstance(value, (bool, str, int)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    return str(value)
