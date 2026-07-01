from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
import json
import logging
import math
import os
from pathlib import Path
import threading
from uuid import uuid4

import duckdb

logger = logging.getLogger(__name__)

RESERVED_TABLE_PREFIX = "_shori_"
STAGING_SUFFIX = "__staging"
META_TABLE = "_shori_node_meta"

# Catalog holding RAM-only "in-memory" node tables. It is an attached
# `:memory:` database: visible across every cursor of the project's DuckDB
# instance (so Transform nodes can join it), but gone the moment the process
# exits — which is exactly the lifecycle the in-memory load mode wants.
SCRATCH_CATALOG = "scratch"

LOCATION_MEMORY = "in_memory"
LOCATION_MATERIALIZED = "materialized"

# One row per (node, location): a node can hold an in-memory copy AND a
# materialized copy at once, each tracked (and invalidated) independently.
_META_TABLE_DDL = """
    node_id VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    cache_key VARCHAR,
    status VARCHAR NOT NULL,
    location VARCHAR NOT NULL,
    row_count BIGINT,
    column_count INTEGER,
    columns_json VARCHAR,
    error VARCHAR,
    started_at VARCHAR,
    finished_at VARCHAR,
    duration_ms DOUBLE,
    PRIMARY KEY (node_id, location)
"""

META_COLUMNS = [
    "node_id",
    "table_name",
    "cache_key",
    "status",
    "location",
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
        self._excel_extension_ready: bool | None = None
        self._catalog = "memory"
        self._search_path = ""
        self.conn = duckdb.connect(self.db_path)
        self._attach_scratch(self.conn)
        self._apply_settings()
        self._ensure_meta_schema()
        self._recover_interrupted_loads()

    def _attach_scratch(self, conn):
        """Attach the RAM-only scratch catalog and pin the cross-catalog search path."""
        conn.execute(f"ATTACH IF NOT EXISTS ':memory:' AS {SCRATCH_CATALOG}")
        self._catalog = conn.execute("SELECT current_database()").fetchone()[0]
        # The meta table always lives in the project catalog; reference it fully
        # qualified so the scratch-first search_path never resolves it to scratch.
        self._meta_ref = _qualified(self._catalog, META_TABLE)
        # Quoted, no space after the comma: DuckDB parses a leading space as part
        # of the next schema name. The project catalog is first so it stays the
        # `current_database()` (CHECKPOINT, compact, unqualified CREATE all target
        # it). Per-node in_memory > materialized read precedence is resolved in the
        # engine (consumable_location), not by search-path order.
        self._search_path = f'"{self._catalog}".main,{SCRATCH_CATALOG}.main'
        self._set_search_path(conn)

    def _set_search_path(self, cur):
        """search_path is connection-local, so every cursor must pin it itself."""
        if self._search_path:
            cur.execute(f"SET search_path = '{self._search_path}'")

    def _catalog_for(self, into_memory: bool) -> str:
        return SCRATCH_CATALOG if into_memory else self._catalog

    def _apply_settings(self):
        if self._memory_limit:
            self.conn.execute(f"SET memory_limit = '{self._memory_limit}'")
        if self._temp_directory and self._is_file_backed:
            Path(self._temp_directory).mkdir(parents=True, exist_ok=True)
            self.conn.execute(f"SET temp_directory = '{self._temp_directory}'")

    def _ensure_meta_schema(self):
        self.conn.execute(f"CREATE TABLE IF NOT EXISTS {self._meta_ref} ({_META_TABLE_DDL})")
        self._migrate_meta_to_per_location()

    def _migrate_meta_to_per_location(self):
        """Rebuild a legacy meta table (keyed by node_id alone, one location per
        node) with a composite (node_id, location) primary key so in-memory and
        materialized copies are tracked independently."""
        pk = self.conn.execute(
            "SELECT constraint_column_names FROM duckdb_constraints() "
            "WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'",
            [META_TABLE],
        ).fetchone()
        if pk is not None and set(pk[0]) == {"node_id", "location"}:
            return
        # Legacy single-column PK: backfill location (pre-in-memory rows were all
        # on disk), then rebuild with the composite key.
        self.conn.execute(f"ALTER TABLE {self._meta_ref} ADD COLUMN IF NOT EXISTS location VARCHAR")
        self.conn.execute(
            f"UPDATE {self._meta_ref} SET location = ? WHERE location IS NULL",
            [LOCATION_MATERIALIZED],
        )
        migrate_ref = _qualified(self._catalog, META_TABLE + "__migrate")
        self.conn.execute(f"DROP TABLE IF EXISTS {migrate_ref}")
        self.conn.execute(f"CREATE TABLE {migrate_ref} ({_META_TABLE_DDL})")
        self.conn.execute(
            f"INSERT INTO {migrate_ref} ({', '.join(META_COLUMNS)}) "
            f"SELECT {', '.join(META_COLUMNS)} FROM {self._meta_ref}"
        )
        self.conn.execute(f"DROP TABLE {self._meta_ref}")
        self.conn.execute(f"ALTER TABLE {migrate_ref} RENAME TO {_quote_identifier(META_TABLE)}")

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
            f"UPDATE {self._meta_ref} SET status = 'failed', "
            "error = 'Load was interrupted (backend stopped mid-load)', "
            "finished_at = ? WHERE status = 'loading'",
            [utc_now_iso()],
        )

    @contextmanager
    def _cursor(self):
        with self._track():
            cur = self.conn.cursor()
            self._set_search_path(cur)
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

    def begin_load(
        self,
        node_id: str,
        table_name: str,
        cache_key: str | None = None,
        *,
        into_memory: bool = False,
    ) -> "StagingLoad":
        return StagingLoad(self, node_id, table_name, cache_key, into_memory=into_memory)

    def register_csv(
        self,
        table_name: str,
        file_path: str,
        *,
        node_id: str | None = None,
        cache_key: str | None = None,
        into_memory: bool = False,
        register_interrupt=None,
    ) -> dict:
        load = self.begin_load(node_id or table_name, table_name, cache_key, into_memory=into_memory)
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

    def register_parquet(
        self,
        table_name: str,
        file_path: str,
        *,
        node_id: str | None = None,
        cache_key: str | None = None,
        into_memory: bool = False,
    ) -> dict:
        """Load a Parquet file (e.g. a cached preprocessing artifact) as a node table."""
        load = self.begin_load(node_id or table_name, table_name, cache_key, into_memory=into_memory)
        try:
            if node_id is not None:
                load.mark_loading()
            load.create_staging_as("SELECT * FROM read_parquet(?)", [file_path])
            return load.commit(record_meta=node_id is not None)
        except BaseException as exc:
            load.abort(str(exc), record_meta=node_id is not None)
            raise

    def register_excel(
        self,
        table_name: str,
        file_path: str,
        *,
        sheet: str | None = None,
        cell_range: str | None = None,
        header: bool = True,
        all_varchar: bool = False,
        node_id: str | None = None,
        cache_key: str | None = None,
        into_memory: bool = False,
        register_interrupt=None,
    ) -> dict:
        self.ensure_excel_extension()
        params: list = [file_path]
        options = ["header => ?"]
        params.append(header)
        if sheet:
            options.append("sheet => ?")
            params.append(sheet)
        if cell_range:
            options.append("range => ?")
            params.append(cell_range)
        if all_varchar:
            options.append("all_varchar => ?")
            params.append(all_varchar)
        select_sql = f"SELECT * FROM read_xlsx(?, {', '.join(options)})"
        load = self.begin_load(node_id or table_name, table_name, cache_key, into_memory=into_memory)
        try:
            if node_id is not None:
                load.mark_loading()
            if register_interrupt is not None:
                register_interrupt(load.interrupt)
            load.create_staging_as(select_sql, params)
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
        into_memory: bool = False,
    ) -> dict:
        load = self.begin_load(node_id or table_name, table_name, cache_key, into_memory=into_memory)
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
        into_memory: bool = False,
        register_interrupt=None,
        upstream_resolution: dict[str, str] | None = None,
    ) -> dict:
        load = self.begin_load(node_id or table_name, table_name, cache_key, into_memory=into_memory)
        try:
            if node_id is not None:
                load.mark_loading()
            if register_interrupt is not None:
                register_interrupt(load.interrupt)
            run_schema = self._install_upstream_views(load, upstream_resolution)
            try:
                load.create_staging_as(f"({sql})")
            finally:
                if run_schema is not None:
                    self._drop_run_schema(load, run_schema)
            return load.commit(record_meta=node_id is not None)
        except BaseException as exc:
            load.abort(str(exc), record_meta=node_id is not None)
            raise

    def _install_upstream_views(self, load: "StagingLoad", resolution: dict[str, str] | None) -> str | None:
        """Shadow each resolved upstream table with a run-scoped view over the
        precedence-chosen copy, so the transform's unqualified reads resolve to
        the right catalog regardless of the project(disk)-first search path.

        The views live in a per-run schema in the RAM-only scratch catalog and
        are dropped as soon as the staging table is built. Returns the schema
        name, or None when there's nothing to override."""
        if not resolution:
            return None
        schema = f"{RESERVED_TABLE_PREFIX}run_{uuid4().hex}"
        cur = load._cur
        cur.execute(f"CREATE SCHEMA {SCRATCH_CATALOG}.{schema}")
        for table_name, location in resolution.items():
            catalog = self._catalog_for(location == LOCATION_MEMORY)
            cur.execute(
                f"CREATE VIEW {SCRATCH_CATALOG}.{schema}.{_quote_identifier(table_name)} "
                f"AS SELECT * FROM {_qualified(catalog, table_name)}"
            )
        # Run schema first so its views win over the same-named base tables.
        cur.execute(f"SET search_path = '{SCRATCH_CATALOG}.{schema},{self._search_path}'")
        return schema

    def _drop_run_schema(self, load: "StagingLoad", schema: str) -> None:
        try:
            load._cur.execute(f"DROP SCHEMA IF EXISTS {SCRATCH_CATALOG}.{schema} CASCADE")
            load._cur.execute(f"SET search_path = '{self._search_path}'")
        except Exception:
            logger.warning("Failed to drop run schema %s", schema, exc_info=True)

    def append_dataframe(self, table_name: str, df) -> dict:
        with self._cursor() as cur:
            quoted = _quote_identifier(table_name)
            cur.register("_shori_append_src", df)
            try:
                cur.execute(f"INSERT INTO {quoted} SELECT * FROM _shori_append_src")
            finally:
                cur.unregister("_shori_append_src")
            return _table_stats(cur, quoted)

    # ------------------------------------------------------------------
    # Node metadata
    # ------------------------------------------------------------------

    def upsert_node_meta(self, **fields) -> None:
        with self._cursor() as cur:
            _upsert_meta(cur, fields, self._meta_ref)

    def get_node_meta(self, node_id: str, location: str | None = None) -> dict | None:
        """One node's meta row. With `location`, that specific copy; without, the
        precedence-preferred present copy (in_memory over materialized)."""
        locations = self.get_node_locations(node_id)
        if not locations:
            return None
        if location is not None:
            return locations.get(location)
        return locations.get(LOCATION_MEMORY) or locations.get(LOCATION_MATERIALIZED)

    def get_node_locations(self, node_id: str) -> dict[str, dict]:
        """Every persisted copy of a node, keyed by location."""
        with self._cursor() as cur:
            rows = cur.execute(
                f"SELECT {', '.join(META_COLUMNS)} FROM {self._meta_ref} WHERE node_id = ?",
                [node_id],
            ).fetchall()
        return {meta["location"]: meta for meta in (_meta_row_to_dict(r) for r in rows)}

    def consumable_location(self, node_id: str, cache_key: str | None) -> str | None:
        """Which persisted copy a downstream node should read (spec §6 precedence):
        fresh over stale, then in_memory over materialized, then most recently
        built. Returns the location, or None if no present copy exists.

        This is what lets a fresh in-memory copy win over a stale materialized one
        even though the default search path is project(disk)-first."""
        best_location: str | None = None
        best_rank: tuple | None = None
        for location, meta in self.get_node_locations(node_id).items():
            if meta["status"] != "complete":
                continue
            if not self.table_exists(meta["table_name"], location=location):
                continue
            fresh = cache_key is not None and meta["cache_key"] == cache_key
            rank = (
                1 if fresh else 0,
                1 if location == LOCATION_MEMORY else 0,
                meta.get("finished_at") or "",
            )
            if best_rank is None or rank > best_rank:
                best_rank, best_location = rank, location
        return best_location

    def all_node_meta(self) -> dict[str, dict]:
        """One representative row per node (precedence-preferred present copy).
        For callers that only need per-node identity (orphan cleanup, rename)."""
        return {
            node_id: (locs.get(LOCATION_MEMORY) or locs.get(LOCATION_MATERIALIZED))
            for node_id, locs in self.all_node_locations().items()
        }

    def all_node_locations(self) -> dict[str, dict[str, dict]]:
        """Every persisted copy, nested as {node_id: {location: meta}}."""
        with self._cursor() as cur:
            rows = cur.execute(
                f"SELECT {', '.join(META_COLUMNS)} FROM {self._meta_ref}"
            ).fetchall()
        nested: dict[str, dict[str, dict]] = {}
        for row in rows:
            meta = _meta_row_to_dict(row)
            nested.setdefault(meta["node_id"], {})[meta["location"]] = meta
        return nested

    def drop_node(self, node_id: str, location: str | None = None) -> bool:
        """Drop a node's table(s) and metadata. With `location`, only that copy;
        without, every copy. Returns True if anything existed."""
        locations = self.get_node_locations(node_id)
        targets = [location] if location is not None else list(locations.keys())
        dropped = False
        with self._cursor() as cur:
            for loc in targets:
                meta = locations.get(loc)
                if meta is None:
                    continue
                dropped = True
                catalog = self._catalog_for(loc == LOCATION_MEMORY)
                name = meta["table_name"]
                cur.execute(f"DROP TABLE IF EXISTS {_qualified(catalog, name)}")
                cur.execute(f"DROP TABLE IF EXISTS {_qualified(catalog, name + STAGING_SUFFIX)}")
                cur.execute(
                    f"DELETE FROM {self._meta_ref} WHERE node_id = ? AND location = ?",
                    [node_id, loc],
                )
        return dropped

    def rename_node_table(self, node_id: str, new_table_name: str) -> bool:
        """Rename a node's table(s), preserving data and cache validity. A node
        shares one table_name across its locations, so every copy is renamed."""
        validate_user_table_name(new_table_name)
        locations = self.get_node_locations(node_id)
        if not locations:
            return False
        old_name = next(iter(locations.values()))["table_name"]
        if old_name == new_table_name:
            return False
        with self._cursor() as cur:
            # Rename the physical table in every catalog that holds a copy. A
            # scratch rename and the project meta update touch different databases,
            # so they can't share a transaction; each is idempotent on retry.
            for loc in locations:
                catalog = self._catalog_for(loc == LOCATION_MEMORY)
                if _table_exists(cur, old_name, catalog):
                    cur.execute(f"DROP TABLE IF EXISTS {_qualified(catalog, new_table_name)}")
                    cur.execute(
                        f"ALTER TABLE {_qualified(catalog, old_name)} "
                        f"RENAME TO {_quote_identifier(new_table_name)}"
                    )
            cur.execute("BEGIN TRANSACTION")
            try:
                cur.execute(
                    f"UPDATE {self._meta_ref} SET table_name = ? WHERE node_id = ?",
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

    def copy_table_to(self, table_name: str, output_path: str, fmt: str):
        """Write a node table to a local path as CSV, Parquet, or XLSX.

        The table reference is unqualified so search_path resolves it in whichever
        catalog holds it (in-memory tables export the same as materialized ones).
        """
        fmt = fmt.lower()
        copy_options = {
            "csv": "(FORMAT csv, HEADER)",
            "parquet": "(FORMAT parquet)",
            "xlsx": "(FORMAT xlsx, HEADER true)",
        }.get(fmt)
        if copy_options is None:
            raise ValueError(f"Unsupported export format '{fmt}'")
        if fmt == "xlsx":
            self.ensure_excel_extension()
        safe_path = output_path.replace("'", "''")
        with self._cursor() as cur:
            cur.execute(
                f"COPY {_quote_identifier(table_name)} TO '{safe_path}' {copy_options}"
            )

    def drop_table(self, table_name: str):
        if table_name.startswith(RESERVED_TABLE_PREFIX):
            raise ValueError(f"Cannot drop internal table '{table_name}'")
        with self._cursor() as cur:
            for catalog in (self._catalog, SCRATCH_CATALOG):
                cur.execute(f"DROP TABLE IF EXISTS {_qualified(catalog, table_name)}")
            cur.execute(f"DELETE FROM {self._meta_ref} WHERE table_name = ?", [table_name])

    def table_exists(self, table_name: str, *, location: str | None = None) -> bool:
        catalog = None if location is None else self._catalog_for(location == LOCATION_MEMORY)
        with self._cursor() as cur:
            return _table_exists(cur, table_name, catalog)

    def table_stats(self, table_name: str) -> dict:
        with self._cursor() as cur:
            return _table_stats(cur, _quote_identifier(table_name))

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
            # Use the pinned project catalog (robust regardless of search_path).
            self.conn.execute(f"ATTACH '{tmp_path}' AS _shori_compact_target")
            self.conn.execute(
                f"COPY FROM DATABASE {_quote_identifier(self._catalog)} TO _shori_compact_target"
            )
            self.conn.execute("DETACH _shori_compact_target")
            self.conn.close()
            os.replace(tmp_path, self.db_path)
            for leftover in (self.db_path + ".wal", tmp_path + ".wal"):
                if os.path.exists(leftover):
                    os.remove(leftover)
            self.conn = duckdb.connect(self.db_path)
            self._attach_scratch(self.conn)
            self._apply_settings()
            self._excel_extension_ready = None
            self._postgres_extension_ready = None
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

    def ensure_excel_extension(self) -> None:
        """Install/load DuckDB's excel extension once per manager (for read_xlsx)."""
        if self._excel_extension_ready:
            return
        with self._cursor() as cur:
            cur.execute("INSTALL excel")
            cur.execute("LOAD excel")
        self._excel_extension_ready = True

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

    def __init__(
        self,
        manager: DuckDBManager,
        node_id: str,
        table_name: str,
        cache_key: str | None,
        *,
        into_memory: bool = False,
    ):
        validate_user_table_name(table_name)
        self.node_id = node_id
        self.table_name = table_name
        self.cache_key = cache_key
        self.into_memory = into_memory
        self.location = LOCATION_MEMORY if into_memory else LOCATION_MATERIALIZED
        self.staging_name = table_name + STAGING_SUFFIX
        self.started_at = utc_now_iso()
        self._manager = manager
        # In-memory loads land in the scratch catalog; materialized in the project
        # file. Staging + final tables are catalog-qualified so the swap stays
        # inside the chosen catalog regardless of search_path.
        self._catalog = manager._catalog_for(into_memory)
        self._staging_ref = _qualified(self._catalog, self.staging_name)
        self._table_ref = _qualified(self._catalog, self.table_name)
        self._track = manager._track()
        self._track.__enter__()
        self._cur = manager.conn.cursor()
        manager._set_search_path(self._cur)
        self._created = False
        self._finished = False
        try:
            self._cur.execute(f"DROP TABLE IF EXISTS {self._staging_ref}")
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
                "location": self.location,
                "started_at": self.started_at,
            },
            self._manager._meta_ref,
        )

    def append(self, data) -> None:
        """Append a chunk: a pandas DataFrame, Arrow table/batch, or any object
        DuckDB can scan (including Arrow PyCapsule streams)."""
        self._cur.register("_shori_chunk_src", data)
        try:
            if not self._created:
                self._cur.execute(
                    f"CREATE TABLE {self._staging_ref} AS SELECT * FROM _shori_chunk_src"
                )
                self._created = True
            else:
                self._cur.execute(
                    f"INSERT INTO {self._staging_ref} SELECT * FROM _shori_chunk_src"
                )
        finally:
            self._cur.unregister("_shori_chunk_src")

    def create_staging_as(self, select_sql: str, params: list | None = None) -> None:
        """Create the staging table directly from a SELECT (CSV scan, transform, ATTACH)."""
        if self._created:
            raise RuntimeError("Staging table already created")
        self._cur.execute(
            f"CREATE TABLE {self._staging_ref} AS {select_sql}",
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
            stats = _table_stats(self._cur, self._staging_ref)
            finished_at = utc_now_iso()
            duration_ms = (
                datetime.fromisoformat(finished_at) - datetime.fromisoformat(self.started_at)
            ).total_seconds() * 1000
            meta = (
                {
                    "node_id": self.node_id,
                    "table_name": self.table_name,
                    "cache_key": self.cache_key,
                    "status": "complete",
                    "location": self.location,
                    "row_count": stats["row_count"],
                    "column_count": stats["column_count"],
                    "columns_json": json.dumps(stats["columns"]),
                    "started_at": self.started_at,
                    "finished_at": finished_at,
                    "duration_ms": duration_ms,
                }
                if record_meta
                else None
            )
            # In-memory and materialized copies of a node coexist — each tracked by
            # its own meta row — so a load into one location no longer drops the
            # other. The scratch swap and the (project) meta write hit different
            # databases, which DuckDB forbids in one transaction.
            if self.into_memory:
                # The scratch swap and the (project) meta write hit different
                # databases — DuckDB forbids that in one transaction — so the
                # swap is atomic on its own and the meta row follows it.
                self._run_swap_txn()
                if meta is not None:
                    _upsert_meta(self._cur, meta, self._manager._meta_ref)
            else:
                self._cur.execute("BEGIN TRANSACTION")
                try:
                    self._apply_swap()
                    if meta is not None:
                        _upsert_meta(self._cur, meta, self._manager._meta_ref)
                    self._cur.execute("COMMIT")
                except BaseException:
                    self._cur.execute("ROLLBACK")
                    raise
            return stats
        finally:
            self._finished = True
            self._cleanup()

    def _apply_swap(self):
        self._cur.execute(f"DROP TABLE IF EXISTS {self._table_ref}")
        self._cur.execute(
            f"ALTER TABLE {self._staging_ref} RENAME TO {_quote_identifier(self.table_name)}"
        )

    def _run_swap_txn(self):
        self._cur.execute("BEGIN TRANSACTION")
        try:
            self._apply_swap()
            self._cur.execute("COMMIT")
        except BaseException:
            self._cur.execute("ROLLBACK")
            raise

    def abort(self, error: str | None = None, record_meta: bool = True) -> None:
        if self._finished:
            return
        self._finished = True
        try:
            self._cur.execute(f"DROP TABLE IF EXISTS {self._staging_ref}")
            if record_meta:
                _upsert_meta(
                    self._cur,
                    {
                        "node_id": self.node_id,
                        "table_name": self.table_name,
                        "cache_key": self.cache_key,
                        "status": "failed",
                        "location": self.location,
                        "error": error,
                        "started_at": self.started_at,
                        "finished_at": utc_now_iso(),
                    },
                    self._manager._meta_ref,
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


def _upsert_meta(cur, fields: dict, meta_ref: str) -> None:
    row = {column: None for column in META_COLUMNS}
    row.update({key: value for key, value in fields.items() if key in row})
    placeholders = ", ".join("?" for _ in META_COLUMNS)
    cur.execute(
        f"INSERT OR REPLACE INTO {meta_ref} ({', '.join(META_COLUMNS)}) "
        f"VALUES ({placeholders})",
        [row[column] for column in META_COLUMNS],
    )


def _meta_row_to_dict(row: tuple) -> dict:
    meta = dict(zip(META_COLUMNS, row))
    meta["columns"] = json.loads(meta.pop("columns_json")) if meta.get("columns_json") else None
    return meta


def _qualified(catalog: str, name: str) -> str:
    """Build a fully catalog-qualified `"catalog".main."name"` reference."""
    return f"{_quote_identifier(catalog)}.main.{_quote_identifier(name)}"


def _table_exists(cur, table_name: str, catalog: str | None = None) -> bool:
    # information_schema.tables spans every attached catalog (project + scratch),
    # so the unqualified check is true if the table lives in either one.
    if catalog is None:
        result = cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()
    else:
        result = cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_name = ?",
            [catalog, table_name],
        ).fetchone()
    return result[0] > 0


def _table_stats(cur, ref: str) -> dict:
    """Stats for an already-quoted/qualified table reference."""
    count = cur.execute(f"SELECT COUNT(*) FROM {ref}").fetchone()[0]
    cols = cur.execute(f"DESCRIBE {ref}").fetchall()
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
