import asyncio
import logging
from uuid import uuid4

import asyncpg
import pandas as pd

logger = logging.getLogger(__name__)


class PostgresAttachUnavailable(Exception):
    """DuckDB's postgres extension can't be used; fall back to the driver."""


def _attach_string(conn_config: dict) -> str:
    def esc(value) -> str:
        return str(value).replace("\\", "\\\\").replace("'", "\\'")

    return (
        f"host='{esc(conn_config['host'])}' port={int(conn_config['port'])} "
        f"dbname='{esc(conn_config['database'])}' user='{esc(conn_config['user'])}' "
        f"password='{esc(conn_config['password'])}'"
    )


def _dollar_quote(query: str) -> str:
    tag = "shori_q"
    while f"${tag}$" in query:
        tag += "x"
    return f"${tag}$ {query} ${tag}$"


class PostgresService:
    async def connect(self, config: dict) -> asyncpg.Connection:
        conn_config = config["connection"]
        return await asyncpg.connect(
            host=conn_config["host"],
            port=conn_config["port"],
            database=conn_config["database"],
            user=conn_config["user"],
            password=conn_config["password"],
        )

    async def fetch_query(self, connection: asyncpg.Connection, query: str) -> pd.DataFrame:
        rows = await connection.fetch(query)
        if not rows:
            return pd.DataFrame()
        columns = list(rows[0].keys())
        data = [list(row.values()) for row in rows]
        return pd.DataFrame(data, columns=columns)

    async def load_query_to_duckdb(
        self,
        conn_config: dict,
        query: str,
        table_name: str,
        duckdb_manager,
        *,
        node_id: str | None = None,
        cache_key: str | None = None,
        register_interrupt=None,
    ) -> dict:
        """Materialize the query without the data passing through Python:
        DuckDB attaches the source database and runs CREATE TABLE AS itself."""
        return await asyncio.to_thread(
            lambda: self._load_via_duckdb_attach_sync(
                conn_config,
                query,
                table_name,
                duckdb_manager,
                node_id=node_id,
                cache_key=cache_key,
                register_interrupt=register_interrupt,
            )
        )

    def _load_via_duckdb_attach_sync(
        self,
        conn_config: dict,
        query: str,
        table_name: str,
        duckdb_manager,
        *,
        node_id: str | None = None,
        cache_key: str | None = None,
        register_interrupt=None,
    ) -> dict:
        if not duckdb_manager.ensure_postgres_extension():
            raise PostgresAttachUnavailable()

        record_meta = node_id is not None
        alias = f"_shori_pg_{uuid4().hex[:8]}"
        load = duckdb_manager.begin_load(node_id or table_name, table_name, cache_key)
        try:
            if record_meta:
                load.mark_loading()
            if register_interrupt is not None:
                register_interrupt(load.interrupt)
            load.execute("LOAD postgres")
            # Two escaping layers: _attach_string already libpq-escapes values
            # (which leaves single quotes around them); double those quotes so
            # the whole conninfo survives as one SQL string literal.
            attach_literal = _attach_string(conn_config).replace("'", "''")
            load.execute(f"ATTACH '{attach_literal}' AS {alias} (TYPE postgres, READ_ONLY)")
            try:
                load.create_staging_as(
                    f"SELECT * FROM postgres_query('{alias}', {_dollar_quote(query)})"
                )
            finally:
                try:
                    load.execute(f"DETACH {alias}")
                except Exception:
                    logger.warning("Failed to detach %s", alias, exc_info=True)
            return load.commit(record_meta=record_meta)
        except BaseException as exc:
            load.abort(str(exc), record_meta=record_meta)
            raise

    async def execute_query(self, config: dict) -> pd.DataFrame:
        connection = await self.connect(config)
        try:
            return await self.fetch_query(connection, config["query"])
        finally:
            await connection.close()

    def abort_query(self, connection: asyncpg.Connection) -> None:
        if not connection.is_closed():
            connection.terminate()

    async def test_connection(self, config: dict) -> bool:
        connection = await asyncpg.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
        )
        await connection.close()
        return True
