"""Connection pools per saved database connection.

One Oracle session (or Postgres connection) can run only one query at a
time, so concurrent extraction needs multiple sessions per database. Pools
amortize Oracle's expensive connection setup across node runs, and the pool
max doubles as the per-database concurrency cap: an extraction that can't
get a session waits instead of piling more load onto the source database.
"""

import asyncio
from collections.abc import Awaitable, Callable
import hashlib
import logging

import asyncpg
import oracledb

from app.services.oracle_service import OracleService

logger = logging.getLogger(__name__)

DEFAULT_MAX_CONNECTIONS_PER_DATABASE = 2


def _identity(db_type: str, conn_config: dict) -> tuple:
    # Password is hashed into the key so rotated credentials get a new pool
    # instead of reusing sessions authenticated with the old ones.
    password_hash = hashlib.sha256(
        str(conn_config.get("password", "")).encode("utf-8")
    ).hexdigest()
    return (
        db_type,
        conn_config.get("host"),
        conn_config.get("port"),
        conn_config.get("service_name") or conn_config.get("database"),
        conn_config.get("user"),
        password_hash,
    )


class ConnectionPoolRegistry:
    """db connection identity -> pool, created lazily on first acquire."""

    def __init__(self, max_connections_per_database: int = DEFAULT_MAX_CONNECTIONS_PER_DATABASE):
        self.max_connections_per_database = max_connections_per_database
        self._oracle_pools: dict[tuple, oracledb.ConnectionPool] = {}
        self._postgres_pools: dict[tuple, asyncpg.Pool] = {}
        self._lock = asyncio.Lock()

    async def acquire(
        self, db_type: str, conn_config: dict, max_size: int | None = None
    ) -> tuple[object, Callable[[], Awaitable[None]]]:
        """Returns (connection, release). Always await release() when done —
        it returns the session to the pool rather than closing it."""
        if db_type == "oracle":
            return await self._acquire_oracle(conn_config, max_size)
        return await self._acquire_postgres(conn_config, max_size)

    async def _acquire_oracle(self, conn_config: dict, max_size: int | None):
        key = _identity("oracle", conn_config)
        async with self._lock:
            pool = self._oracle_pools.get(key)
            if pool is None:
                pool = await asyncio.to_thread(self._create_oracle_pool, conn_config, max_size)
                self._oracle_pools[key] = pool
        connection = await asyncio.to_thread(pool.acquire)

        async def release():
            # Closing a pooled oracledb connection returns it to the pool.
            try:
                await asyncio.to_thread(connection.close)
            except oracledb.Error:
                logger.warning("Failed to release oracle connection", exc_info=True)

        return connection, release

    def _create_oracle_pool(self, conn_config: dict, max_size: int | None):
        OracleService._ensure_thick_mode()
        dsn = oracledb.makedsn(
            conn_config["host"],
            conn_config["port"],
            service_name=conn_config["service_name"],
        )
        return oracledb.create_pool(
            user=conn_config["user"],
            password=conn_config["password"],
            dsn=dsn,
            min=0,
            max=max_size or self.max_connections_per_database,
            increment=1,
        )

    async def _acquire_postgres(self, conn_config: dict, max_size: int | None):
        key = _identity("postgres", conn_config)
        async with self._lock:
            pool = self._postgres_pools.get(key)
            if pool is None:
                pool = await asyncpg.create_pool(
                    host=conn_config["host"],
                    port=conn_config["port"],
                    database=conn_config["database"],
                    user=conn_config["user"],
                    password=conn_config["password"],
                    min_size=0,
                    max_size=max_size or self.max_connections_per_database,
                )
                self._postgres_pools[key] = pool
        connection = await pool.acquire()

        async def release():
            try:
                await pool.release(connection)
            except Exception:
                logger.warning("Failed to release postgres connection", exc_info=True)

        return connection, release

    async def close_all(self):
        async with self._lock:
            oracle_pools = list(self._oracle_pools.values())
            postgres_pools = list(self._postgres_pools.values())
            self._oracle_pools.clear()
            self._postgres_pools.clear()
        for pool in oracle_pools:
            try:
                await asyncio.to_thread(pool.close, True)
            except Exception:
                logger.warning("Failed to close oracle pool", exc_info=True)
        for pool in postgres_pools:
            try:
                pool.terminate()
            except Exception:
                logger.warning("Failed to close postgres pool", exc_info=True)
