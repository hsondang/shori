"""DBeaver-style live preview sessions for database source nodes.

A session executes the node's query on a pooled connection and keeps the
cursor open server-side. The frontend pages through results chunk by chunk
(scroll-to-load); no DuckDB table is created. All fetched rows stay buffered
in memory (typed, not JSON-serialized) so a later "materialize" can stream
the buffer into the project's DuckDB and continue draining the same cursor —
the query never reruns.
"""

import asyncio
import logging
import time
from uuid import uuid4

import pandas as pd

from app.models.pipeline import NodeDefinition
from app.services.duckdb_manager import _json_safe_value

logger = logging.getLogger(__name__)

DEFAULT_PREVIEW_CHUNK_ROWS = 200
DEFAULT_MAX_BUFFER_ROWS = 10_000
DEFAULT_SESSION_TTL_SECONDS = 600
REAPER_INTERVAL_SECONDS = 30


class PreviewSessionError(Exception):
    pass


class PreviewSessionNotFound(PreviewSessionError):
    pass


class PreviewSession:
    def __init__(
        self,
        *,
        project_id: str,
        node: NodeDefinition,
        cache_key: str | None,
        db_type: str,
        connection,
        release,
        chunk_rows: int,
        max_buffer_rows: int,
        ttl_seconds: int,
    ):
        self.session_id = uuid4().hex
        self.project_id = project_id
        self.node = node
        self.cache_key = cache_key
        self.db_type = db_type
        self.connection = connection
        self._release = release
        self.chunk_rows = chunk_rows
        self.max_buffer_rows = max_buffer_rows
        self.ttl_seconds = ttl_seconds
        self.columns: list[str] = []
        self.column_types: list[str] = []
        self.buffer: list[tuple] = []  # typed rows, used for materialize
        self.exhausted = False
        self.closed = False
        self.last_used = time.monotonic()
        self.lock = asyncio.Lock()
        # driver state
        self._oracle_cursor = None
        self._pg_transaction = None
        self._pg_cursor = None

    @property
    def buffer_capped(self) -> bool:
        return len(self.buffer) >= self.max_buffer_rows

    def touch(self):
        self.last_used = time.monotonic()

    def expired(self, now: float) -> bool:
        return now - self.last_used > self.ttl_seconds

    # ------------------------------------------------------------------

    async def open(self):
        query = self.node.config["query"]
        if self.db_type == "oracle":
            def _open_sync():
                cursor = self.connection.cursor()
                cursor.arraysize = self.chunk_rows
                cursor.execute(query)
                columns = [col[0] for col in cursor.description]
                column_types = [self._oracle_type_name(col) for col in cursor.description]
                return cursor, columns, column_types

            self._oracle_cursor, self.columns, self.column_types = await asyncio.to_thread(_open_sync)
        else:
            # asyncpg cursors only live inside a transaction; preparing first
            # gives us the column schema even for empty results.
            self._pg_transaction = self.connection.transaction()
            await self._pg_transaction.start()
            statement = await self.connection.prepare(query)
            self.columns = [attr.name for attr in statement.get_attributes()]
            self.column_types = [attr.type.name for attr in statement.get_attributes()]
            self._pg_cursor = await statement.cursor()

    @staticmethod
    def _oracle_type_name(description_entry) -> str:
        try:
            return description_entry[1].name
        except Exception:
            return str(description_entry[1])

    async def fetch_chunk(self) -> list[tuple]:
        """Fetch the next chunk from the open cursor into the buffer."""
        if self.exhausted:
            return []
        if self.db_type == "oracle":
            rows = await asyncio.to_thread(self._oracle_cursor.fetchmany, self.chunk_rows)
            rows = [tuple(row) for row in rows]
        else:
            records = await self._pg_cursor.fetch(self.chunk_rows)
            rows = [tuple(record.values()) for record in records]
        if len(rows) < self.chunk_rows:
            self.exhausted = True
        self.buffer.extend(rows)
        return rows

    async def drain_into_load(self, load, *, chunk_rows: int | None = None):
        """Stream remaining cursor rows into a staging load (post-buffer)."""
        size = chunk_rows or self.chunk_rows
        while not self.exhausted:
            if self.db_type == "oracle":
                rows = await asyncio.to_thread(self._oracle_cursor.fetchmany, size)
                rows = [tuple(row) for row in rows]
            else:
                records = await self._pg_cursor.fetch(size)
                rows = [tuple(record.values()) for record in records]
            if len(rows) < size:
                self.exhausted = True
            if rows:
                await asyncio.to_thread(
                    load.append, pd.DataFrame(rows, columns=self.columns)
                )

    def interrupt(self):
        """Abort an in-flight fetch (used when a materialize run is cancelled)."""
        try:
            if self.db_type == "oracle":
                self.connection.cancel()
            else:
                self.connection.terminate()
        except Exception:
            logger.warning("Failed to interrupt preview session %s", self.session_id, exc_info=True)

    async def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            if self._oracle_cursor is not None:
                await asyncio.to_thread(self._oracle_cursor.close)
            if self._pg_cursor is not None and self._pg_transaction is not None:
                try:
                    await self._pg_transaction.rollback()
                except Exception:
                    pass
        except Exception:
            logger.warning("Error closing preview session cursor", exc_info=True)
        finally:
            try:
                await self._release()
            except Exception:
                logger.warning("Error releasing preview session connection", exc_info=True)

    def json_rows(self, rows: list[tuple]) -> list[list]:
        return [[_json_safe_value(value) for value in row] for row in rows]


class PreviewSessionManager:
    def __init__(self, connection_pools):
        self._pools = connection_pools
        self._sessions: dict[str, PreviewSession] = {}
        self._lock = asyncio.Lock()
        self._reaper_task: asyncio.Task | None = None

    async def start(
        self,
        *,
        project_id: str,
        node: NodeDefinition,
        cache_key: str | None,
        chunk_rows: int = DEFAULT_PREVIEW_CHUNK_ROWS,
        max_buffer_rows: int = DEFAULT_MAX_BUFFER_ROWS,
        ttl_seconds: int = DEFAULT_SESSION_TTL_SECONDS,
        max_connections_per_database: int | None = None,
    ) -> dict:
        db_type = node.config.get("db_type", "postgres")
        connection, release = await self._pools.acquire(
            db_type, node.config["connection"], max_connections_per_database
        )
        session = PreviewSession(
            project_id=project_id,
            node=node,
            cache_key=cache_key,
            db_type=db_type,
            connection=connection,
            release=release,
            chunk_rows=chunk_rows,
            max_buffer_rows=max_buffer_rows,
            ttl_seconds=ttl_seconds,
        )
        try:
            await session.open()
            rows = await session.fetch_chunk()
        except Exception:
            await session.close()
            raise
        async with self._lock:
            self._sessions[session.session_id] = session
            self._ensure_reaper()
        return {
            "session_id": session.session_id,
            "node_id": node.id,
            "columns": session.columns,
            "column_types": session.column_types,
            "rows": session.json_rows(rows),
            "buffered_rows": len(session.buffer),
            "has_more": not session.exhausted,
            "buffer_capped": session.buffer_capped,
        }

    async def _get(self, session_id: str) -> PreviewSession:
        async with self._lock:
            session = self._sessions.get(session_id)
        if session is None or session.closed:
            raise PreviewSessionNotFound(f"Preview session '{session_id}' not found or expired")
        return session

    async def get_session(self, session_id: str) -> PreviewSession:
        return await self._get(session_id)

    async def fetch_more(self, session_id: str) -> dict:
        session = await self._get(session_id)
        async with session.lock:
            session.touch()
            if session.buffer_capped and not session.exhausted:
                return {
                    "session_id": session_id,
                    "rows": [],
                    "buffered_rows": len(session.buffer),
                    "has_more": True,
                    "buffer_capped": True,
                }
            rows = await session.fetch_chunk()
            session.touch()
            return {
                "session_id": session_id,
                "rows": session.json_rows(rows),
                "buffered_rows": len(session.buffer),
                "has_more": not session.exhausted,
                "buffer_capped": session.buffer_capped,
            }

    async def materialize(
        self,
        session_id: str,
        duckdb_manager,
        *,
        register_interrupt=None,
    ) -> dict:
        """Stream buffered rows into the node's table, then keep draining the
        same open cursor to completion. The query is never re-executed."""
        session = await self._get(session_id)
        async with session.lock:
            session.touch()
            node = session.node
            load = duckdb_manager.begin_load(node.id, node.table_name, session.cache_key)
            try:
                load.mark_loading()
                if register_interrupt is not None:
                    register_interrupt(session.interrupt)
                buffered = pd.DataFrame(session.buffer, columns=session.columns)
                await asyncio.to_thread(load.append, buffered)
                await session.drain_into_load(load)
                stats = await asyncio.to_thread(load.commit)
            except BaseException as exc:
                await asyncio.to_thread(load.abort, str(exc))
                raise
            finally:
                await self._remove_and_close(session)
            return stats

    async def close(self, session_id: str) -> bool:
        async with self._lock:
            session = self._sessions.pop(session_id, None)
        if session is None:
            return False
        await session.close()
        return True

    async def _remove_and_close(self, session: PreviewSession):
        async with self._lock:
            self._sessions.pop(session.session_id, None)
        await session.close()

    def _ensure_reaper(self):
        if self._reaper_task is None or self._reaper_task.done():
            self._reaper_task = asyncio.create_task(self._reap_expired())

    async def _reap_expired(self):
        while True:
            await asyncio.sleep(REAPER_INTERVAL_SECONDS)
            now = time.monotonic()
            async with self._lock:
                expired = [s for s in self._sessions.values() if s.expired(now)]
                for session in expired:
                    self._sessions.pop(session.session_id, None)
                if not self._sessions and not expired:
                    self._reaper_task = None
                    return
            for session in expired:
                logger.info("Closing idle preview session %s", session.session_id)
                await session.close()

    async def close_all(self):
        if self._reaper_task is not None:
            self._reaper_task.cancel()
            self._reaper_task = None
        async with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            await session.close()
