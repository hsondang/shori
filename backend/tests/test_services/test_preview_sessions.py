import asyncio

import pytest

from app.models.pipeline import NodeDefinition, NodeType, Position
from app.services.preview_sessions import (
    PreviewSessionManager,
    PreviewSessionNotFound,
)


class FakeOracleCursor:
    """Yields `total_rows` two-column rows in fetchmany batches."""

    def __init__(self, connection, total_rows):
        self.connection = connection
        self.total_rows = total_rows
        self.position = 0
        self.arraysize = 100
        self.closed = False
        self.description = [("ID", type("T", (), {"name": "NUMBER"})), ("NAME", type("T", (), {"name": "VARCHAR2"}))]

    def execute(self, query):
        self.connection.executed_queries.append(query)

    def fetchmany(self, size):
        rows = [
            (i, f"row-{i}")
            for i in range(self.position, min(self.position + size, self.total_rows))
        ]
        self.position += len(rows)
        return rows

    def close(self):
        self.closed = True


class FakeOracleConnection:
    def __init__(self, total_rows):
        self.total_rows = total_rows
        self.executed_queries = []
        self.cancelled = False
        self.cursors = []

    def cursor(self):
        cursor = FakeOracleCursor(self, self.total_rows)
        self.cursors.append(cursor)
        return cursor

    def cancel(self):
        self.cancelled = True


class FakePoolRegistry:
    def __init__(self, total_rows=500):
        self.total_rows = total_rows
        self.acquired = 0
        self.released = 0
        self.connections = []

    async def acquire(self, db_type, conn_config, max_size=None):
        assert db_type == "oracle"
        self.acquired += 1
        connection = FakeOracleConnection(self.total_rows)
        self.connections.append(connection)

        async def release():
            self.released += 1

        return connection, release


def _db_node(node_id="db1", table_name="orders"):
    return NodeDefinition(
        id=node_id,
        type=NodeType.DB_SOURCE,
        table_name=table_name,
        label="Orders",
        position=Position(x=0, y=0),
        config={
            "db_type": "oracle",
            "connection": {"host": "h", "port": 1521, "service_name": "s", "user": "u", "password": "p"},
            "query": "SELECT * FROM orders",
        },
    )


@pytest.mark.asyncio
async def test_start_returns_first_chunk_without_creating_table(duckdb_mgr):
    pools = FakePoolRegistry(total_rows=500)
    manager = PreviewSessionManager(pools)

    result = await manager.start(project_id="p1", node=_db_node(), cache_key="k1", chunk_rows=200)

    assert result["columns"] == ["ID", "NAME"]
    assert len(result["rows"]) == 200
    assert result["has_more"] is True
    assert result["buffered_rows"] == 200
    assert duckdb_mgr.table_exists("orders") is False
    await manager.close_all()


@pytest.mark.asyncio
async def test_fetch_more_pages_until_exhausted():
    pools = FakePoolRegistry(total_rows=450)
    manager = PreviewSessionManager(pools)
    started = await manager.start(project_id="p1", node=_db_node(), cache_key=None, chunk_rows=200)
    sid = started["session_id"]

    second = await manager.fetch_more(sid)
    assert len(second["rows"]) == 200
    assert second["has_more"] is True

    third = await manager.fetch_more(sid)
    assert len(third["rows"]) == 50
    assert third["has_more"] is False
    assert third["buffered_rows"] == 450
    await manager.close_all()


@pytest.mark.asyncio
async def test_buffer_cap_stops_paging_but_keeps_session():
    pools = FakePoolRegistry(total_rows=1000)
    manager = PreviewSessionManager(pools)
    started = await manager.start(
        project_id="p1", node=_db_node(), cache_key=None, chunk_rows=200, max_buffer_rows=400
    )
    sid = started["session_id"]

    second = await manager.fetch_more(sid)
    assert second["buffer_capped"] is True

    capped = await manager.fetch_more(sid)
    assert capped["rows"] == []
    assert capped["has_more"] is True
    assert capped["buffer_capped"] is True
    assert capped["buffered_rows"] == 400
    await manager.close_all()


@pytest.mark.asyncio
async def test_materialize_streams_buffer_then_drains_cursor(duckdb_mgr):
    pools = FakePoolRegistry(total_rows=450)
    manager = PreviewSessionManager(pools)
    started = await manager.start(project_id="p1", node=_db_node(), cache_key="key-9", chunk_rows=200)
    sid = started["session_id"]
    connection = pools.connections[0]

    stats = await manager.materialize(sid, duckdb_mgr)

    assert stats["row_count"] == 450
    assert duckdb_mgr.table_stats("orders")["row_count"] == 450
    meta = duckdb_mgr.get_node_meta("db1")
    assert meta["status"] == "complete"
    assert meta["cache_key"] == "key-9"
    # The query ran exactly once: materialize reused the open cursor.
    assert len(connection.executed_queries) == 1
    assert pools.released == 1
    with pytest.raises(PreviewSessionNotFound):
        await manager.fetch_more(sid)
    await manager.close_all()


@pytest.mark.asyncio
async def test_close_releases_connection():
    pools = FakePoolRegistry(total_rows=10)
    manager = PreviewSessionManager(pools)
    started = await manager.start(project_id="p1", node=_db_node(), cache_key=None)

    closed = await manager.close(started["session_id"])
    assert closed is True
    assert pools.released == 1
    assert await manager.close("nope") is False
    await manager.close_all()


@pytest.mark.asyncio
async def test_expired_sessions_are_reaped():
    pools = FakePoolRegistry(total_rows=10)
    manager = PreviewSessionManager(pools)
    started = await manager.start(project_id="p1", node=_db_node(), cache_key=None, ttl_seconds=0)
    sid = started["session_id"]

    session = await manager.get_session(sid)
    assert session.expired(__import__("time").monotonic() + 1) is True

    # Force one reaper pass without waiting 30s.
    import app.services.preview_sessions as ps_mod
    original_interval = ps_mod.REAPER_INTERVAL_SECONDS
    ps_mod.REAPER_INTERVAL_SECONDS = 0.01
    try:
        await asyncio.sleep(0.1)
    finally:
        ps_mod.REAPER_INTERVAL_SECONDS = original_interval

    with pytest.raises(PreviewSessionNotFound):
        await manager.fetch_more(sid)
    assert pools.released == 1
    await manager.close_all()
