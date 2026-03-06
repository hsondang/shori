from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.postgres_service import PostgresService


CONNECTION_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "testdb",
    "user": "user",
    "password": "pass",
}

QUERY_CONFIG = {
    "connection": CONNECTION_CONFIG,
    "query": "SELECT id, name FROM users",
}


def _make_mock_row(keys, values):
    row = MagicMock()
    row.keys.return_value = keys
    row.values.return_value = values
    return row


@pytest.mark.asyncio
async def test_execute_query_returns_dataframe():
    mock_rows = [
        _make_mock_row(["id", "name"], [1, "Alice"]),
        _make_mock_row(["id", "name"], [2, "Bob"]),
    ]
    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=mock_rows)

    with patch("asyncpg.connect", new=AsyncMock(return_value=mock_conn)):
        svc = PostgresService()
        df = await svc.execute_query(QUERY_CONFIG)

    assert list(df.columns) == ["id", "name"]
    assert len(df) == 2
    assert df.iloc[0]["name"] == "Alice"


@pytest.mark.asyncio
async def test_execute_query_empty_result():
    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=[])

    with patch("asyncpg.connect", new=AsyncMock(return_value=mock_conn)):
        svc = PostgresService()
        df = await svc.execute_query(QUERY_CONFIG)

    assert df.empty


@pytest.mark.asyncio
async def test_execute_query_closes_connection_on_success():
    mock_rows = [_make_mock_row(["id"], [1])]
    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=mock_rows)

    with patch("asyncpg.connect", new=AsyncMock(return_value=mock_conn)):
        svc = PostgresService()
        await svc.execute_query(QUERY_CONFIG)

    mock_conn.close.assert_called_once()


@pytest.mark.asyncio
async def test_execute_query_closes_connection_on_error():
    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(side_effect=RuntimeError("query failed"))

    with patch("asyncpg.connect", new=AsyncMock(return_value=mock_conn)):
        svc = PostgresService()
        with pytest.raises(RuntimeError):
            await svc.execute_query(QUERY_CONFIG)

    mock_conn.close.assert_called_once()


@pytest.mark.asyncio
async def test_test_connection_success():
    mock_conn = AsyncMock()
    with patch("asyncpg.connect", new=AsyncMock(return_value=mock_conn)):
        svc = PostgresService()
        result = await svc.test_connection(CONNECTION_CONFIG)
    assert result is True
    mock_conn.close.assert_called_once()


@pytest.mark.asyncio
async def test_test_connection_failure():
    with patch("asyncpg.connect", new=AsyncMock(side_effect=OSError("refused"))):
        svc = PostgresService()
        with pytest.raises(OSError):
            await svc.test_connection(CONNECTION_CONFIG)
