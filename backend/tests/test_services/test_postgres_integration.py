import pytest

from app.services.postgres_service import PostgresService

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "shori_test",
    "user": "shori_test",
    "password": "shori_test",
}

QUERY_CONFIG = {
    "connection": DB_CONFIG,
    "query": "SELECT * FROM customers ORDER BY id",
}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execute_query_real_db():
    svc = PostgresService()
    df = await svc.execute_query(QUERY_CONFIG)
    assert len(df) == 10
    assert "name" in df.columns
    assert "country" in df.columns


@pytest.mark.integration
@pytest.mark.asyncio
async def test_test_connection_real_db():
    svc = PostgresService()
    result = await svc.test_connection(DB_CONFIG)
    assert result is True


@pytest.mark.integration
@pytest.mark.asyncio
async def test_invalid_query_raises():
    svc = PostgresService()
    bad_config = {**QUERY_CONFIG, "query": "SELECT * FROM table_that_does_not_exist"}
    with pytest.raises(Exception):
        await svc.execute_query(bad_config)
