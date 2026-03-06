import io
from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_upload_csv_success(client, tmp_path):
    csv_content = b"a,b,c\n1,2,3\n4,5,6\n"
    resp = await client.post(
        "/api/upload/csv",
        files={"file": ("data.csv", io.BytesIO(csv_content), "text/csv")},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["filename"] == "data.csv"
    assert body["file_path"].endswith("data.csv")


@pytest.mark.asyncio
async def test_upload_non_csv_rejected(client):
    resp = await client.post(
        "/api/upload/csv",
        files={"file": ("data.txt", io.BytesIO(b"hello"), "text/plain")},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_upload_no_extension_rejected(client):
    resp = await client.post(
        "/api/upload/csv",
        files={"file": ("datafile", io.BytesIO(b"hello"), "application/octet-stream")},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_postgres_test_connection_success(client):
    mock_conn = AsyncMock()
    with patch("asyncpg.connect", new=AsyncMock(return_value=mock_conn)):
        resp = await client.post(
            "/api/postgres/test-connection",
            json={
                "host": "localhost",
                "port": 5432,
                "database": "testdb",
                "user": "user",
                "password": "pass",
            },
        )
    assert resp.status_code == 200
    assert resp.json()["success"] is True


@pytest.mark.asyncio
async def test_postgres_test_connection_failure(client):
    with patch("asyncpg.connect", new=AsyncMock(side_effect=OSError("connection refused"))):
        resp = await client.post(
            "/api/postgres/test-connection",
            json={
                "host": "bad-host",
                "port": 5432,
                "database": "testdb",
                "user": "user",
                "password": "pass",
            },
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is False
    assert "error" in body
