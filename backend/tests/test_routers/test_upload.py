import io
from pathlib import Path
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
async def test_upload_excel_returns_sheet_names_and_previews(client, sample_excel_file):
    resp = await client.post(
        "/api/upload/excel",
        files={"file": ("sample.xlsx", io.BytesIO(Path(sample_excel_file).read_bytes()), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["filename"] == "sample.xlsx"
    assert body["sheet_names"] == ["Orders", "Summary"]
    assert body["sheets"][0]["name"] == "Orders"
    assert body["sheets"][0]["rows"][:2] == [
        ["id", "name", "value"],
        ["1", "Alice", "10.5"],
    ]


@pytest.mark.asyncio
async def test_upload_non_excel_rejected(client):
    resp = await client.post(
        "/api/upload/excel",
        files={"file": ("data.csv", io.BytesIO(b"a,b\n1,2\n"), "text/csv")},
    )

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_materialize_excel_sheet_writes_csv(client, sample_excel_file):
    resp = await client.post(
        "/api/upload/excel/materialize-sheet",
        json={"file_path": sample_excel_file, "sheet_name": "Summary"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["sheet_name"] == "Summary"
    assert body["filename"].endswith(".csv")
    assert Path(body["file_path"]).read_text(encoding="utf-8").splitlines() == [
        "metric,value",
        "total,2",
    ]


@pytest.mark.asyncio
async def test_materialize_excel_sheet_rejects_missing_sheet(client, sample_excel_file):
    resp = await client.post(
        "/api/upload/excel/materialize-sheet",
        json={"file_path": sample_excel_file, "sheet_name": "Missing"},
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
