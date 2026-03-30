import pytest


@pytest.fixture
async def populated_client(client, pipeline_def):
    """Client with a CSV table already loaded into DuckDB."""
    await client.post("/api/execute/node", json=pipeline_def["nodes"][0])
    return client


@pytest.mark.asyncio
async def test_preview_not_found(client):
    resp = await client.get("/api/data/preview/nonexistent_table")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_csv_source_preview_not_found(client):
    resp = await client.post("/api/data/preview/csv-source", json={"file_path": "/tmp/missing.csv"})
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_csv_source_preview_returns_first_rows(client, office365_csv_file):
    resp = await client.post("/api/data/preview/csv-source", json={"file_path": office365_csv_file, "limit": 3})
    assert resp.status_code == 200
    data = resp.json()
    assert data["kind"] == "csv_text"
    assert data["csv_stage"] == "raw"
    assert data["rows"] == [
        ["Created by: user x"],
        ["Created Time: 2026-03-13 17:03:20"],
        ["id", "name", "value"],
    ]
    assert data["truncated"] is True


@pytest.mark.asyncio
async def test_csv_source_preview_handles_excel_style_commas_and_notes(client, excel_style_csv_file):
    resp = await client.post("/api/data/preview/csv-source", json={"file_path": excel_style_csv_file, "limit": 20})

    assert resp.status_code == 200
    data = resp.json()
    assert data["rows"] == [
        ["", "MONTHLY DATA ALLOCATION", "", ""],
        ["Notes", "Synthetic spreadsheet-style export for CSV preview regression testing", "", ""],
        ["", "", "", ""],
        ["", "", "", ""],
        ["Employee ID", "Agent Name", "User", "Quota"],
        ["EMP001", "Agent One", "user.one", " 1,120   "],
        ["EMP002", "Agent Two", "user.two", " 1,120   "],
        ["EMP003", "Agent Three", "user.three", " 770   "],
        ["EMP004", "Agent Four", "user.four", " 770   "],
        ["", "", "", " 3,780   "],
    ]
    assert data["truncated"] is False


@pytest.mark.asyncio
async def test_preprocessed_csv_source_preview_returns_reviewed_rows(client, office365_csv_file):
    resp = await client.post(
        "/api/data/preview/csv-source/preprocessed",
        json={
            "node_id": "node-1",
            "file_path": office365_csv_file,
            "preprocessing": {
                "enabled": True,
                "runtime": "python",
                "script": "import sys; from pathlib import Path; lines = Path(sys.argv[1]).read_text().splitlines()[2:]; sys.stdout.write('\\n'.join(lines))",
            },
            "limit": 3,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["kind"] == "csv_text"
    assert data["csv_stage"] == "preprocessed"
    assert data["artifact_ready"] is True
    assert data["rows"] == [
        ["id", "name", "value"],
        ["1", "Alice", "10.5"],
        ["2", "Bob", "20.0"],
    ]


@pytest.mark.asyncio
async def test_delete_preprocessed_csv_artifact(client, office365_csv_file):
    await client.post(
        "/api/data/preview/csv-source/preprocessed",
        json={
            "node_id": "node-1",
            "file_path": office365_csv_file,
            "preprocessing": {
                "enabled": True,
                "runtime": "bash",
                "script": "tail -n +3 \"$1\"",
            },
        },
    )

    resp = await client.delete("/api/data/preview/csv-source/preprocessed/node-1")
    assert resp.status_code == 200
    assert resp.json() == {"deleted": True}


@pytest.mark.asyncio
async def test_preview_returns_rows(populated_client):
    resp = await populated_client.get("/api/data/preview/my_table")
    assert resp.status_code == 200
    data = resp.json()
    assert data["kind"] == "table"
    assert data["total_rows"] == 5
    assert "id" in data["columns"]
    assert len(data["rows"]) == 5


@pytest.mark.asyncio
async def test_preview_pagination(populated_client):
    resp = await populated_client.get("/api/data/preview/my_table?offset=2&limit=2")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["rows"]) == 2
    assert data["offset"] == 2
    assert data["limit"] == 2
    assert data["total_rows"] == 5


@pytest.mark.asyncio
async def test_preview_offset_beyond_total(populated_client):
    resp = await populated_client.get("/api/data/preview/my_table?offset=100")
    assert resp.status_code == 200
    data = resp.json()
    assert data["rows"] == []
    assert data["total_rows"] == 5


@pytest.mark.asyncio
async def test_schema_not_found(client):
    resp = await client.get("/api/data/schema/nonexistent_table")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_schema_structure(populated_client):
    resp = await populated_client.get("/api/data/schema/my_table")
    assert resp.status_code == 200
    data = resp.json()
    assert data["table_name"] == "my_table"
    assert "id" in data["columns"]
    assert len(data["column_types"]) == len(data["columns"])
    assert data["total_rows"] == 5


@pytest.mark.asyncio
async def test_export_not_found(client):
    resp = await client.get("/api/data/export/nonexistent_table")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_export_returns_csv(populated_client):
    resp = await populated_client.get("/api/data/export/my_table")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers["content-type"]
    content = resp.text
    assert "id" in content
    assert "Alice" in content


@pytest.mark.asyncio
async def test_delete_table_removes_materialized_table(populated_client):
    delete_resp = await populated_client.delete("/api/data/table/my_table")
    assert delete_resp.status_code == 200
    assert delete_resp.json() == {"deleted": True}

    preview_resp = await populated_client.get("/api/data/preview/my_table")
    assert preview_resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_table_is_idempotent(client):
    resp = await client.delete("/api/data/table/nonexistent_table")
    assert resp.status_code == 200
    assert resp.json() == {"deleted": False}
