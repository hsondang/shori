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
async def test_preview_returns_rows(populated_client):
    resp = await populated_client.get("/api/data/preview/my_table")
    assert resp.status_code == 200
    data = resp.json()
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
