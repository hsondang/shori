import pytest


@pytest.mark.asyncio
async def test_list_empty(client):
    resp = await client.get("/api/pipelines")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_create_and_list(client, pipeline_def):
    await client.post("/api/pipelines", json=pipeline_def)
    resp = await client.get("/api/pipelines")
    assert resp.status_code == 200
    items = resp.json()
    assert len(items) == 1
    assert items[0]["id"] == pipeline_def["id"]
    assert items[0]["name"] == pipeline_def["name"]


@pytest.mark.asyncio
async def test_create_and_get(client, pipeline_def):
    await client.post("/api/pipelines", json=pipeline_def)
    resp = await client.get(f"/api/pipelines/{pipeline_def['id']}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == pipeline_def["id"]
    assert data["name"] == pipeline_def["name"]
    assert data["database_connections"] == []
    assert len(data["nodes"]) == 1


@pytest.mark.asyncio
async def test_get_not_found(client):
    resp = await client.get("/api/pipelines/nonexistent-id")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update(client, pipeline_def):
    await client.post("/api/pipelines", json=pipeline_def)
    updated = {**pipeline_def, "name": "Updated Name"}
    resp = await client.put(f"/api/pipelines/{pipeline_def['id']}", json=updated)
    assert resp.status_code == 200
    assert resp.json()["id"] == pipeline_def["id"]

    get_resp = await client.get(f"/api/pipelines/{pipeline_def['id']}")
    assert get_resp.json()["name"] == "Updated Name"


@pytest.mark.asyncio
async def test_update_id_overwritten_by_path(client, pipeline_def):
    """PUT should force the pipeline id to match the path param."""
    await client.post("/api/pipelines", json=pipeline_def)
    modified = {**pipeline_def, "id": "wrong-id", "name": "Renamed"}
    resp = await client.put(f"/api/pipelines/{pipeline_def['id']}", json=modified)
    assert resp.status_code == 200
    assert resp.json()["id"] == pipeline_def["id"]


@pytest.mark.asyncio
async def test_delete(client, pipeline_def):
    await client.post("/api/pipelines", json=pipeline_def)
    del_resp = await client.delete(f"/api/pipelines/{pipeline_def['id']}")
    assert del_resp.status_code == 200
    assert del_resp.json() == {"ok": True}

    get_resp = await client.get(f"/api/pipelines/{pipeline_def['id']}")
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_nonexistent(client):
    resp = await client.delete("/api/pipelines/does-not-exist")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
