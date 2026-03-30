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
    assert items[0]["starred"] is False
    assert items[0]["created_at"]
    assert items[0]["updated_at"]


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
async def test_create_and_get_preserves_optional_label_metadata(client, pipeline_def):
    pipeline_with_label_meta = {
        **pipeline_def,
        "nodes": [
            {
                **pipeline_def["nodes"][0],
                "auto_label": "CSV Source",
                "label_mode": "custom",
            }
        ],
    }

    await client.post("/api/pipelines", json=pipeline_with_label_meta)
    resp = await client.get(f"/api/pipelines/{pipeline_def['id']}")

    assert resp.status_code == 200
    node = resp.json()["nodes"][0]
    assert node["auto_label"] == "CSV Source"
    assert node["label_mode"] == "custom"


@pytest.mark.asyncio
async def test_get_not_found(client):
    resp = await client.get("/api/pipelines/nonexistent-id")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update(client, pipeline_def):
    await client.post("/api/pipelines", json=pipeline_def)
    first_list = (await client.get("/api/pipelines")).json()
    updated = {**pipeline_def, "name": "Updated Name"}
    resp = await client.put(f"/api/pipelines/{pipeline_def['id']}", json=updated)
    assert resp.status_code == 200
    assert resp.json()["id"] == pipeline_def["id"]

    get_resp = await client.get(f"/api/pipelines/{pipeline_def['id']}")
    assert get_resp.json()["name"] == "Updated Name"
    second_list = (await client.get("/api/pipelines")).json()
    assert second_list[0]["created_at"] == first_list[0]["created_at"]
    assert second_list[0]["updated_at"] >= first_list[0]["updated_at"]


@pytest.mark.asyncio
async def test_post_upserts_existing_pipeline(client, pipeline_def):
    await client.post("/api/pipelines", json=pipeline_def)
    resp = await client.post("/api/pipelines", json={**pipeline_def, "name": "Renamed Via Post"})
    assert resp.status_code == 200

    get_resp = await client.get(f"/api/pipelines/{pipeline_def['id']}")
    assert get_resp.json()["name"] == "Renamed Via Post"


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


@pytest.mark.asyncio
async def test_update_project_star(client, pipeline_def):
    await client.post("/api/pipelines", json=pipeline_def)

    resp = await client.patch(
        f"/api/pipelines/{pipeline_def['id']}/star",
        json={"starred": True},
    )
    assert resp.status_code == 200
    assert resp.json() == {"id": pipeline_def["id"], "starred": True}

    items = (await client.get("/api/pipelines")).json()
    assert items[0]["starred"] is True


@pytest.mark.asyncio
async def test_update_project_star_not_found(client):
    resp = await client.patch("/api/pipelines/missing/star", json={"starred": True})
    assert resp.status_code == 404
