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


# --- Project storage lifecycle ---

@pytest.mark.asyncio
async def test_save_drops_tables_for_removed_nodes(client, pipeline_def):
    from app.main import app

    await client.post("/api/pipelines", json=pipeline_def)
    resp = await client.post(
        "/api/execute/node",
        json={"pipeline": pipeline_def, "node_id": "node-1"},
    )
    assert resp.json()["status"] == "success"
    manager = app.state.project_dbs.get(pipeline_def["id"])
    assert manager.table_exists("my_table")

    emptied = {**pipeline_def, "nodes": [], "edges": []}
    save_resp = await client.put(f"/api/pipelines/{pipeline_def['id']}", json=emptied)
    assert save_resp.status_code == 200

    assert manager.table_exists("my_table") is False
    assert manager.get_node_meta("node-1") is None


@pytest.mark.asyncio
async def test_save_renames_table_when_table_name_changes(client, pipeline_def):
    from app.main import app

    await client.post("/api/pipelines", json=pipeline_def)
    await client.post(
        "/api/execute/node",
        json={"pipeline": pipeline_def, "node_id": "node-1"},
    )
    manager = app.state.project_dbs.get(pipeline_def["id"])

    renamed = {
        **pipeline_def,
        "nodes": [{**pipeline_def["nodes"][0], "table_name": "renamed_table"}],
    }
    save_resp = await client.put(f"/api/pipelines/{pipeline_def['id']}", json=renamed)
    assert save_resp.status_code == 200

    assert manager.table_exists("my_table") is False
    assert manager.table_stats("renamed_table")["row_count"] == 5
    assert manager.get_node_meta("node-1")["table_name"] == "renamed_table"


@pytest.mark.asyncio
async def test_save_rejects_reserved_and_duplicate_table_names(client, pipeline_def):
    reserved = {
        **pipeline_def,
        "nodes": [{**pipeline_def["nodes"][0], "table_name": "_shori_hax"}],
    }
    resp = await client.post("/api/pipelines", json=reserved)
    assert resp.status_code == 400
    assert "reserved" in resp.json()["detail"]

    duplicated = {
        **pipeline_def,
        "nodes": [
            pipeline_def["nodes"][0],
            {**pipeline_def["nodes"][0], "id": "node-2"},
        ],
    }
    resp = await client.post("/api/pipelines", json=duplicated)
    assert resp.status_code == 400
    assert "unique" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_delete_project_removes_duckdb_directory(client, pipeline_def):
    import app.config as config_module
    from app.main import app

    await client.post("/api/pipelines", json=pipeline_def)
    await client.post(
        "/api/execute/node",
        json={"pipeline": pipeline_def, "node_id": "node-1"},
    )
    project_dir = config_module.PROJECTS_DIR / pipeline_def["id"]
    assert project_dir.exists()

    resp = await client.delete(f"/api/pipelines/{pipeline_def['id']}")
    assert resp.status_code == 200
    assert project_dir.exists() is False


@pytest.mark.asyncio
async def test_storage_and_compact_endpoints(client, pipeline_def):
    await client.post("/api/pipelines", json=pipeline_def)
    await client.post(
        "/api/execute/node",
        json={"pipeline": pipeline_def, "node_id": "node-1"},
    )

    storage_resp = await client.get(f"/api/pipelines/{pipeline_def['id']}/storage")
    assert storage_resp.status_code == 200
    assert storage_resp.json()["file_size_bytes"] > 0

    compact_resp = await client.post(f"/api/pipelines/{pipeline_def['id']}/compact")
    assert compact_resp.status_code == 200
    assert compact_resp.json()["file_size_bytes"] > 0


@pytest.mark.asyncio
async def test_settings_round_trip_with_defaults(client, pipeline_def):
    await client.post("/api/pipelines", json=pipeline_def)
    loaded = (await client.get(f"/api/pipelines/{pipeline_def['id']}")).json()
    assert loaded["settings"]["max_concurrent_nodes"] == 4
    assert loaded["settings"]["duckdb_memory_limit"] == "2GB"

    tuned = {**loaded, "settings": {**loaded["settings"], "max_concurrent_nodes": 8}}
    await client.put(f"/api/pipelines/{pipeline_def['id']}", json=tuned)
    reloaded = (await client.get(f"/api/pipelines/{pipeline_def['id']}")).json()
    assert reloaded["settings"]["max_concurrent_nodes"] == 8
