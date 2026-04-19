import pytest


@pytest.mark.asyncio
async def test_list_global_connections_empty(client):
    resp = await client.get("/api/settings/database-connections")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_create_update_and_delete_global_connection(client):
    create_resp = await client.post(
        "/api/settings/database-connections",
        json={
            "name": "Warehouse",
            "db_type": "postgres",
            "host": "db.internal",
            "port": 5432,
            "database": "warehouse",
            "user": "readonly",
            "password": "secret",
        },
    )
    assert create_resp.status_code == 200
    created = create_resp.json()
    assert created["name"] == "Warehouse"

    list_resp = await client.get("/api/settings/database-connections")
    assert list_resp.status_code == 200
    assert list_resp.json() == [created]

    update_resp = await client.put(
        f"/api/settings/database-connections/{created['id']}",
        json={
            "name": "Warehouse Prod",
            "db_type": "oracle",
            "host": "ora.internal",
            "port": 1521,
            "service_name": "DW",
            "user": "readonly",
            "password": "secret",
        },
    )
    assert update_resp.status_code == 200
    updated = update_resp.json()
    assert updated["name"] == "Warehouse Prod"
    assert updated["db_type"] == "oracle"
    assert updated["service_name"] == "DW"

    delete_resp = await client.delete(f"/api/settings/database-connections/{created['id']}")
    assert delete_resp.status_code == 200
    assert delete_resp.json() == {"ok": True}


@pytest.mark.asyncio
async def test_delete_global_connection_in_use_returns_conflict(client, pipeline_def):
    create_resp = await client.post(
        "/api/settings/database-connections",
        json={
            "name": "Analytics Global",
            "db_type": "postgres",
            "host": "db.internal",
            "port": 5432,
            "database": "analytics",
            "user": "readonly",
            "password": "secret",
        },
    )
    connection = create_resp.json()

    pipeline = {
        **pipeline_def,
        "nodes": [
            {
                "id": "node-1",
                "type": "db_source",
                "table_name": "orders",
                "label": "Orders",
                "position": {"x": 0, "y": 0},
                "config": {
                    "connection_mode": "global",
                    "connection_source_id": connection["id"],
                    "db_type": "postgres",
                    "query": "SELECT 1",
                },
            }
        ],
    }
    await client.post("/api/pipelines", json=pipeline)

    delete_resp = await client.delete(f"/api/settings/database-connections/{connection['id']}")
    assert delete_resp.status_code == 409
    assert "Test Pipeline" in delete_resp.json()["detail"]
