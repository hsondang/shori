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


ORACLE_CONNECTION = {
    "name": "Oracle Prod",
    "db_type": "oracle",
    "host": "ora.internal",
    "port": 1521,
    "service_name": "KM",
    "user": "app",
    "password": "secret",
}


@pytest.mark.asyncio
async def test_oracle_connection_is_not_export_approved_by_default(client):
    resp = await client.post("/api/settings/database-connections", json=ORACLE_CONNECTION)
    assert resp.status_code == 200
    assert resp.json()["allow_export"] is False


@pytest.mark.asyncio
async def test_allow_export_round_trips_through_create_and_update(client):
    created = (
        await client.post(
            "/api/settings/database-connections",
            json={**ORACLE_CONNECTION, "allow_export": True},
        )
    ).json()
    assert created["allow_export"] is True

    listed = (await client.get("/api/settings/database-connections")).json()
    assert listed[0]["allow_export"] is True

    # Revoking is always allowed, even while export nodes reference it.
    revoked = (
        await client.put(
            f"/api/settings/database-connections/{created['id']}",
            json={**ORACLE_CONNECTION, "allow_export": False},
        )
    ).json()
    assert revoked["allow_export"] is False


@pytest.mark.asyncio
async def test_postgres_connections_have_no_export_permission(client):
    resp = await client.post(
        "/api/settings/database-connections",
        json={
            "name": "PG",
            "db_type": "postgres",
            "host": "db.internal",
            "port": 5432,
            "database": "analytics",
            "user": "readonly",
            "password": "secret",
            "allow_export": True,
        },
    )
    assert resp.status_code == 200
    assert "allow_export" not in resp.json()


@pytest.mark.asyncio
async def test_delete_conflicts_when_an_export_node_uses_the_connection(client, pipeline_def):
    """Export nodes reference a connection without connection_mode, so the
    in-use guard has to recognise their shape too."""
    connection = (
        await client.post(
            "/api/settings/database-connections",
            json={**ORACLE_CONNECTION, "allow_export": True},
        )
    ).json()

    pipeline = {
        **pipeline_def,
        "nodes": [
            *pipeline_def["nodes"],
            {
                "id": "export-1",
                "type": "export",
                "table_name": "export_1",
                "label": "Export",
                "position": {"x": 200, "y": 0},
                "config": {
                    "destination": "database",
                    "connection_source_id": connection["id"],
                    "target_table": "SALES.ORDERS",
                },
            },
        ],
    }
    await client.post("/api/pipelines", json=pipeline)

    delete_resp = await client.delete(f"/api/settings/database-connections/{connection['id']}")
    assert delete_resp.status_code == 409
    assert "Test Pipeline" in delete_resp.json()["detail"]
