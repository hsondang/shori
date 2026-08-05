import pytest

from app.main import app

PROJECT_ID = "test-pipeline-1"


@pytest.fixture
async def populated_client(client, pipeline_def):
    """Client with a CSV table already loaded into the project's DuckDB."""
    resp = await client.post(
        "/api/execute/node",
        json={"pipeline": pipeline_def, "node_id": pipeline_def["nodes"][0]["id"]},
    )
    assert resp.status_code == 200
    return client


@pytest.mark.asyncio
async def test_preview_not_found(client):
    resp = await client.get(f"/api/data/{PROJECT_ID}/preview/nonexistent_table")
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


_SKIP_TWO_LINES = (
    "import pandas as pd\n"
    "def preprocess(file):\n"
    "    return pd.read_csv(file, skiprows=2)\n"
)


@pytest.mark.asyncio
async def test_preprocessed_csv_source_preview_returns_reviewed_rows(client, office365_csv_file, tmp_path):
    script_path = tmp_path / "pp.py"
    script_path.write_text(_SKIP_TWO_LINES, encoding="utf-8")
    resp = await client.post(
        "/api/data/preview/csv-source/preprocessed",
        json={
            "node_id": "node-1",
            "file_path": office365_csv_file,
            "preprocessing": {"enabled": True, "script_path": str(script_path)},
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
async def test_delete_preprocessed_csv_artifact(client, office365_csv_file, tmp_path):
    script_path = tmp_path / "pp.py"
    script_path.write_text(_SKIP_TWO_LINES, encoding="utf-8")
    await client.post(
        "/api/data/preview/csv-source/preprocessed",
        json={
            "node_id": "node-1",
            "file_path": office365_csv_file,
            "preprocessing": {"enabled": True, "script_path": str(script_path)},
        },
    )

    resp = await client.delete("/api/data/preview/csv-source/preprocessed/node-1")
    assert resp.status_code == 200
    assert resp.json() == {"deleted": True}


@pytest.mark.asyncio
async def test_preview_returns_rows(populated_client):
    resp = await populated_client.get(f"/api/data/{PROJECT_ID}/preview/my_table")
    assert resp.status_code == 200
    data = resp.json()
    assert data["kind"] == "table"
    assert data["total_rows"] == 5
    assert "id" in data["columns"]
    assert len(data["rows"]) == 5


@pytest.mark.asyncio
async def test_preview_pagination(populated_client):
    resp = await populated_client.get(f"/api/data/{PROJECT_ID}/preview/my_table?offset=2&limit=2")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["rows"]) == 2
    assert data["offset"] == 2
    assert data["limit"] == 2
    assert data["total_rows"] == 5


@pytest.mark.asyncio
async def test_preview_offset_beyond_total(populated_client):
    resp = await populated_client.get(f"/api/data/{PROJECT_ID}/preview/my_table?offset=100")
    assert resp.status_code == 200
    data = resp.json()
    assert data["rows"] == []
    assert data["total_rows"] == 5


@pytest.mark.asyncio
async def test_preview_returns_json_safe_non_finite_float_values(client):
    app.state.project_dbs.get(PROJECT_ID).execute_transform(
        "non_finite_values",
        "SELECT 'NaN'::DOUBLE AS score, 'Infinity'::DOUBLE AS high",
    )

    resp = await client.get(f"/api/data/{PROJECT_ID}/preview/non_finite_values")

    assert resp.status_code == 200
    assert resp.json()["rows"] == [["NaN", "Infinity"]]


@pytest.mark.asyncio
async def test_preview_internal_metadata_table_is_hidden(client):
    app.state.project_dbs.get(PROJECT_ID)  # ensure the project db (and meta table) exists
    resp = await client.get(f"/api/data/{PROJECT_ID}/preview/_shori_node_meta")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_schema_not_found(client):
    resp = await client.get(f"/api/data/{PROJECT_ID}/schema/nonexistent_table")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_schema_structure(populated_client):
    resp = await populated_client.get(f"/api/data/{PROJECT_ID}/schema/my_table")
    assert resp.status_code == 200
    data = resp.json()
    assert data["table_name"] == "my_table"
    assert "id" in data["columns"]
    assert len(data["column_types"]) == len(data["columns"])
    assert data["total_rows"] == 5


@pytest.mark.asyncio
async def test_export_not_found(client):
    resp = await client.get(f"/api/data/{PROJECT_ID}/export/nonexistent_table")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_export_returns_csv(populated_client):
    resp = await populated_client.get(f"/api/data/{PROJECT_ID}/export/my_table")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers["content-type"]
    content = resp.text
    assert "id" in content
    assert "Alice" in content


@pytest.mark.asyncio
async def test_export_to_path_writes_local_file(populated_client, tmp_path):
    out = tmp_path / "export.parquet"
    resp = await populated_client.post(
        f"/api/data/{PROJECT_ID}/export-to-path",
        json={"table_name": "my_table", "output_path": str(out), "format": "parquet"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["format"] == "parquet"
    assert body["row_count"] == 5
    assert out.exists()


@pytest.mark.asyncio
async def test_export_to_path_rejects_missing_table(populated_client, tmp_path):
    resp = await populated_client.post(
        f"/api/data/{PROJECT_ID}/export-to-path",
        json={"table_name": "nope", "output_path": str(tmp_path / "x.csv"), "format": "csv"},
    )
    assert resp.status_code == 400


ORACLE_CONNECTION = {
    "name": "Oracle Prod",
    "db_type": "oracle",
    "host": "ora.internal",
    "port": 1521,
    "service_name": "KM",
    "user": "app",
    "password": "secret",
}


async def _create_connection(client, **overrides):
    resp = await client.post(
        "/api/settings/database-connections", json={**ORACLE_CONNECTION, **overrides}
    )
    assert resp.status_code == 200
    return resp.json()


def _pipeline_with_export(pipeline_def, export_config: dict) -> dict:
    """The CSV source fixture plus an export node wired to it."""
    return {
        **pipeline_def,
        "nodes": [
            *pipeline_def["nodes"],
            {
                "id": "export-1",
                "type": "export",
                "table_name": "export_1",
                "label": "Export",
                "position": {"x": 200, "y": 0},
                "config": export_config,
            },
        ],
        "edges": [{"id": "e1", "source": pipeline_def["nodes"][0]["id"], "target": "export-1"}],
    }


@pytest.mark.asyncio
async def test_export_to_database_refuses_a_connection_without_permission(
    populated_client, pipeline_def
):
    """Client-side filtering is UX; a hand-edited or stale node config must
    still be refused by the server."""
    connection = await _create_connection(populated_client, allow_export=False)
    pipeline = _pipeline_with_export(
        pipeline_def,
        {
            "destination": "database",
            "connection_source_id": connection["id"],
            "target_table": "SALES.ORDERS",
        },
    )

    resp = await populated_client.post(
        f"/api/data/{PROJECT_ID}/export-to-database",
        json={"pipeline": pipeline, "node_id": "export-1"},
    )
    assert resp.status_code == 400
    assert "not enabled" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_export_to_database_refuses_a_postgres_connection(populated_client, pipeline_def):
    resp = await populated_client.post(
        "/api/settings/database-connections",
        json={
            "name": "PG",
            "db_type": "postgres",
            "host": "db.internal",
            "port": 5432,
            "database": "analytics",
            "user": "readonly",
            "password": "secret",
        },
    )
    connection = resp.json()
    pipeline = _pipeline_with_export(
        pipeline_def,
        {
            "destination": "database",
            "connection_source_id": connection["id"],
            "target_table": "SALES.ORDERS",
        },
    )

    resp = await populated_client.post(
        f"/api/data/{PROJECT_ID}/export-to-database",
        json={"pipeline": pipeline, "node_id": "export-1"},
    )
    assert resp.status_code == 400
    assert "postgres" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_export_to_database_rejects_a_malformed_target_table(populated_client, pipeline_def):
    connection = await _create_connection(populated_client, allow_export=True)
    pipeline = _pipeline_with_export(
        pipeline_def,
        {
            "destination": "database",
            "connection_source_id": connection["id"],
            "target_table": "orders",
        },
    )

    resp = await populated_client.post(
        f"/api/data/{PROJECT_ID}/export-to-database",
        json={"pipeline": pipeline, "node_id": "export-1"},
    )
    assert resp.status_code == 400
    assert "SCHEMA.TABLE_NAME" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_export_to_database_rejects_a_missing_connection(populated_client, pipeline_def):
    pipeline = _pipeline_with_export(
        pipeline_def,
        {"destination": "database", "target_table": "SALES.ORDERS"},
    )

    resp = await populated_client.post(
        f"/api/data/{PROJECT_ID}/export-to-database",
        json={"pipeline": pipeline, "node_id": "export-1"},
    )
    assert resp.status_code == 400
    assert "no database connection" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_export_to_database_rejects_a_non_database_destination(
    populated_client, pipeline_def
):
    pipeline = _pipeline_with_export(pipeline_def, {"destination": "local", "output_path": "/tmp/x"})

    resp = await populated_client.post(
        f"/api/data/{PROJECT_ID}/export-to-database",
        json={"pipeline": pipeline, "node_id": "export-1"},
    )
    assert resp.status_code == 400
    assert "not configured to export to a database" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_export_to_database_reports_unloaded_upstreams(client, pipeline_def):
    """Nothing has been executed, so the upstream has no copy in either DuckDB
    location and the export cannot read it."""
    connection = await _create_connection(client, allow_export=True)
    pipeline = _pipeline_with_export(
        pipeline_def,
        {
            "destination": "database",
            "connection_source_id": connection["id"],
            "target_table": "SALES.ORDERS",
        },
    )

    resp = await client.post(
        f"/api/data/{PROJECT_ID}/export-to-database",
        json={"pipeline": pipeline, "node_id": "export-1"},
    )
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["error"] == "upstreams_unavailable"
    assert detail["missing_tables"] == ["my_table"]


@pytest.mark.asyncio
async def test_export_node_live_preview_stays_in_duckdb(populated_client, pipeline_def):
    """Preview must not need (or touch) the destination database — the fake
    Oracle host here would fail the moment a connection were attempted."""
    connection = await _create_connection(populated_client, allow_export=True)
    pipeline = _pipeline_with_export(
        pipeline_def,
        {
            "destination": "database",
            "connection_source_id": connection["id"],
            "target_table": "SALES.ORDERS",
        },
    )

    resp = await populated_client.post(
        "/api/data/preview-session/start",
        json={"pipeline": pipeline, "node_id": "export-1"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "id" in body["columns"]
    assert len(body["rows"]) == 5


@pytest.mark.asyncio
async def test_export_node_live_preview_uses_the_node_sql_when_enabled(
    populated_client, pipeline_def
):
    connection = await _create_connection(populated_client, allow_export=True)
    pipeline = _pipeline_with_export(
        pipeline_def,
        {
            "destination": "database",
            "connection_source_id": connection["id"],
            "target_table": "SALES.ORDERS",
            "use_sql": True,
            "sql": "SELECT name FROM my_table WHERE id <= 2",
        },
    )

    resp = await populated_client.post(
        "/api/data/preview-session/start",
        json={"pipeline": pipeline, "node_id": "export-1"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["columns"] == ["name"]
    assert len(body["rows"]) == 2


@pytest.mark.asyncio
async def test_local_export_node_has_no_live_preview(populated_client, pipeline_def):
    pipeline = _pipeline_with_export(pipeline_def, {"destination": "local", "output_path": "/tmp/x"})

    resp = await populated_client.post(
        "/api/data/preview-session/start",
        json={"pipeline": pipeline, "node_id": "export-1"},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_delete_table_removes_materialized_table(populated_client):
    delete_resp = await populated_client.delete(f"/api/data/{PROJECT_ID}/table/my_table")
    assert delete_resp.status_code == 200
    assert delete_resp.json() == {"deleted": True}

    preview_resp = await populated_client.get(f"/api/data/{PROJECT_ID}/preview/my_table")
    assert preview_resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_table_clears_node_metadata(populated_client, pipeline_def):
    node_id = pipeline_def["nodes"][0]["id"]
    manager = app.state.project_dbs.get(PROJECT_ID)
    assert manager.get_node_meta(node_id) is not None

    await populated_client.delete(f"/api/data/{PROJECT_ID}/table/my_table")
    assert manager.get_node_meta(node_id) is None


@pytest.mark.asyncio
async def test_delete_table_is_idempotent(client):
    resp = await client.delete(f"/api/data/{PROJECT_ID}/table/nonexistent_table")
    assert resp.status_code == 200
    assert resp.json() == {"deleted": False}
