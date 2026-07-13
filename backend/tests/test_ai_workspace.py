"""Tests for the AI workspace sub-app (Phases 0-1 — docs/ai-workspace-model.md §8)."""

import json
import sqlite3

import duckdb
import pytest
from httpx import ASGITransport, AsyncClient

from ai_workspace import build_ai_app
from ai_workspace.workspace import AIWorkspace


@pytest.fixture
def ai_data_dir(tmp_path):
    """A data dir with a projects store containing two projects."""
    db_path = tmp_path / "projects.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE projects (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            pipeline_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            starred INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.executemany(
        "INSERT INTO projects (id, name, pipeline_json, created_at, updated_at) "
        "VALUES (?, ?, '{}', ?, ?)",
        [
            ("proj-a", "Alpha", "2026-01-01", "2026-01-02"),
            ("proj-b", "Beta", "2026-01-01", "2026-01-03"),
        ],
    )
    conn.commit()
    conn.close()
    return tmp_path


@pytest.fixture
def ai_client(ai_data_dir):
    app = build_ai_app(data_dir=ai_data_dir)
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def test_health(ai_client):
    async with ai_client as client:
        resp = await client.get("/api/health")
    assert resp.status_code == 200
    assert resp.json()["component"] == "ai_workspace"


async def test_list_projects(ai_client):
    async with ai_client as client:
        resp = await client.get("/api/projects")
    assert resp.status_code == 200
    projects = resp.json()
    # Ordered by updated_at DESC
    assert [p["id"] for p in projects] == ["proj-b", "proj-a"]
    assert all(p["has_ai_workspace"] is False for p in projects)


async def test_list_projects_detects_existing_workspace(ai_data_dir, ai_client):
    workspace = ai_data_dir / "ai" / "proj-a" / "workspace.duckdb"
    workspace.parent.mkdir(parents=True)
    workspace.touch()
    async with ai_client as client:
        resp = await client.get("/api/projects")
    flags = {p["id"]: p["has_ai_workspace"] for p in resp.json()}
    assert flags == {"proj-a": True, "proj-b": False}


async def test_list_projects_without_store_is_empty(tmp_path):
    app = build_ai_app(data_dir=tmp_path)  # no projects.sqlite3 at all
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
        resp = await client.get("/api/projects")
    assert resp.status_code == 200
    assert resp.json() == []


async def test_state_for_known_project(ai_client):
    async with ai_client as client:
        resp = await client.get("/api/projects/proj-a/state")
    assert resp.status_code == 200
    body = resp.json()
    assert body["project_id"] == "proj-a"
    assert body["project_name"] == "Alpha"
    assert body["settings"] == {"autonomous_execute": False, "auto_share_results": False}
    assert body["editor"]["sql"] == ""
    assert body["pending_requests"] == []


async def test_state_unknown_project_404(ai_client):
    async with ai_client as client:
        resp = await client.get("/api/projects/nope/state")
    assert resp.status_code == 404


async def test_sub_app_never_writes_project_store(ai_data_dir, ai_client):
    """The sub-app opens the projects store read-only; the file must be untouched."""
    db_path = ai_data_dir / "projects.sqlite3"
    before = db_path.read_bytes()
    async with ai_client as client:
        await client.get("/api/projects")
        await client.get("/api/projects/proj-a/permissions")
    assert db_path.read_bytes() == before


async def test_main_app_mounts_ai_sub_app():
    """The guarded mount in app/main.py exposes the sub-app under /ai."""
    from app.main import app as main_app

    async with AsyncClient(transport=ASGITransport(app=main_app), base_url="http://t") as client:
        resp = await client.get("/ai/api/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "component": "ai_workspace"}


# --- Phase 1: spool ingestion, tables, schema --------------------------------


def _spool_clone(data_dir, project_id, table_name, sql="SELECT 1 AS id, 'a' AS label"):
    """Write a parquet + sidecar pair the way the main app's export does."""
    inbox = data_dir / "ai" / project_id / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    duckdb.connect().execute(
        f"COPY ({sql}) TO '{inbox / (table_name + '.parquet')}' (FORMAT PARQUET)"
    )
    (inbox / f"{table_name}.json").write_text(
        json.dumps(
            {
                "table_name": table_name,
                "source_node_id": "node-1",
                "source_table": table_name,
                "row_count": 1,
                "exported_at": "2026-07-12T00:00:00+00:00",
            }
        )
    )
    return inbox


async def test_sweep_ingests_spool_and_lists_tables(ai_data_dir, ai_client):
    inbox = _spool_clone(ai_data_dir, "proj-a", "orders")
    async with ai_client as client:
        resp = await client.get("/api/projects/proj-a/tables")
    assert resp.status_code == 200
    (table,) = resp.json()
    assert table["name"] == "orders"
    assert table["column_count"] == 2
    assert table["row_count"] == 1
    assert table["cloned_from"] == {"node_id": "node-1", "table": "orders"}
    assert table["cloned_at"]
    # Spool is drained after ingestion
    assert list(inbox.iterdir()) == []


async def test_schema_endpoint(ai_data_dir, ai_client):
    _spool_clone(ai_data_dir, "proj-a", "orders", sql="SELECT 1 AS id, 2.5 AS amount")
    async with ai_client as client:
        resp = await client.get("/api/projects/proj-a/tables/orders/schema")
        missing = await client.get("/api/projects/proj-a/tables/nope/schema")
    body = resp.json()
    assert body["table"] == "orders"
    assert body["columns"] == [
        {"name": "id", "type": "INTEGER"},
        {"name": "amount", "type": "DECIMAL(2,1)"},
    ]
    assert missing.status_code == 404
    assert "export" in missing.json()["detail"]


async def test_reexport_replaces_snapshot(ai_data_dir, ai_client):
    _spool_clone(ai_data_dir, "proj-a", "orders")
    async with ai_client as client:
        await client.get("/api/projects/proj-a/tables")
        _spool_clone(ai_data_dir, "proj-a", "orders", sql="SELECT 1 AS a, 2 AS b, 3 AS c")
        resp = await client.get("/api/projects/proj-a/tables")
    (table,) = resp.json()
    assert table["column_count"] == 3


async def test_pending_inbox_counts_as_workspace(ai_data_dir, ai_client):
    _spool_clone(ai_data_dir, "proj-b", "events")
    async with ai_client as client:
        resp = await client.get("/api/projects")
    flags = {p["id"]: p["has_ai_workspace"] for p in resp.json()}
    assert flags == {"proj-a": False, "proj-b": True}


def test_unsafe_table_name_is_not_ingested(tmp_path):
    workspace = AIWorkspace(tmp_path / "ai" / "p")
    inbox = tmp_path / "ai" / "p" / "inbox"
    inbox.mkdir(parents=True)
    (inbox / "evil.json").write_text(json.dumps({"table_name": 'x"; DROP TABLE y'}))
    assert workspace.sweep_inbox() == 0
    assert (inbox / "evil.json").exists()  # left for inspection, not deleted


def test_workspace_sandbox_is_locked(tmp_path):
    """The §2 invariants: no external access, and configuration cannot be unlocked."""
    _spool_clone(tmp_path, "p", "t")
    workspace = AIWorkspace(tmp_path / "ai" / "p")
    assert workspace.sweep_inbox() == 1
    conn = workspace._connection()

    assert conn.execute("SELECT current_setting('enable_external_access')").fetchone()[0] is False
    with pytest.raises(Exception, match="lock"):  # cannot re-enable
        conn.execute("SET enable_external_access = true")
    with pytest.raises(Exception):  # cannot read files
        conn.execute(f"SELECT * FROM read_parquet('{tmp_path}/anything.parquet')")
    with pytest.raises(Exception):  # cannot attach other databases
        conn.execute(f"ATTACH '{tmp_path}/other.duckdb' AS other")
    # ...but the ingested table is fully queryable
    assert conn.execute('SELECT count(*) FROM "t"').fetchone()[0] == 1
    workspace.close()


# --- Phase 2: shared editor, validation, activity -----------------------------

AGENT = {"X-Shori-Client": "mcp"}


async def test_editor_conflict_semantics(ai_client):
    async with ai_client as client:
        # Empty editor: agent writes land directly
        resp = await client.post(
            "/api/projects/proj-a/editor/agent-write",
            json={"sql": "SELECT 1", "note": "first"},
            headers=AGENT,
        )
        assert resp.json() == {"mode": "written"}

        # Agent-owned editor: agent may overwrite directly
        resp = await client.post(
            "/api/projects/proj-a/editor/agent-write", json={"sql": "SELECT 2"}, headers=AGENT
        )
        assert resp.json() == {"mode": "written"}

        # User edits -> next agent write is STAGED, never clobbering
        await client.put("/api/projects/proj-a/editor", json={"sql": "SELECT 2 -- tweaked"})
        resp = await client.post(
            "/api/projects/proj-a/editor/agent-write",
            json={"sql": "SELECT 3", "note": "v3"},
            headers=AGENT,
        )
        body = resp.json()
        assert body["mode"] == "staged"
        first_draft = body["draft_id"]

        # A second staged write supersedes the first
        resp = await client.post(
            "/api/projects/proj-a/editor/agent-write",
            json={"sql": "SELECT 4", "note": "v4"},
            headers=AGENT,
        )
        second_draft = resp.json()["draft_id"]

        editor = (await client.get("/api/projects/proj-a/editor")).json()
        assert editor["sql"] == "SELECT 2 -- tweaked"  # user content untouched
        assert editor["last_editor"] == "user"
        assert editor["staged_draft"]["id"] == second_draft

        # Superseded draft can no longer be loaded
        resp = await client.post(f"/api/projects/proj-a/editor/drafts/{first_draft}/load")
        assert resp.status_code == 404

        # Loading the staged draft makes it the editor content, agent-owned...
        resp = await client.post(f"/api/projects/proj-a/editor/drafts/{second_draft}/load")
        editor = resp.json()
        assert editor["sql"] == "SELECT 4"
        assert editor["last_editor"] == "agent"
        assert editor["staged_draft"] is None

        # ...so the next agent write is direct again
        resp = await client.post(
            "/api/projects/proj-a/editor/agent-write", json={"sql": "SELECT 5"}, headers=AGENT
        )
        assert resp.json() == {"mode": "written"}


async def test_agent_cannot_use_user_editor_path(ai_client):
    async with ai_client as client:
        resp = await client.put(
            "/api/projects/proj-a/editor", json={"sql": "SELECT 1"}, headers=AGENT
        )
    assert resp.status_code == 403


async def test_validate_sql(ai_data_dir, ai_client):
    _spool_clone(ai_data_dir, "proj-a", "orders", sql="SELECT 1 AS id, 'x' AS label")
    async with ai_client as client:
        ok = await client.post(
            "/api/projects/proj-a/validate", json={"sql": "SELECT id FROM orders"}, headers=AGENT
        )
        bad_column = await client.post(
            "/api/projects/proj-a/validate", json={"sql": "SELECT nope FROM orders"}, headers=AGENT
        )
        multi = await client.post(
            "/api/projects/proj-a/validate", json={"sql": "SELECT 1; SELECT 2"}, headers=AGENT
        )
        sandboxed = await client.post(
            "/api/projects/proj-a/validate",
            json={"sql": "SELECT * FROM read_parquet('/tmp/x.parquet')"},
            headers=AGENT,
        )
    assert ok.json() == {"valid": True, "columns": [{"name": "id", "type": "INTEGER"}]}
    assert bad_column.json()["valid"] is False
    assert "nope" in bad_column.json()["error"]
    assert multi.json()["valid"] is False
    assert "one statement" in multi.json()["error"]
    assert sandboxed.json()["valid"] is False  # sandbox applies even to validation


async def test_validate_never_executes(ai_data_dir, ai_client):
    """DDL validates via EXPLAIN, and injection via a second statement is rejected —
    in neither case may anything actually run."""
    _spool_clone(ai_data_dir, "proj-a", "orders")
    async with ai_client as client:
        ddl = await client.post(
            "/api/projects/proj-a/validate",
            json={"sql": "CREATE TABLE hacked AS SELECT 1 AS x"},
            headers=AGENT,
        )
        injected = await client.post(
            "/api/projects/proj-a/validate",
            json={"sql": "SELECT 1; CREATE TABLE hacked2 AS SELECT 1 AS x"},
            headers=AGENT,
        )
        tables = await client.get("/api/projects/proj-a/tables")
        schema_hacked = await client.get("/api/projects/proj-a/tables/hacked/schema")
    assert ddl.json()["valid"] is True  # valid DDL...
    assert injected.json()["valid"] is False
    assert [t["name"] for t in tables.json()] == ["orders"]  # ...but nothing was created
    assert schema_hacked.status_code == 404


async def test_agent_calls_are_audited_and_user_calls_are_not(ai_data_dir, ai_client):
    _spool_clone(ai_data_dir, "proj-a", "orders")
    async with ai_client as client:
        await client.get("/api/projects/proj-a/tables", headers=AGENT)
        await client.get("/api/projects/proj-a/tables")  # UI/user: not audited
        await client.post(
            "/api/projects/proj-a/editor/agent-write",
            json={"sql": "SELECT 1", "note": "draft"},
            headers=AGENT,
        )
        feed = (await client.get("/api/projects/proj-a/activity")).json()
    assert [(e["tool"], e["decision"]) for e in feed] == [
        ("write_editor", "allowed"),
        ("list_tables", "allowed"),
    ]
    assert feed[0]["detail"]["sql"] == "SELECT 1"


async def test_workspace_page_served(ai_client):
    async with ai_client as client:
        page = await client.get("/proj-a")
        missing = await client.get("/nope")
    assert page.status_code == 200
    assert "AI Workspace" in page.text
    assert missing.status_code == 404


def test_export_service_writes_spool(monkeypatch, tmp_path):
    """Main-app side: parquet lands first, sidecar last (the commit marker)."""
    import app.services.export_service as export_service

    monkeypatch.setattr(export_service, "DATA_DIR", tmp_path)

    class _StubManager:
        def table_exists(self, name):
            return True

        def copy_table_to(self, table, target, fmt):
            assert fmt == "parquet"
            duckdb.connect().execute(f"COPY (SELECT 42 AS answer) TO '{target}' (FORMAT PARQUET)")

        def table_stats(self, name):
            return {"row_count": 1}

    result = export_service.export_table_to_ai_workspace(
        _StubManager(), "proj-a", "answers", source_node_id="node-9"
    )
    assert result == {"destination": "ai_workspace", "table_name": "answers", "row_count": 1}
    inbox = tmp_path / "ai" / "proj-a" / "inbox"
    assert (inbox / "answers.parquet").exists()
    sidecar = json.loads((inbox / "answers.json").read_text())
    assert sidecar["source_node_id"] == "node-9"
    assert sidecar["table_name"] == "answers"
    assert sidecar["exported_at"]
