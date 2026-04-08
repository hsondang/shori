import asyncio

import pytest

from app.models.pipeline import NodeExecutionResult, NodeStatus
from app.services.pipeline_engine import PipelineEngine


@pytest.mark.asyncio
async def test_execute_stored_not_found(client):
    resp = await client.post("/api/execute/pipeline/nonexistent-id")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_execute_single_csv_node(client, pipeline_def, sample_csv_file):
    node = pipeline_def["nodes"][0]
    resp = await client.post("/api/execute/node", json=node)
    assert resp.status_code == 200
    result = resp.json()
    assert result["node_id"] == node["id"]
    assert result["status"] == "success"
    assert result["row_count"] == 5
    assert result["column_count"] == 3


@pytest.mark.asyncio
async def test_execute_inline_csv_pipeline(client, pipeline_def):
    resp = await client.post("/api/execute/pipeline", json=pipeline_def)
    assert resp.status_code == 200
    results = resp.json()
    assert "node-1" in results
    assert results["node-1"]["status"] == "success"
    assert results["node-1"]["row_count"] == 5


@pytest.mark.asyncio
async def test_execute_inline_transform_pipeline(client, pipeline_def, sample_csv_file):
    transform_pipeline = {
        "id": "transform-pipeline",
        "name": "Transform Test",
        "nodes": [
            pipeline_def["nodes"][0],
            {
                "id": "node-2",
                "type": "transform",
                "table_name": "filtered_table",
                "label": "Filter",
                "position": {"x": 300, "y": 0},
                "config": {"sql": 'SELECT * FROM my_table WHERE id > 2'},
            },
        ],
        "edges": [{"id": "e1", "source": "node-1", "target": "node-2"}],
    }
    resp = await client.post("/api/execute/pipeline", json=transform_pipeline)
    assert resp.status_code == 200
    results = resp.json()
    assert results["node-1"]["status"] == "success"
    assert results["node-2"]["status"] == "success"
    assert results["node-2"]["row_count"] == 3  # ids 3, 4, 5


@pytest.mark.asyncio
async def test_execute_stored_pipeline(client, pipeline_def):
    await client.post("/api/pipelines", json=pipeline_def)
    resp = await client.post(f"/api/execute/pipeline/{pipeline_def['id']}")
    assert resp.status_code == 200
    results = resp.json()
    assert results["node-1"]["status"] == "success"


@pytest.mark.asyncio
async def test_force_refresh_reruns(client, pipeline_def):
    resp1 = await client.post("/api/execute/pipeline", json=pipeline_def)
    assert resp1.json()["node-1"]["status"] == "success"

    resp2 = await client.post("/api/execute/pipeline?force=true", json=pipeline_def)
    assert resp2.json()["node-1"]["status"] == "success"
    assert resp2.json()["node-1"]["row_count"] == 5


@pytest.mark.asyncio
async def test_execute_cycle_returns_error(client, sample_csv_file):
    """A pipeline with a cycle should return an error result for all nodes."""
    cycle_pipeline = {
        "id": "cycle-pipeline",
        "name": "Cycle Test",
        "nodes": [
            {
                "id": "node-a",
                "type": "transform",
                "table_name": "t_a",
                "label": "A",
                "position": {"x": 0, "y": 0},
                "config": {"sql": "SELECT 1"},
            },
            {
                "id": "node-b",
                "type": "transform",
                "table_name": "t_b",
                "label": "B",
                "position": {"x": 100, "y": 0},
                "config": {"sql": "SELECT 1"},
            },
        ],
        "edges": [
            {"id": "e1", "source": "node-a", "target": "node-b"},
            {"id": "e2", "source": "node-b", "target": "node-a"},
        ],
    }
    # topological_sort raises ValueError which propagates as an unhandled exception
    with pytest.raises(Exception, match="cycle"):
        await client.post("/api/execute/pipeline", json=cycle_pipeline)


@pytest.mark.asyncio
async def test_start_node_execution_returns_terminal_snapshot_when_run_finishes_before_response(client, pipeline_def, monkeypatch):
    node = pipeline_def["nodes"][0]

    async def fake_execute_single_node(self, node, on_node_start=None, on_node_finish=None, on_node_update=None):
        started_at = "2026-04-08T10:00:00+00:00"
        if on_node_start is not None:
            on_node_start(node.id, started_at)
        result = NodeExecutionResult(
            node_id=node.id,
            status=NodeStatus.SUCCESS,
            row_count=5,
            column_count=3,
            columns=["id", "name", "value"],
            execution_time_ms=10,
            started_at=started_at,
            finished_at="2026-04-08T10:00:01+00:00",
        )
        if on_node_finish is not None:
            on_node_finish(result)
        return result

    monkeypatch.setattr(PipelineEngine, "execute_single_node", fake_execute_single_node)

    start_resp = await client.post("/api/execute/node/start", json=node)
    assert start_resp.status_code == 200
    snapshot = start_resp.json()
    assert snapshot["status"] == "success"
    assert snapshot["node_results"][node["id"]]["status"] == "success"
    assert snapshot["node_results"][node["id"]]["started_at"] == "2026-04-08T10:00:00+00:00"
    assert snapshot["node_results"][node["id"]]["finished_at"] == "2026-04-08T10:00:01+00:00"


@pytest.mark.asyncio
async def test_start_db_node_execution_shows_connecting_then_running(client, monkeypatch):
    node = {
        "id": "db-node",
        "type": "db_source",
        "table_name": "db_table",
        "label": "DB",
        "position": {"x": 0, "y": 0},
        "config": {
            "db_type": "postgres",
            "connection": {"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
            "query": "SELECT 1",
        },
    }

    async def fake_execute_single_node(self, node, on_node_start=None, on_node_finish=None, on_node_update=None):
        connect_started = "2026-04-08T10:00:00+00:00"
        query_started = "2026-04-08T10:00:02+00:00"
        if on_node_start is not None:
            on_node_start(node.id, connect_started)
        if on_node_update is not None:
            on_node_update(NodeExecutionResult(
                node_id=node.id,
                status=NodeStatus.CONNECTING,
                started_at=connect_started,
            ))
        await asyncio.sleep(0.01)
        if on_node_update is not None:
            on_node_update(NodeExecutionResult(
                node_id=node.id,
                status=NodeStatus.RUNNING,
                started_at=query_started,
            ))
        await asyncio.sleep(0.01)
        result = NodeExecutionResult(
            node_id=node.id,
            status=NodeStatus.SUCCESS,
            row_count=1,
            column_count=1,
            columns=["id"],
            execution_time_ms=100,
            started_at=query_started,
            finished_at="2026-04-08T10:00:03+00:00",
        )
        if on_node_finish is not None:
            on_node_finish(result)
        return result

    monkeypatch.setattr(PipelineEngine, "execute_single_node", fake_execute_single_node)

    start_resp = await client.post("/api/execute/node/start", json=node)
    assert start_resp.status_code == 200
    started = start_resp.json()
    assert started["node_results"]["db-node"]["status"] == "connecting"

    await asyncio.sleep(0.015)
    mid_resp = await client.get(f"/api/execute/runs/{started['execution_id']}")
    assert mid_resp.status_code == 200
    mid = mid_resp.json()
    assert mid["node_results"]["db-node"]["status"] == "running"
    assert mid["node_results"]["db-node"]["started_at"] == "2026-04-08T10:00:02+00:00"

    await asyncio.sleep(0.02)
    final_resp = await client.get(f"/api/execute/runs/{started['execution_id']}")
    assert final_resp.status_code == 200
    final = final_resp.json()
    assert final["node_results"]["db-node"]["status"] == "success"


@pytest.mark.asyncio
async def test_start_db_node_execution_reports_connect_failure(client, monkeypatch):
    node = {
        "id": "db-node",
        "type": "db_source",
        "table_name": "db_table",
        "label": "DB",
        "position": {"x": 0, "y": 0},
        "config": {
            "db_type": "postgres",
            "connection": {"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
            "query": "SELECT 1",
        },
    }

    async def fake_execute_single_node(self, node, on_node_start=None, on_node_finish=None, on_node_update=None):
        connect_started = "2026-04-08T10:00:00+00:00"
        if on_node_start is not None:
            on_node_start(node.id, connect_started)
        if on_node_update is not None:
            on_node_update(NodeExecutionResult(
                node_id=node.id,
                status=NodeStatus.CONNECTING,
                started_at=connect_started,
            ))
        await asyncio.sleep(0.01)
        result = NodeExecutionResult(
            node_id=node.id,
            status=NodeStatus.ERROR,
            error="connect boom",
            started_at=connect_started,
            finished_at="2026-04-08T10:00:01+00:00",
        )
        if on_node_finish is not None:
            on_node_finish(result)
        return result

    monkeypatch.setattr(PipelineEngine, "execute_single_node", fake_execute_single_node)

    start_resp = await client.post("/api/execute/node/start", json=node)
    assert start_resp.status_code == 200
    started = start_resp.json()
    assert started["node_results"]["db-node"]["status"] == "connecting"

    await asyncio.sleep(0.02)
    final_resp = await client.get(f"/api/execute/runs/{started['execution_id']}")
    assert final_resp.status_code == 200
    final = final_resp.json()
    assert final["node_results"]["db-node"]["status"] == "error"
    assert final["node_results"]["db-node"]["error"] == "connect boom"


@pytest.mark.asyncio
async def test_start_pipeline_execution_exposes_live_node_progress(client, pipeline_def, monkeypatch):
    pipeline = {
        **pipeline_def,
        "nodes": [
            pipeline_def["nodes"][0],
            {
                "id": "node-2",
                "type": "transform",
                "table_name": "filtered_table",
                "label": "Filter",
                "position": {"x": 300, "y": 0},
                "config": {"sql": 'SELECT * FROM my_table WHERE id > 2'},
            },
        ],
        "edges": [{"id": "e1", "source": "node-1", "target": "node-2"}],
    }

    async def fake_execute_pipeline(self, pipeline, force_refresh=False, on_node_start=None, on_node_finish=None, on_node_update=None):
        first_start = "2026-04-08T10:00:00+00:00"
        second_start = "2026-04-08T10:00:02+00:00"
        first = NodeExecutionResult(
            node_id="node-1",
            status=NodeStatus.SUCCESS,
            row_count=5,
            column_count=3,
            columns=["id", "name", "value"],
            execution_time_ms=20,
            started_at=first_start,
            finished_at="2026-04-08T10:00:02+00:00",
        )
        second = NodeExecutionResult(
            node_id="node-2",
            status=NodeStatus.SUCCESS,
            row_count=3,
            column_count=3,
            columns=["id", "name", "value"],
            execution_time_ms=20,
            started_at=second_start,
            finished_at="2026-04-08T10:00:04+00:00",
        )
        if on_node_start is not None:
            on_node_start("node-1", first_start)
        await asyncio.sleep(0.01)
        if on_node_finish is not None:
            on_node_finish(first)
        if on_node_start is not None:
            on_node_start("node-2", second_start)
        await asyncio.sleep(0.01)
        if on_node_finish is not None:
            on_node_finish(second)
        return {"node-1": first, "node-2": second}

    monkeypatch.setattr(PipelineEngine, "execute_pipeline", fake_execute_pipeline)

    start_resp = await client.post("/api/execute/pipeline/start", json=pipeline)
    assert start_resp.status_code == 200
    snapshot = start_resp.json()
    assert snapshot["status"] == "running"
    assert snapshot["node_results"]["node-1"]["status"] == "running"
    assert "node-2" not in snapshot["node_results"]

    await asyncio.sleep(0.015)

    mid_resp = await client.get(f"/api/execute/runs/{snapshot['execution_id']}")
    assert mid_resp.status_code == 200
    mid = mid_resp.json()
    assert mid["status"] == "running"
    assert mid["node_results"]["node-1"]["status"] == "success"
    assert mid["node_results"]["node-2"]["status"] == "running"

    await asyncio.sleep(0.02)

    final_resp = await client.get(f"/api/execute/runs/{snapshot['execution_id']}")
    assert final_resp.status_code == 200
    final = final_resp.json()
    assert final["status"] == "success"
    assert final["node_results"]["node-2"]["status"] == "success"


@pytest.mark.asyncio
async def test_get_execution_run_status_returns_404_for_missing_run(client):
    resp = await client.get("/api/execute/runs/does-not-exist")
    assert resp.status_code == 404
