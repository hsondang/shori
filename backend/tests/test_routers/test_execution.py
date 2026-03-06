import pytest


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
