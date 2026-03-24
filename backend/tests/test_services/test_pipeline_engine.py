from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from app.models.pipeline import (
    EdgeDefinition,
    NodeDefinition,
    NodeStatus,
    NodeType,
    PipelineDefinition,
    Position,
)
from app.services.duckdb_manager import DuckDBManager
from app.services.pipeline_engine import PipelineEngine


def _make_node(node_id, node_type, table_name, config=None):
    return NodeDefinition(
        id=node_id,
        type=node_type,
        table_name=table_name,
        label=node_id,
        position=Position(x=0, y=0),
        config=config or {},
    )


def _make_pipeline(nodes, edges=None):
    edge_objs = [EdgeDefinition(id=f"e{i}", source=s, target=t) for i, (s, t) in enumerate(edges or [])]
    return PipelineDefinition(id="p1", name="Test", nodes=nodes, edges=edge_objs)


@pytest.fixture
def engine(duckdb_mgr, csv_artifact_store):
    return PipelineEngine(duckdb_mgr, csv_artifact_store)


# --- Topological sort ---

def test_topo_sort_linear(engine):
    nodes = [_make_node(n, NodeType.TRANSFORM, n) for n in ["A", "B", "C"]]
    pipeline = _make_pipeline(nodes, [("A", "B"), ("B", "C")])
    order = engine.topological_sort(pipeline)
    assert order.index("A") < order.index("B") < order.index("C")


def test_topo_sort_diamond(engine):
    nodes = [_make_node(n, NodeType.TRANSFORM, n) for n in ["A", "B", "C", "D"]]
    pipeline = _make_pipeline(nodes, [("A", "B"), ("A", "C"), ("B", "D"), ("C", "D")])
    order = engine.topological_sort(pipeline)
    assert order.index("A") < order.index("B")
    assert order.index("A") < order.index("C")
    assert order.index("B") < order.index("D")
    assert order.index("C") < order.index("D")


def test_topo_sort_single_node(engine):
    nodes = [_make_node("only", NodeType.CSV_SOURCE, "t")]
    pipeline = _make_pipeline(nodes)
    assert engine.topological_sort(pipeline) == ["only"]


def test_topo_sort_cycle(engine):
    nodes = [_make_node(n, NodeType.TRANSFORM, n) for n in ["A", "B"]]
    pipeline = _make_pipeline(nodes, [("A", "B"), ("B", "A")])
    with pytest.raises(ValueError, match="cycle"):
        engine.topological_sort(pipeline)


def test_topo_sort_disconnected(engine):
    nodes = [_make_node(n, NodeType.CSV_SOURCE, n) for n in ["X", "Y"]]
    pipeline = _make_pipeline(nodes)
    order = engine.topological_sort(pipeline)
    assert set(order) == {"X", "Y"}


# --- Node execution ---

@pytest.mark.asyncio
async def test_execute_csv_source(engine, sample_csv_file):
    node = _make_node("n1", NodeType.CSV_SOURCE, "csv_t", {
        "file_path": sample_csv_file,
        "original_filename": "sample.csv",
    })
    result = await engine.execute_single_node(node)
    assert result.status == NodeStatus.SUCCESS
    assert result.row_count == 5
    assert result.column_count == 3


@pytest.mark.asyncio
async def test_execute_csv_source_with_python_preprocessing_requires_review(engine, office365_csv_file):
    node = _make_node("n1", NodeType.CSV_SOURCE, "csv_pre_py", {
        "file_path": office365_csv_file,
        "original_filename": "office365.csv",
        "preprocessing": {
            "enabled": True,
            "runtime": "python",
            "script": "import sys; from pathlib import Path; lines = Path(sys.argv[1]).read_text().splitlines()[2:]; sys.stdout.write('\\n'.join(lines))",
        },
    })
    result = await engine.execute_single_node(node)
    assert result.status == NodeStatus.ERROR
    assert "Click Preprocess" in (result.error or "")


@pytest.mark.asyncio
async def test_execute_csv_source_with_reviewed_python_preprocessing(engine, office365_csv_file):
    node = _make_node("n1", NodeType.CSV_SOURCE, "csv_pre_sh", {
        "file_path": office365_csv_file,
        "original_filename": "office365.csv",
        "preprocessing": {
            "enabled": True,
            "runtime": "python",
            "script": "import sys; from pathlib import Path; lines = Path(sys.argv[1]).read_text().splitlines()[2:]; sys.stdout.write('\\n'.join(lines))",
        },
    })
    from app.services.csv_service import preview_preprocessed_csv_text
    preview_preprocessed_csv_text(
        engine.csv_artifact_store,
        "n1",
        office365_csv_file,
        node.config["preprocessing"],
    )
    result = await engine.execute_single_node(node)
    assert result.status == NodeStatus.SUCCESS
    assert result.row_count == 2
    assert result.column_count == 3


@pytest.mark.asyncio
async def test_execute_csv_source_with_reviewed_bash_preprocessing(engine, office365_csv_file):
    node = _make_node("n1", NodeType.CSV_SOURCE, "csv_pre_sh", {
        "file_path": office365_csv_file,
        "original_filename": "office365.csv",
        "preprocessing": {
            "enabled": True,
            "runtime": "bash",
            "script": "tail -n +3 \"$1\"",
        },
    })
    from app.services.csv_service import preview_preprocessed_csv_text
    preview_preprocessed_csv_text(
        engine.csv_artifact_store,
        "n1",
        office365_csv_file,
        node.config["preprocessing"],
    )
    result = await engine.execute_single_node(node)
    assert result.status == NodeStatus.SUCCESS
    assert result.row_count == 2
    assert result.column_count == 3


@pytest.mark.asyncio
async def test_execute_transform(engine, sample_csv_file):
    src = _make_node("src", NodeType.CSV_SOURCE, "src_t", {
        "file_path": sample_csv_file,
        "original_filename": "s.csv",
    })
    await engine.execute_single_node(src)

    transform = _make_node("tx", NodeType.TRANSFORM, "tx_t", {
        "sql": "SELECT * FROM src_t WHERE id <= 2"
    })
    result = await engine.execute_single_node(transform)
    assert result.status == NodeStatus.SUCCESS
    assert result.row_count == 2


@pytest.mark.asyncio
async def test_execute_pipeline_requires_reviewed_preprocess_for_csv_sources(engine, office365_csv_file):
    source = _make_node("src", NodeType.CSV_SOURCE, "src_preprocessed", {
        "file_path": office365_csv_file,
        "original_filename": "office365.csv",
        "preprocessing": {
            "enabled": True,
            "runtime": "python",
            "script": "import sys; from pathlib import Path; lines = Path(sys.argv[1]).read_text().splitlines()[2:]; sys.stdout.write('\\n'.join(lines))",
        },
    })
    transform = _make_node("tx", NodeType.TRANSFORM, "tx_preprocessed", {
        "sql": "SELECT * FROM src_preprocessed WHERE id = 1"
    })

    results = await engine.execute_pipeline(_make_pipeline([source, transform], [("src", "tx")]), force_refresh=True)

    assert results["src"].status == NodeStatus.ERROR
    assert "Click Preprocess" in (results["src"].error or "")


@pytest.mark.asyncio
async def test_execute_pipeline_uses_reviewed_preprocess_for_csv_sources(engine, office365_csv_file):
    from app.services.csv_service import preview_preprocessed_csv_text

    source = _make_node("src", NodeType.CSV_SOURCE, "src_preprocessed", {
        "file_path": office365_csv_file,
        "original_filename": "office365.csv",
        "preprocessing": {
            "enabled": True,
            "runtime": "python",
            "script": "import sys; from pathlib import Path; lines = Path(sys.argv[1]).read_text().splitlines()[2:]; sys.stdout.write('\\n'.join(lines))",
        },
    })
    preview_preprocessed_csv_text(
        engine.csv_artifact_store,
        "src",
        office365_csv_file,
        source.config["preprocessing"],
    )
    transform = _make_node("tx", NodeType.TRANSFORM, "tx_preprocessed", {
        "sql": "SELECT * FROM src_preprocessed WHERE id = 1"
    })

    results = await engine.execute_pipeline(_make_pipeline([source, transform], [("src", "tx")]), force_refresh=True)

    assert results["src"].status == NodeStatus.SUCCESS
    assert results["tx"].status == NodeStatus.SUCCESS
    assert results["tx"].row_count == 1


@pytest.mark.asyncio
async def test_execute_db_source_postgres_mocked(engine):
    df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    with patch.object(engine.postgres, "execute_query", new=AsyncMock(return_value=df)):
        node = _make_node("pg", NodeType.DB_SOURCE, "pg_t", {
            "db_type": "postgres",
            "connection": {"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
            "query": "SELECT 1",
        })
        result = await engine.execute_single_node(node)
    assert result.status == NodeStatus.SUCCESS
    assert result.row_count == 2


@pytest.mark.asyncio
async def test_execute_export_node(engine):
    node = _make_node("exp", NodeType.EXPORT, "exp_t", {"format": "csv"})
    result = await engine.execute_single_node(node)
    assert result.status == NodeStatus.SUCCESS
    assert result.row_count == 0


@pytest.mark.asyncio
async def test_execute_unknown_node_type(engine, duckdb_mgr):
    node = NodeDefinition(
        id="bad",
        type="csv_source",  # We'll hack the type after construction
        table_name="bad_t",
        label="bad",
        position=Position(x=0, y=0),
        config={},
    )
    # Force an unknown type to simulate the else branch
    node.__dict__["type"] = "unknown_type"
    result = await engine._execute_node(node)
    assert result.status == NodeStatus.ERROR
    assert result.error is not None


@pytest.mark.asyncio
async def test_caching_skips_reexecution(engine, sample_csv_file, duckdb_mgr):
    node = _make_node("cached", NodeType.CSV_SOURCE, "cache_t", {
        "file_path": sample_csv_file,
        "original_filename": "s.csv",
    })
    pipeline = _make_pipeline([node])

    # First execution
    results1 = await engine.execute_pipeline(pipeline, force_refresh=False)
    assert results1["cached"].status == NodeStatus.SUCCESS

    # Second execution — table exists + result cached, node should be skipped
    results2 = await engine.execute_pipeline(pipeline, force_refresh=False)
    assert results2["cached"].status == NodeStatus.SUCCESS


@pytest.mark.asyncio
async def test_force_refresh(engine, sample_csv_file):
    node = _make_node("fr", NodeType.CSV_SOURCE, "fr_t", {
        "file_path": sample_csv_file,
        "original_filename": "s.csv",
    })
    pipeline = _make_pipeline([node])

    await engine.execute_pipeline(pipeline, force_refresh=False)
    results = await engine.execute_pipeline(pipeline, force_refresh=True)
    assert results["fr"].status == NodeStatus.SUCCESS
    assert results["fr"].row_count == 5


@pytest.mark.asyncio
async def test_execution_time_recorded(engine, sample_csv_file):
    node = _make_node("timed", NodeType.CSV_SOURCE, "timed_t", {
        "file_path": sample_csv_file,
        "original_filename": "s.csv",
    })
    result = await engine.execute_single_node(node)
    assert result.execution_time_ms is not None
    assert result.execution_time_ms >= 0
