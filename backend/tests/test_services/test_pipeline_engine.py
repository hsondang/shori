import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest

from app.models.pipeline import (
    EdgeDefinition,
    NodeDefinition,
    NodeExecutionResult,
    NodeStatus,
    NodeType,
    PipelineDefinition,
    Position,
)
from app.services.duckdb_manager import DuckDBManager
from app.services.execution_registry import ExecutionCancelled, ExecutionRegistry
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
async def test_execute_excel_source(engine, sample_excel_file):
    from app.services.excel_service import materialize_excel_sheet

    materialized = materialize_excel_sheet(sample_excel_file, "Orders")
    node = _make_node("n1", NodeType.EXCEL_SOURCE, "excel_t", {
        "file_path": sample_excel_file,
        "original_filename": "sample.xlsx",
        "sheet_names": ["Orders", "Summary"],
        "selected_sheet": "Orders",
        "materialized_csv_path": materialized["file_path"],
        "materialized_csv_filename": materialized["filename"],
    })

    result = await engine.execute_single_node(node)

    assert result.status == NodeStatus.SUCCESS
    assert result.row_count == 2
    assert result.column_count == 3


@pytest.mark.asyncio
async def test_execute_excel_source_with_preprocessing_requires_review(engine, sample_excel_file):
    from app.services.excel_service import materialize_excel_sheet

    materialized = materialize_excel_sheet(sample_excel_file, "Orders")
    node = _make_node("n1", NodeType.EXCEL_SOURCE, "excel_pre", {
        "file_path": sample_excel_file,
        "original_filename": "sample.xlsx",
        "sheet_names": ["Orders", "Summary"],
        "selected_sheet": "Orders",
        "materialized_csv_path": materialized["file_path"],
        "materialized_csv_filename": materialized["filename"],
        "preprocessing": {
            "enabled": True,
            "runtime": "python",
            "script": "import sys; from pathlib import Path; lines = Path(sys.argv[1]).read_text().splitlines()[1:]; sys.stdout.write('\\n'.join(lines))",
        },
    })

    result = await engine.execute_single_node(node)

    assert result.status == NodeStatus.ERROR
    assert "Click Preprocess" in (result.error or "")


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
    connection = AsyncMock()
    with (
        patch.object(engine.postgres, "connect", new=AsyncMock(return_value=connection)),
        patch.object(engine.postgres, "fetch_query", new=AsyncMock(return_value=df)),
    ):
        node = _make_node("pg", NodeType.DB_SOURCE, "pg_t", {
            "db_type": "postgres",
            "connection": {"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
            "query": "SELECT 1",
        })
        result = await engine.execute_single_node(node)
    assert result.status == NodeStatus.SUCCESS
    assert result.row_count == 2


@pytest.mark.asyncio
async def test_execute_db_source_oracle_fetchall_uses_fetch_config(engine):
    connection = AsyncMock()

    with (
        patch.object(engine.oracle, "connect", new=AsyncMock(return_value=connection)),
        patch.object(engine.oracle, "load_query_to_duckdb", new=AsyncMock(return_value={
            "row_count": 2,
            "column_count": 2,
            "columns": ["ID", "NAME"],
        })) as load_query_to_duckdb,
    ):
        node = _make_node("oracle", NodeType.DB_SOURCE, "oracle_t", {
            "db_type": "oracle",
            "connection": {"host": "h", "port": 1521, "service_name": "svc", "user": "u", "password": "p"},
            "query": "SELECT * FROM dual",
            "fetch_config": {
                "mode": "fetchall",
                "arraysize": 500,
                "prefetchrows": 10,
            },
        })
        result = await engine.execute_single_node(node)

    assert result.status == NodeStatus.SUCCESS
    assert result.row_count == 2
    load_query_to_duckdb.assert_awaited_once_with(
        connection,
        "SELECT * FROM dual",
        "oracle_t",
        engine.duckdb,
        {"mode": "fetchall", "arraysize": 500, "prefetchrows": 10},
        node_id="oracle",
        cache_key=None,
    )


@pytest.mark.asyncio
async def test_execute_db_source_oracle_fetchmany_loads_directly_into_duckdb(engine):
    connection = AsyncMock()

    with (
        patch.object(engine.oracle, "connect", new=AsyncMock(return_value=connection)),
        patch.object(engine.oracle, "load_query_to_duckdb", new=AsyncMock(return_value={
            "row_count": 3,
            "column_count": 2,
            "columns": ["ID", "NAME"],
        })) as load_query_to_duckdb,
        patch.object(engine.oracle, "fetch_query", new=AsyncMock()) as fetch_query,
    ):
        node = _make_node("oracle", NodeType.DB_SOURCE, "oracle_chunked_t", {
            "db_type": "oracle",
            "connection": {"host": "h", "port": 1521, "service_name": "svc", "user": "u", "password": "p"},
            "query": "SELECT * FROM dual",
            "fetch_config": {
                "mode": "fetchmany",
                "arraysize": 1000,
                "prefetchrows": 2,
            },
        })
        result = await engine.execute_single_node(node)

    assert result.status == NodeStatus.SUCCESS
    assert result.row_count == 3
    load_query_to_duckdb.assert_awaited_once_with(
        connection,
        "SELECT * FROM dual",
        "oracle_chunked_t",
        engine.duckdb,
        {"mode": "fetchmany", "arraysize": 1000, "prefetchrows": 2},
        node_id="oracle",
        cache_key=None,
    )
    fetch_query.assert_not_called()


@pytest.mark.asyncio
async def test_execute_db_source_oracle_without_fetch_config_defaults_to_fetchall(engine):
    connection = AsyncMock()

    with (
        patch.object(engine.oracle, "connect", new=AsyncMock(return_value=connection)),
        patch.object(engine.oracle, "fetch_query", new=AsyncMock()) as fetch_query,
        patch.object(engine.oracle, "load_query_to_duckdb", new=AsyncMock(return_value={
            "row_count": 1,
            "column_count": 2,
            "columns": ["ID", "NAME"],
        })) as load_query_to_duckdb,
    ):
        node = _make_node("oracle", NodeType.DB_SOURCE, "oracle_default_t", {
            "db_type": "oracle",
            "connection": {"host": "h", "port": 1521, "service_name": "svc", "user": "u", "password": "p"},
            "query": "SELECT * FROM dual",
        })
        result = await engine.execute_single_node(node)

    assert result.status == NodeStatus.SUCCESS
    load_query_to_duckdb.assert_awaited_once_with(
        connection,
        "SELECT * FROM dual",
        "oracle_default_t",
        engine.duckdb,
        None,
        node_id="oracle",
        cache_key=None,
    )
    fetch_query.assert_not_called()


@pytest.mark.asyncio
async def test_execute_db_source_reports_connecting_before_running(engine):
    df = pd.DataFrame({"col1": [1], "col2": ["a"]})
    connection = AsyncMock()
    updates = []
    finishes = []

    with (
        patch.object(engine.postgres, "connect", new=AsyncMock(return_value=connection)),
        patch.object(engine.postgres, "fetch_query", new=AsyncMock(return_value=df)),
    ):
        node = _make_node("pg", NodeType.DB_SOURCE, "pg_connecting_t", {
            "db_type": "postgres",
            "connection": {"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
            "query": "SELECT 1",
        })
        result = await engine.execute_single_node(
            node,
            on_node_update=lambda current: updates.append(current),
            on_node_finish=lambda current: finishes.append(current),
        )

    assert updates[0].status == NodeStatus.CONNECTING
    assert updates[1].status == NodeStatus.RUNNING
    assert updates[1].started_at is not None
    assert result.status == NodeStatus.SUCCESS
    assert result.started_at == updates[1].started_at
    assert finishes[0].started_at == result.started_at


@pytest.mark.asyncio
async def test_execute_db_source_connection_failure_stays_in_connecting_phase(engine):
    updates = []

    with patch.object(engine.postgres, "connect", new=AsyncMock(side_effect=RuntimeError("connect boom"))):
        node = _make_node("pg", NodeType.DB_SOURCE, "pg_connect_fail_t", {
            "db_type": "postgres",
            "connection": {"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
            "query": "SELECT 1",
        })
        result = await engine.execute_single_node(
            node,
            on_node_update=lambda current: updates.append(current),
        )

    assert updates[0].status == NodeStatus.CONNECTING
    assert result.status == NodeStatus.ERROR
    assert result.error == "connect boom"


@pytest.mark.asyncio
async def test_execute_db_source_query_failure_reports_running_phase(engine):
    connection = AsyncMock()
    updates = []

    with (
        patch.object(engine.postgres, "connect", new=AsyncMock(return_value=connection)),
        patch.object(engine.postgres, "fetch_query", new=AsyncMock(side_effect=RuntimeError("query boom"))),
    ):
        node = _make_node("pg", NodeType.DB_SOURCE, "pg_query_fail_t", {
            "db_type": "postgres",
            "connection": {"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
            "query": "SELECT 1",
        })
        result = await engine.execute_single_node(
            node,
            on_node_update=lambda current: updates.append(current),
        )

    assert [update.status for update in updates] == [NodeStatus.CONNECTING, NodeStatus.RUNNING]
    assert result.status == NodeStatus.ERROR
    assert result.error == "query boom"
    assert result.started_at == updates[1].started_at


@pytest.mark.asyncio
async def test_execute_db_source_postgres_registers_abort_callback(engine):
    connection = Mock()
    connection.is_closed.return_value = False
    connection.terminate = Mock()
    connection.close = AsyncMock()
    query_started = asyncio.Event()
    allow_finish = asyncio.Event()
    registry = ExecutionRegistry(retention_seconds=60)
    run = registry.create_run("node", ["pg"])
    controller = registry.create_controller(run.execution_id)

    async def fetch_query(_connection, _query):
        query_started.set()
        await allow_finish.wait()
        return pd.DataFrame({"col1": [1]})

    with (
        patch.object(engine.postgres, "connect", new=AsyncMock(return_value=connection)),
        patch.object(engine.postgres, "fetch_query", new=AsyncMock(side_effect=fetch_query)),
    ):
        node = _make_node("pg", NodeType.DB_SOURCE, "pg_abort_t", {
            "db_type": "postgres",
            "connection": {"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
            "query": "SELECT pg_sleep(10)",
        })
        task = asyncio.create_task(engine.execute_single_node(node, execution_controller=controller))
        registry.attach_task(run.execution_id, task)
        await query_started.wait()

        snapshot = registry.abort_run(run.execution_id)
        allow_finish.set()

        assert snapshot is not None
        assert snapshot.status == NodeStatus.CANCELLED
        connection.terminate.assert_called_once()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_execute_db_source_oracle_registers_abort_callback(engine):
    connection = Mock()
    connection.cancel = Mock()
    connection.close = AsyncMock()
    query_started = asyncio.Event()
    allow_finish = asyncio.Event()
    registry = ExecutionRegistry(retention_seconds=60)
    run = registry.create_run("node", ["oracle"])
    controller = registry.create_controller(run.execution_id)

    async def load_query_to_duckdb(_connection, _query, _table, _duckdb, _fetch_config=None, **_kwargs):
        query_started.set()
        await allow_finish.wait()
        return {"row_count": 1, "column_count": 1, "columns": ["col1"]}

    with (
        patch.object(engine.oracle, "connect", new=AsyncMock(return_value=connection)),
        patch.object(engine.oracle, "load_query_to_duckdb", new=AsyncMock(side_effect=load_query_to_duckdb)),
    ):
        node = _make_node("oracle", NodeType.DB_SOURCE, "oracle_abort_t", {
            "db_type": "oracle",
            "connection": {"host": "h", "port": 1521, "service_name": "svc", "user": "u", "password": "p"},
            "query": "SELECT 1 FROM dual",
        })
        task = asyncio.create_task(engine.execute_single_node(node, execution_controller=controller))
        registry.attach_task(run.execution_id, task)
        await query_started.wait()

        snapshot = registry.abort_run(run.execution_id)
        allow_finish.set()

        assert snapshot is not None
        assert snapshot.status == NodeStatus.CANCELLED
        connection.cancel.assert_called_once()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_execute_pipeline_stops_before_downstream_node_after_cancellation(engine):
    registry = ExecutionRegistry(retention_seconds=60)
    run = registry.create_run("pipeline", ["src", "tx"])
    controller = registry.create_controller(run.execution_id)
    source = _make_node("src", NodeType.CSV_SOURCE, "src_t", {
        "file_path": "sample.csv",
        "original_filename": "sample.csv",
    })
    transform = _make_node("tx", NodeType.TRANSFORM, "tx_t", {
        "sql": "SELECT * FROM src_t",
    })

    async def execute_node(node, *, cache_key=None, started_at=None, on_node_update=None, execution_controller=None):
        if node.id == "src":
            registry.abort_run(run.execution_id)
            return NodeExecutionResult(
                node_id=node.id,
                status=NodeStatus.SUCCESS,
                row_count=1,
                column_count=1,
                columns=["id"],
                started_at=started_at,
                finished_at=started_at,
            )
        raise AssertionError("downstream node should not execute")

    with patch.object(engine, "_execute_node", side_effect=execute_node):
        with pytest.raises(ExecutionCancelled):
            await engine.execute_pipeline(
                _make_pipeline([source, transform], [("src", "tx")]),
                force_refresh=True,
                execution_controller=controller,
            )


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
    assert result.started_at is not None
    assert result.finished_at is not None


@pytest.mark.asyncio
async def test_execute_pipeline_reports_node_lifecycle_callbacks(engine, sample_csv_file):
    node = _make_node("timed", NodeType.CSV_SOURCE, "timed_cb_t", {
        "file_path": sample_csv_file,
        "original_filename": "s.csv",
    })
    starts = []
    finishes = []

    results = await engine.execute_pipeline(
        _make_pipeline([node]),
        on_node_start=lambda node_id, started_at: starts.append((node_id, started_at)),
        on_node_finish=lambda result: finishes.append(result),
    )

    assert results["timed"].status == NodeStatus.SUCCESS
    assert starts == [("timed", results["timed"].started_at)]
    assert len(finishes) == 1
    assert finishes[0].node_id == "timed"
    assert finishes[0].finished_at is not None


# --- Concurrent scheduling ---

@pytest.mark.asyncio
async def test_execute_pipeline_runs_independent_nodes_concurrently(engine):
    in_flight = 0
    max_in_flight = 0

    async def fake_execute_node(node, *, cache_key=None, started_at=None, on_node_update=None, execution_controller=None):
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.02)
        in_flight -= 1
        return NodeExecutionResult(
            node_id=node.id,
            status=NodeStatus.SUCCESS,
            row_count=1,
            column_count=1,
            columns=["id"],
            started_at=started_at,
            finished_at=started_at,
        )

    engine._execute_node = fake_execute_node
    nodes = [_make_node(f"n{i}", NodeType.TRANSFORM, f"t{i}", {"sql": "SELECT 1"}) for i in range(3)]
    pipeline = _make_pipeline(nodes)

    results = await engine.execute_pipeline(pipeline)

    assert all(result.status == NodeStatus.SUCCESS for result in results.values())
    assert max_in_flight == 3


@pytest.mark.asyncio
async def test_execute_pipeline_respects_max_concurrent_nodes(duckdb_mgr, csv_artifact_store):
    engine = PipelineEngine(duckdb_mgr, csv_artifact_store, max_concurrent_nodes=2)
    in_flight = 0
    max_in_flight = 0

    async def fake_execute_node(node, *, cache_key=None, started_at=None, on_node_update=None, execution_controller=None):
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.02)
        in_flight -= 1
        return NodeExecutionResult(node_id=node.id, status=NodeStatus.SUCCESS, row_count=1, column_count=1, columns=["id"])

    engine._execute_node = fake_execute_node
    nodes = [_make_node(f"n{i}", NodeType.TRANSFORM, f"t{i}", {"sql": "SELECT 1"}) for i in range(5)]
    pipeline = _make_pipeline(nodes)

    await engine.execute_pipeline(pipeline)

    assert max_in_flight == 2


@pytest.mark.asyncio
async def test_execute_pipeline_dependent_node_waits_for_upstream(engine):
    finished_order = []

    async def fake_execute_node(node, *, cache_key=None, started_at=None, on_node_update=None, execution_controller=None):
        await asyncio.sleep(0.03 if node.id == "src" else 0.0)
        finished_order.append(node.id)
        return NodeExecutionResult(node_id=node.id, status=NodeStatus.SUCCESS, row_count=1, column_count=1, columns=["id"])

    engine._execute_node = fake_execute_node
    nodes = [
        _make_node("src", NodeType.TRANSFORM, "src_t", {"sql": "SELECT 1"}),
        _make_node("child", NodeType.TRANSFORM, "child_t", {"sql": "SELECT 1"}),
        _make_node("other", NodeType.TRANSFORM, "other_t", {"sql": "SELECT 1"}),
    ]
    pipeline = _make_pipeline(nodes, [("src", "child")])

    results = await engine.execute_pipeline(pipeline)

    assert all(result.status == NodeStatus.SUCCESS for result in results.values())
    # "other" has no upstream and should not wait for the slow "src";
    # "child" must wait for "src".
    assert finished_order.index("other") < finished_order.index("src")
    assert finished_order.index("src") < finished_order.index("child")


@pytest.mark.asyncio
async def test_execute_pipeline_failure_cancels_descendants_but_not_independent_branches(engine):
    async def fake_execute_node(node, *, cache_key=None, started_at=None, on_node_update=None, execution_controller=None):
        if node.id == "bad":
            return NodeExecutionResult(node_id=node.id, status=NodeStatus.ERROR, error="boom")
        return NodeExecutionResult(node_id=node.id, status=NodeStatus.SUCCESS, row_count=1, column_count=1, columns=["id"])

    engine._execute_node = fake_execute_node
    nodes = [
        _make_node("bad", NodeType.TRANSFORM, "bad_t", {"sql": "SELECT 1"}),
        _make_node("bad_child", NodeType.TRANSFORM, "bad_child_t", {"sql": "SELECT 1"}),
        _make_node("bad_grandchild", NodeType.TRANSFORM, "bad_grandchild_t", {"sql": "SELECT 1"}),
        _make_node("independent", NodeType.TRANSFORM, "independent_t", {"sql": "SELECT 1"}),
    ]
    pipeline = _make_pipeline(nodes, [("bad", "bad_child"), ("bad_child", "bad_grandchild")])

    results = await engine.execute_pipeline(pipeline)

    assert results["bad"].status == NodeStatus.ERROR
    assert results["bad_child"].status == NodeStatus.CANCELLED
    assert "Upstream node failed" in results["bad_child"].error
    assert results["bad_grandchild"].status == NodeStatus.CANCELLED
    assert results["independent"].status == NodeStatus.SUCCESS


@pytest.mark.asyncio
async def test_execute_pipeline_rejects_duplicate_table_names(engine):
    nodes = [
        _make_node("a", NodeType.TRANSFORM, "same_t", {"sql": "SELECT 1"}),
        _make_node("b", NodeType.TRANSFORM, "same_t", {"sql": "SELECT 2"}),
    ]
    pipeline = _make_pipeline(nodes)

    with pytest.raises(ValueError, match="table name"):
        await engine.execute_pipeline(pipeline)


@pytest.mark.asyncio
async def test_execute_pipeline_serves_cached_node_without_rerunning(engine, sample_csv_file):
    node = _make_node("csv", NodeType.CSV_SOURCE, "cached_csv_t", {
        "file_path": sample_csv_file,
        "original_filename": "sample.csv",
    })
    pipeline = _make_pipeline([node])

    first = await engine.execute_pipeline(pipeline)
    assert first["csv"].status == NodeStatus.SUCCESS
    assert first["csv"].cached is False

    calls = []
    original = engine._execute_node

    async def spying_execute_node(node, **kwargs):
        calls.append(node.id)
        return await original(node, **kwargs)

    engine._execute_node = spying_execute_node
    second = await engine.execute_pipeline(pipeline)

    assert second["csv"].status == NodeStatus.SUCCESS
    assert second["csv"].cached is True
    assert second["csv"].row_count == 5
    assert calls == []


@pytest.mark.asyncio
async def test_execute_pipeline_reruns_when_config_changes(engine, sample_csv_file, tmp_path):
    node = _make_node("csv", NodeType.CSV_SOURCE, "rerun_csv_t", {
        "file_path": sample_csv_file,
        "original_filename": "sample.csv",
    })
    first = await engine.execute_pipeline(_make_pipeline([node]))
    assert first["csv"].cached is False

    other_csv = tmp_path / "other.csv"
    other_csv.write_text("id,name\n1,Zed\n")
    changed = _make_node("csv", NodeType.CSV_SOURCE, "rerun_csv_t", {
        "file_path": str(other_csv),
        "original_filename": "other.csv",
    })
    second = await engine.execute_pipeline(_make_pipeline([changed]))

    assert second["csv"].cached is False
    assert second["csv"].row_count == 1


@pytest.mark.asyncio
async def test_execute_pipeline_upstream_change_invalidates_downstream(engine, sample_csv_file, tmp_path):
    source = _make_node("src", NodeType.CSV_SOURCE, "merkle_src_t", {
        "file_path": sample_csv_file,
        "original_filename": "sample.csv",
    })
    transform = _make_node("tx", NodeType.TRANSFORM, "merkle_tx_t", {
        "sql": "SELECT * FROM merkle_src_t",
    })
    pipeline = _make_pipeline([source, transform], [("src", "tx")])

    first = await engine.execute_pipeline(pipeline)
    assert first["tx"].row_count == 5

    other_csv = tmp_path / "merkle_other.csv"
    other_csv.write_text("id,name,value\n9,Zed,1.0\n")
    changed_source = _make_node("src", NodeType.CSV_SOURCE, "merkle_src_t", {
        "file_path": str(other_csv),
        "original_filename": "merkle_other.csv",
    })
    second = await engine.execute_pipeline(_make_pipeline([changed_source, transform], [("src", "tx")]))

    # The transform's own config didn't change, but its upstream did — it must rerun.
    assert second["tx"].cached is False
    assert second["tx"].row_count == 1
