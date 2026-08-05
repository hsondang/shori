"""DuckDB-backed streaming live preview for transform nodes."""

import pandas as pd
import pytest

from app.models.pipeline import (
    EdgeDefinition,
    NodeDefinition,
    NodeType,
    PipelineDefinition,
    Position,
)
from app.services.pipeline_graph import resolve_direct_upstreams
from app.services.duckdb_manager import DuckDBManager, LOCATION_MEMORY
from app.services.preview_sessions import PreviewSessionManager


@pytest.fixture
def file_mgr(tmp_path):
    mgr = DuckDBManager(str(tmp_path / "project.duckdb"))
    yield mgr
    mgr.close()


def _transform(node_id: str, table_name: str, sql: str) -> NodeDefinition:
    return NodeDefinition(
        id=node_id,
        type=NodeType.TRANSFORM,
        table_name=table_name,
        label=table_name,
        position=Position(x=0, y=0),
        config={"sql": sql},
    )


@pytest.mark.asyncio
async def test_transform_preview_streams_in_chunks(file_mgr):
    file_mgr.register_dataframe(
        "src", pd.DataFrame({"id": [1, 2, 3]}), node_id="s", cache_key="k", into_memory=True
    )
    sessions = PreviewSessionManager(None)
    node = _transform("t", "t_out", "SELECT id * 10 AS v FROM src ORDER BY id")
    try:
        start = await sessions.start_transform(
            project_id="p", node=node, cache_key="c", duckdb=file_mgr,
            sql=node.config["sql"], upstream_resolution={"src": LOCATION_MEMORY},
            chunk_rows=2, max_buffer_rows=100, ttl_seconds=60,
        )
        assert start["columns"] == ["v"]
        assert [row[0] for row in start["rows"]] == [10, 20]
        assert start["has_more"] is True

        more = await sessions.fetch_more(start["session_id"])
        assert [row[0] for row in more["rows"]] == [30]
        assert more["has_more"] is False
    finally:
        await sessions.close_all()


@pytest.mark.asyncio
async def test_transform_preview_reads_resolved_copy(file_mgr):
    # Upstream "u": a stale materialized (disk) copy vs a fresh in-memory copy,
    # holding different values so we can tell which the preview read.
    file_mgr.register_dataframe("u", pd.DataFrame({"v": ["disk"]}), node_id="u", cache_key="k_old", into_memory=False)
    file_mgr.register_dataframe("u", pd.DataFrame({"v": ["mem"]}), node_id="u", cache_key="k_new", into_memory=True)
    sessions = PreviewSessionManager(None)
    node = _transform("t", "t_out", "SELECT v FROM u")
    try:
        # Pinned to in-memory via a run-scoped temp view → reads the fresh copy.
        resolved = await sessions.start_transform(
            project_id="p", node=node, cache_key="c", duckdb=file_mgr,
            sql=node.config["sql"], upstream_resolution={"u": LOCATION_MEMORY},
            chunk_rows=10, max_buffer_rows=100, ttl_seconds=60,
        )
        assert resolved["rows"][0][0] == "mem"

        # No resolution → default project(disk)-first search path → disk copy.
        default = await sessions.start_transform(
            project_id="p", node=node, cache_key="c", duckdb=file_mgr,
            sql=node.config["sql"], upstream_resolution={},
            chunk_rows=10, max_buffer_rows=100, ttl_seconds=60,
        )
        assert default["rows"][0][0] == "disk"
    finally:
        await sessions.close_all()


def test_transform_upstream_gate_flags_missing(file_mgr):
    src = _transform("src", "src_t", "SELECT 1")
    tx = _transform("tx", "tx_t", "SELECT * FROM src_t")
    pipeline = PipelineDefinition(
        id="p", name="p", nodes=[src, tx],
        edges=[EdgeDefinition(id="e", source="src", target="tx")],
    )

    # Upstream has no data anywhere → flagged as missing.
    resolution, missing = resolve_direct_upstreams(pipeline, "tx", {}, file_mgr)
    assert missing == ["src_t"]
    assert resolution == {}

    # Load the upstream in memory → resolves, no longer missing.
    file_mgr.register_dataframe("src_t", pd.DataFrame({"x": [1]}), node_id="src", cache_key="k", into_memory=True)
    resolution2, missing2 = resolve_direct_upstreams(pipeline, "tx", {"src": "k"}, file_mgr)
    assert missing2 == []
    assert resolution2 == {"src_t": LOCATION_MEMORY}
