"""In-memory (scratch catalog) load behaviors and Excel sheet listing."""

import json

import duckdb
import pandas as pd
import pytest

from app.services.duckdb_manager import (
    DuckDBManager,
    LOCATION_MATERIALIZED,
    LOCATION_MEMORY,
    META_TABLE,
)
from app.services.excel_service import list_sheet_names


@pytest.fixture
def file_mgr(tmp_path):
    mgr = DuckDBManager(str(tmp_path / "project.duckdb"))
    yield mgr
    mgr.close()


def test_in_memory_load_lands_in_scratch(file_mgr):
    file_mgr.register_dataframe(
        "mem_t", pd.DataFrame({"id": [1, 2]}), node_id="n", cache_key="k", into_memory=True
    )
    assert file_mgr.table_exists("mem_t", location=LOCATION_MEMORY) is True
    assert file_mgr.table_exists("mem_t", location=LOCATION_MATERIALIZED) is False
    assert file_mgr.get_node_meta("n")["location"] == LOCATION_MEMORY


def test_transform_joins_scratch_and_project_tables(file_mgr, sample_csv_file):
    # disk table (materialized) + memory table (scratch), joined by an unqualified transform.
    file_mgr.register_csv("disk_t", sample_csv_file, node_id="d", cache_key="k1", into_memory=False)
    file_mgr.register_dataframe(
        "mem_t",
        pd.DataFrame({"id": [1, 2], "tag": ["x", "y"]}),
        node_id="m",
        cache_key="k2",
        into_memory=True,
    )
    stats = file_mgr.execute_transform(
        "joined",
        "SELECT d.id, d.name, m.tag FROM disk_t d JOIN mem_t m ON d.id = m.id",
        node_id="j",
        cache_key="k3",
        into_memory=True,
    )
    assert stats["row_count"] == 2
    assert set(stats["columns"]) == {"id", "name", "tag"}


def test_load_then_materialize_coexist(file_mgr):
    df = pd.DataFrame({"id": [1]})
    file_mgr.register_dataframe("t", df, node_id="n", cache_key="k", into_memory=True)
    assert file_mgr.table_exists("t", location=LOCATION_MEMORY) is True

    # Materializing the same node no longer drops the in-memory copy — both
    # coexist, each with its own independently-keyed meta row.
    file_mgr.register_dataframe("t", df, node_id="n", cache_key="k2", into_memory=False)
    assert file_mgr.table_exists("t", location=LOCATION_MATERIALIZED) is True
    assert file_mgr.table_exists("t", location=LOCATION_MEMORY) is True

    locs = file_mgr.get_node_locations("n")
    assert set(locs) == {LOCATION_MEMORY, LOCATION_MATERIALIZED}
    assert locs[LOCATION_MEMORY]["cache_key"] == "k"
    assert locs[LOCATION_MATERIALIZED]["cache_key"] == "k2"


def test_in_memory_gone_after_reopen_but_meta_remembers(tmp_path, sample_csv_file):
    path = str(tmp_path / "project.duckdb")
    mgr = DuckDBManager(path)
    mgr.register_csv("disk_t", sample_csv_file, node_id="d", cache_key="k1", into_memory=False)
    mgr.register_dataframe(
        "mem_t", pd.DataFrame({"id": [1]}), node_id="m", cache_key="k2", into_memory=True
    )
    mgr.close()

    reopened = DuckDBManager(path)
    try:
        # Materialized survives; in-memory is gone (scratch is RAM-only).
        assert reopened.table_exists("disk_t", location=LOCATION_MATERIALIZED) is True
        assert reopened.table_exists("mem_t", location=LOCATION_MEMORY) is False
        # The meta row still remembers it WAS in-memory, so the UI derives "Idle".
        meta = reopened.get_node_meta("m")
        assert meta["location"] == LOCATION_MEMORY
        assert meta["status"] == "complete"
    finally:
        reopened.close()


def test_opening_a_legacy_single_location_project_migrates_meta_in_place(tmp_path, sample_csv_file):
    """A project file saved before the per-location schema (node_id-only PK, no
    `location` column) must migrate cleanly on the next open: existing rows
    survive, get backfilled to `materialized` (the pre-in-memory default), and
    the real data table stays intact and queryable."""
    path = str(tmp_path / "project.duckdb")

    # Build a real project the normal way, then downgrade its meta table to the
    # legacy shape on a raw connection (no location column, single-column PK) —
    # this is exactly what a project saved before this feature looks like.
    mgr = DuckDBManager(path)
    mgr.register_csv("legacy_t", sample_csv_file, node_id="legacy-node", cache_key="k-old", into_memory=False)
    original_meta = mgr.get_node_meta("legacy-node", LOCATION_MATERIALIZED)
    mgr.close()

    raw = duckdb.connect(path)
    try:
        raw.execute(f'DROP TABLE "{META_TABLE}"')
        raw.execute(f"""
            CREATE TABLE "{META_TABLE}" (
                node_id VARCHAR PRIMARY KEY,
                table_name VARCHAR NOT NULL,
                cache_key VARCHAR,
                status VARCHAR NOT NULL,
                row_count BIGINT,
                column_count INTEGER,
                columns_json VARCHAR,
                error VARCHAR,
                started_at VARCHAR,
                finished_at VARCHAR,
                duration_ms DOUBLE
            )
        """)
        raw.execute(
            f'INSERT INTO "{META_TABLE}" '
            "(node_id, table_name, cache_key, status, row_count, column_count, columns_json, started_at, finished_at, duration_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                "legacy-node", "legacy_t", "k-old", "complete",
                original_meta["row_count"], original_meta["column_count"], json.dumps(original_meta["columns"]),
                original_meta["started_at"], original_meta["finished_at"], original_meta["duration_ms"],
            ],
        )
        # Sanity check: this really is the old shape (no location column, single-column PK).
        pk = raw.execute(
            "SELECT constraint_column_names FROM duckdb_constraints() "
            "WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'",
            [META_TABLE],
        ).fetchone()
        assert pk[0] == ["node_id"]
    finally:
        raw.close()

    # Reopening through the real manager must trigger the migration path.
    migrated = DuckDBManager(path)
    try:
        assert migrated.table_exists("legacy_t", location=LOCATION_MATERIALIZED) is True
        locations = migrated.get_node_locations("legacy-node")
        assert set(locations) == {LOCATION_MATERIALIZED}
        meta = locations[LOCATION_MATERIALIZED]
        assert meta["cache_key"] == "k-old"
        assert meta["row_count"] == original_meta["row_count"]
        assert meta["status"] == "complete"

        # Migration is idempotent: opening it again must not error or duplicate rows.
        migrated.close()
        reopened_again = DuckDBManager(path)
        try:
            assert set(reopened_again.get_node_locations("legacy-node")) == {LOCATION_MATERIALIZED}
        finally:
            reopened_again.close()
    finally:
        if not migrated._closed:
            migrated.close()


def test_consumable_location_precedence(file_mgr):
    # A node with both copies: materialized built at k_old, in-memory built at k_new.
    file_mgr.register_dataframe("u", pd.DataFrame({"v": ["disk"]}), node_id="u", cache_key="k_old", into_memory=False)
    file_mgr.register_dataframe("u", pd.DataFrame({"v": ["mem"]}), node_id="u", cache_key="k_new", into_memory=True)

    # Current key = k_new → in-memory copy is fresh, materialized is stale → memory wins.
    assert file_mgr.consumable_location("u", "k_new") == LOCATION_MEMORY
    # Current key = k_old → materialized is fresh, in-memory is stale → fresh disk beats stale mem.
    assert file_mgr.consumable_location("u", "k_old") == LOCATION_MATERIALIZED
    # No key context → neither is "fresh"; precedence falls to in_memory over materialized.
    assert file_mgr.consumable_location("u", None) == LOCATION_MEMORY
    # Unknown node → nothing consumable.
    assert file_mgr.consumable_location("missing", "k") is None


def test_transform_reads_resolved_upstream_copy(file_mgr, tmp_path):
    # Upstream "u" has a STALE materialized (disk) copy and a FRESH in-memory copy
    # holding different values, so we can tell which one a transform actually read.
    file_mgr.register_dataframe("u", pd.DataFrame({"v": ["disk"]}), node_id="u", cache_key="k_old", into_memory=False)
    file_mgr.register_dataframe("u", pd.DataFrame({"v": ["mem"]}), node_id="u", cache_key="k_new", into_memory=True)

    file_mgr.execute_transform("t_default", "SELECT v FROM u", node_id="td", cache_key="c1", into_memory=True)
    file_mgr.execute_transform(
        "t_resolved", "SELECT v FROM u", node_id="tr", cache_key="c2", into_memory=True,
        upstream_resolution={"u": LOCATION_MEMORY},
    )

    default_out = tmp_path / "d.csv"
    resolved_out = tmp_path / "r.csv"
    file_mgr.copy_table_to("t_default", str(default_out), "csv")
    file_mgr.copy_table_to("t_resolved", str(resolved_out), "csv")

    # Default search path is project(disk)-first, so a plain transform reads the disk copy.
    assert "disk" in default_out.read_text()
    # The run-scoped view pins the read to the precedence-chosen (fresh in-memory) copy.
    resolved_text = resolved_out.read_text()
    assert "mem" in resolved_text and "disk" not in resolved_text


def test_copy_table_to_exports_from_scratch(file_mgr, tmp_path):
    file_mgr.register_dataframe(
        "mem_t", pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}), node_id="n", cache_key="k", into_memory=True
    )
    out = tmp_path / "out.csv"
    file_mgr.copy_table_to("mem_t", str(out), "csv")
    assert out.exists()
    assert out.read_text().splitlines()[0] == "a,b"


def test_list_sheet_names_reads_workbook(sample_excel_file):
    assert list_sheet_names(sample_excel_file) == ["Orders", "Summary"]


def test_list_sheet_names_rejects_unsupported_extension(tmp_path):
    legacy = tmp_path / "old.xls"
    legacy.write_bytes(b"not really an xls")
    with pytest.raises(ValueError, match="Unsupported"):
        list_sheet_names(str(legacy))
