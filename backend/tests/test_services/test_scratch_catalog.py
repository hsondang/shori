"""In-memory (scratch catalog) load behaviors and Excel sheet listing."""

import pandas as pd
import pytest

from app.services.duckdb_manager import (
    DuckDBManager,
    LOCATION_MATERIALIZED,
    LOCATION_MEMORY,
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


def test_mode_switch_drops_other_catalog_copy(file_mgr):
    df = pd.DataFrame({"id": [1]})
    file_mgr.register_dataframe("t", df, node_id="n", cache_key="k", into_memory=True)
    assert file_mgr.table_exists("t", location=LOCATION_MEMORY) is True

    file_mgr.register_dataframe("t", df, node_id="n", cache_key="k2", into_memory=False)
    assert file_mgr.table_exists("t", location=LOCATION_MATERIALIZED) is True
    assert file_mgr.table_exists("t", location=LOCATION_MEMORY) is False


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
