import csv
import pathlib

import pandas as pd
import pytest

from app.services.duckdb_manager import DuckDBManager


def test_register_csv(duckdb_mgr, sample_csv_file):
    stats = duckdb_mgr.register_csv("t", sample_csv_file)
    assert stats["row_count"] == 5
    assert stats["column_count"] == 3
    assert "id" in stats["columns"]


def test_register_csv_drops_existing(duckdb_mgr, sample_csv_file):
    duckdb_mgr.register_csv("t", sample_csv_file)
    stats = duckdb_mgr.register_csv("t", sample_csv_file)
    assert stats["row_count"] == 5  # no duplicate rows


def test_register_csv_uses_full_file_sampling_for_mixed_type_ids(duckdb_mgr, tmp_path):
    path = tmp_path / "mixed_ids.csv"
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Lead ID", "name"])
        for index in range(20481):
            writer.writerow([1000000 + index, f"User {index}"])
        writer.writerow(["1C251002482142vKqu", "Late Mixed Type"])

    stats = duckdb_mgr.register_csv("mixed_ids", str(path))
    describe_rows = duckdb_mgr.conn.execute('DESCRIBE "mixed_ids"').fetchall()
    column_types = {name: type_name for name, type_name, *_ in describe_rows}
    last_lead_id = duckdb_mgr.conn.execute(
        'SELECT "Lead ID" FROM "mixed_ids" ORDER BY rowid DESC LIMIT 1'
    ).fetchone()[0]

    assert stats["row_count"] == 20482
    assert column_types["Lead ID"] == "VARCHAR"
    assert last_lead_id == "1C251002482142vKqu"


def test_register_dataframe(duckdb_mgr):
    df = pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    stats = duckdb_mgr.register_dataframe("df_table", df)
    assert stats["row_count"] == 3
    assert stats["column_count"] == 2
    assert "x" in stats["columns"]


def test_append_dataframe(duckdb_mgr):
    duckdb_mgr.register_dataframe("df_table", pd.DataFrame({"x": [1], "y": ["a"]}))
    stats = duckdb_mgr.append_dataframe("df_table", pd.DataFrame({"x": [2, 3], "y": ["b", "c"]}))
    rows = duckdb_mgr.conn.execute('SELECT * FROM "df_table" ORDER BY x').fetchall()

    assert stats["row_count"] == 3
    assert rows == [(1, "a"), (2, "b"), (3, "c")]


def test_execute_transform(duckdb_mgr, sample_csv_file):
    duckdb_mgr.register_csv("src", sample_csv_file)
    stats = duckdb_mgr.execute_transform("filtered", 'SELECT * FROM src WHERE id > 2')
    assert stats["row_count"] == 3


def test_execute_transform_invalid_sql(duckdb_mgr):
    with pytest.raises(Exception):
        duckdb_mgr.execute_transform("bad", "THIS IS NOT SQL")


def test_table_exists_true(duckdb_mgr, sample_csv_file):
    duckdb_mgr.register_csv("exists_table", sample_csv_file)
    assert duckdb_mgr.table_exists("exists_table") is True


def test_table_exists_false(duckdb_mgr):
    assert duckdb_mgr.table_exists("no_such_table") is False


def test_drop_table(duckdb_mgr, sample_csv_file):
    duckdb_mgr.register_csv("drop_me", sample_csv_file)
    assert duckdb_mgr.table_exists("drop_me") is True
    duckdb_mgr.drop_table("drop_me")
    assert duckdb_mgr.table_exists("drop_me") is False


def test_drop_table_nonexistent(duckdb_mgr):
    duckdb_mgr.drop_table("ghost_table")  # should not raise


def test_preview_limit_offset(duckdb_mgr, sample_csv_file):
    duckdb_mgr.register_csv("paged", sample_csv_file)
    result = duckdb_mgr.preview("paged", offset=1, limit=2)
    assert result["kind"] == "table"
    assert len(result["rows"]) == 2
    assert result["total_rows"] == 5
    assert result["offset"] == 1
    assert result["limit"] == 2


def test_preview_empty_table(duckdb_mgr):
    df = pd.DataFrame({"a": pd.Series([], dtype=int), "b": pd.Series([], dtype=str)})
    duckdb_mgr.register_dataframe("empty_t", df)
    result = duckdb_mgr.preview("empty_t")
    assert result["kind"] == "table"
    assert result["rows"] == []
    assert result["total_rows"] == 0
    assert "a" in result["columns"]


def test_export_to_csv(duckdb_mgr, sample_csv_file, tmp_path):
    duckdb_mgr.register_csv("export_src", sample_csv_file)
    out = str(tmp_path / "out.csv")
    duckdb_mgr.export_to_csv("export_src", out)
    assert pathlib.Path(out).exists()
    content = pathlib.Path(out).read_text()
    assert "Alice" in content
