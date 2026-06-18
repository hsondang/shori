import csv
import pathlib

import pandas as pd
import pytest
from starlette.responses import JSONResponse

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


def test_preview_returns_json_safe_non_finite_float_values(duckdb_mgr):
    duckdb_mgr.execute_transform(
        "non_finite_values",
        "SELECT 'NaN'::DOUBLE AS score, 'Infinity'::DOUBLE AS high, '-Infinity'::DOUBLE AS low",
    )

    result = duckdb_mgr.preview("non_finite_values")

    assert result["rows"] == [["NaN", "Infinity", "-Infinity"]]
    assert JSONResponse(result).status_code == 200


def test_preview_falls_back_to_text_for_values_duckdb_cannot_fetch_as_python(duckdb_mgr):
    duckdb_mgr.execute_transform(
        "zoned_times",
        "SELECT TIMESTAMPTZ '2026-01-02 03:04:05+07' AS occurred_at",
    )

    result = duckdb_mgr.preview("zoned_times")

    assert result["column_types"] == ["TIMESTAMP WITH TIME ZONE"]
    assert isinstance(result["rows"][0][0], str)


def test_export_to_csv(duckdb_mgr, sample_csv_file, tmp_path):
    duckdb_mgr.register_csv("export_src", sample_csv_file)
    out = str(tmp_path / "out.csv")
    duckdb_mgr.export_to_csv("export_src", out)
    assert pathlib.Path(out).exists()
    content = pathlib.Path(out).read_text()
    assert "Alice" in content


# --- Staging loads, metadata, and recovery ---

def test_begin_load_appends_arrow_batches_and_swaps(duckdb_mgr):
    import pyarrow as pa

    load = duckdb_mgr.begin_load("node-arrow", "arrow_t", "key-1")
    load.mark_loading()
    load.append(pa.table({"id": [1, 2], "name": ["a", "b"]}))
    load.append(pa.table({"id": [3], "name": ["c"]}))
    stats = load.commit()

    assert stats["row_count"] == 3
    assert duckdb_mgr.table_exists("arrow_t")
    assert not duckdb_mgr.table_exists("arrow_t__staging")
    meta = duckdb_mgr.get_node_meta("node-arrow")
    assert meta["status"] == "complete"
    assert meta["cache_key"] == "key-1"
    assert meta["row_count"] == 3
    assert meta["columns"] == ["id", "name"]


def test_aborted_load_keeps_previous_table_version(duckdb_mgr):
    import pandas as pd

    duckdb_mgr.register_dataframe("versioned_t", pd.DataFrame({"x": [1]}), node_id="n1", cache_key="k1")

    load = duckdb_mgr.begin_load("n1", "versioned_t", "k2")
    load.mark_loading()
    load.append(pd.DataFrame({"x": [99, 100]}))
    load.abort("source exploded")

    # Old data still intact and queryable; metadata records the failure.
    assert duckdb_mgr.table_stats("versioned_t")["row_count"] == 1
    assert not duckdb_mgr.table_exists("versioned_t__staging")
    meta = duckdb_mgr.get_node_meta("n1")
    assert meta["status"] == "failed"
    assert "exploded" in meta["error"]


def test_commit_without_data_raises(duckdb_mgr):
    load = duckdb_mgr.begin_load("n1", "never_t", None)
    with pytest.raises(RuntimeError, match="no data"):
        load.commit()
    load.abort()


def test_recovery_drops_staging_and_fails_loading_meta(tmp_path):
    import pandas as pd
    from app.services.duckdb_manager import DuckDBManager

    db_path = tmp_path / "recover.duckdb"
    mgr = DuckDBManager(db_path)
    mgr.register_dataframe("good_t", pd.DataFrame({"x": [1]}), node_id="good", cache_key="k")
    # Simulate a crash mid-load: staging exists, meta stuck in 'loading'.
    load = mgr.begin_load("crashed", "crash_t", "k2")
    load.mark_loading()
    load.append(pd.DataFrame({"x": [1, 2, 3]}))
    mgr.close()  # without commit/abort

    reopened = DuckDBManager(db_path)
    assert reopened.table_exists("good_t")
    assert not reopened.table_exists("crash_t")
    assert not reopened.table_exists("crash_t__staging")
    crashed_meta = reopened.get_node_meta("crashed")
    assert crashed_meta["status"] == "failed"
    assert "interrupted" in crashed_meta["error"].lower()
    good_meta = reopened.get_node_meta("good")
    assert good_meta["status"] == "complete"
    reopened.close()


def test_persistence_across_reopen(tmp_path, sample_csv_file):
    from app.services.duckdb_manager import DuckDBManager

    db_path = tmp_path / "persist.duckdb"
    mgr = DuckDBManager(db_path)
    mgr.register_csv("persisted_t", sample_csv_file, node_id="n1", cache_key="key-a")
    mgr.close()

    reopened = DuckDBManager(db_path)
    assert reopened.table_stats("persisted_t")["row_count"] == 5
    assert reopened.get_node_meta("n1")["cache_key"] == "key-a"
    reopened.close()


def test_drop_node_removes_table_and_meta(duckdb_mgr, sample_csv_file):
    duckdb_mgr.register_csv("dropme_t", sample_csv_file, node_id="n1", cache_key="k")
    assert duckdb_mgr.drop_node("n1") is True
    assert not duckdb_mgr.table_exists("dropme_t")
    assert duckdb_mgr.get_node_meta("n1") is None
    assert duckdb_mgr.drop_node("n1") is False


def test_rename_node_table_preserves_data_and_meta(duckdb_mgr, sample_csv_file):
    duckdb_mgr.register_csv("old_name_t", sample_csv_file, node_id="n1", cache_key="k")
    assert duckdb_mgr.rename_node_table("n1", "new_name_t") is True
    assert not duckdb_mgr.table_exists("old_name_t")
    assert duckdb_mgr.table_stats("new_name_t")["row_count"] == 5
    meta = duckdb_mgr.get_node_meta("n1")
    assert meta["table_name"] == "new_name_t"
    assert meta["cache_key"] == "k"


def test_reserved_table_names_rejected(duckdb_mgr):
    import pandas as pd

    with pytest.raises(ValueError, match="reserved"):
        duckdb_mgr.register_dataframe("_shori_sneaky", pd.DataFrame({"x": [1]}))
    with pytest.raises(ValueError, match="reserved"):
        duckdb_mgr.register_dataframe("sneaky__staging", pd.DataFrame({"x": [1]}))
    with pytest.raises(ValueError, match="internal"):
        duckdb_mgr.drop_table("_shori_node_meta")


def test_concurrent_loads_to_different_tables(duckdb_mgr):
    import threading
    import pandas as pd

    errors = []

    def load_table(name, count):
        try:
            duckdb_mgr.register_dataframe(
                name,
                pd.DataFrame({"v": list(range(count))}),
                node_id=f"node_{name}",
                cache_key="k",
            )
        except Exception as exc:  # pragma: no cover - failure detail
            errors.append(exc)

    threads = [
        threading.Thread(target=load_table, args=(f"conc_t{i}", 100 + i))
        for i in range(6)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    for i in range(6):
        assert duckdb_mgr.table_stats(f"conc_t{i}")["row_count"] == 100 + i


def test_compact_shrinks_file_after_drop(tmp_path):
    import pandas as pd
    from app.services.duckdb_manager import DuckDBManager

    db_path = tmp_path / "compact.duckdb"
    mgr = DuckDBManager(db_path)
    big = pd.DataFrame({"v": list(range(200_000)), "s": ["padding-" * 4] * 200_000})
    mgr.register_dataframe("big_t", big, node_id="big", cache_key="k")
    mgr.register_dataframe("small_t", pd.DataFrame({"x": [1]}), node_id="small", cache_key="k")
    mgr.checkpoint()
    size_before = mgr.storage_info()["file_size_bytes"]

    mgr.drop_node("big")
    info = mgr.compact()

    assert info["file_size_bytes"] < size_before
    assert mgr.table_stats("small_t")["row_count"] == 1
    assert mgr.get_node_meta("small")["status"] == "complete"
    mgr.close()
