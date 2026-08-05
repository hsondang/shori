"""Appending a DuckDB query's rows to an external table.

The Oracle round trip is faked with a stub cursor: what matters here is the
plan (which source column lands in which target column, and what is refused),
the generated SQL and binds, the batching, and the transaction semantics.
"""

import pandas as pd
import pytest

from app.models.pipeline import NodeDefinition, NodeType, Position
from app.services.database_export import (
    BATCH_ROWS,
    ColumnPlan,
    DatabaseExportError,
    ExportAborted,
    OracleTableWriter,
    TargetColumn,
    append_query_to_table_sync,
    build_export_plan,
    effective_export_sql,
    parse_target_table,
    validate_export_sync,
)
from app.services.duckdb_manager import DuckDBManager, LOCATION_MEMORY


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class StubCursor:
    def __init__(self, connection):
        self._conn = connection
        self.closed = False

    def execute(self, sql, **kwargs):
        self._conn.executed.append((sql, kwargs))

    def fetchall(self):
        return self._conn.describe_rows

    def setinputsizes(self, *sizes):
        self._conn.input_sizes = sizes

    def executemany(self, sql, rows):
        if self._conn.fail_on_batch is not None and len(self._conn.batches) == self._conn.fail_on_batch:
            raise RuntimeError("ORA-12345: simulated failure")
        self._conn.batches.append((sql, list(rows)))

    def close(self):
        self.closed = True


class StubOracleConnection:
    """Enough of an oracledb connection for the writer to run against."""

    def __init__(self, target_columns=(), fail_on_batch=None):
        self.describe_rows = [
            (c.name, c.data_type, "Y" if c.nullable else "N", "d" if c.has_default else None,
             c.char_length, c.data_length, c.precision, c.scale)
            for c in target_columns
        ]
        self.executed: list = []
        self.batches: list = []
        self.input_sizes: tuple = ()
        self.committed = False
        self.rolled_back = False
        self.fail_on_batch = fail_on_batch

    def cursor(self):
        return StubCursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def _col(name, data_type="VARCHAR2", *, nullable=True, has_default=False, char_length=100, **kw):
    return TargetColumn(
        name=name,
        data_type=data_type,
        nullable=nullable,
        has_default=has_default,
        char_length=char_length,
        **kw,
    )


@pytest.fixture
def mgr(tmp_path):
    manager = DuckDBManager(str(tmp_path / "project.duckdb"))
    yield manager
    manager.close()


# ---------------------------------------------------------------------------
# Target parsing
# ---------------------------------------------------------------------------


def test_parse_target_table_uppercases_both_parts():
    assert parse_target_table("sales.orders") == ("SALES", "ORDERS")
    assert parse_target_table("  SALES.ORDERS  ") == ("SALES", "ORDERS")


@pytest.mark.parametrize(
    "value",
    ["", "orders", "a.b.c", "sales.", ".orders", "sales.1orders", "sales.or ders", "sa;les.orders"],
)
def test_parse_target_table_rejects_malformed(value):
    with pytest.raises(DatabaseExportError):
        parse_target_table(value)


# ---------------------------------------------------------------------------
# Column planning
# ---------------------------------------------------------------------------


def test_plan_matches_columns_case_insensitively():
    plan = build_export_plan(
        ["id", "Name"],
        ["INTEGER", "VARCHAR"],
        [_col("ID", "NUMBER", char_length=None, precision=10), _col("NAME")],
    )
    assert plan.ok
    assert [(p.source_column, p.target.name) for p in plan.plans] == [("id", "ID"), ("Name", "NAME")]
    assert all(check.status == "ok" for check in plan.columns)


def test_plan_rejects_source_column_with_no_target():
    plan = build_export_plan(["id", "ghost"], ["INTEGER", "VARCHAR"], [_col("ID", "NUMBER")])
    assert not plan.ok
    assert any("ghost" in error for error in plan.errors)
    ghost = next(c for c in plan.columns if c.source_column == "ghost")
    assert ghost.status == "missing_in_target"


def test_plan_rejects_unsupplied_not_null_column_without_default():
    plan = build_export_plan(
        ["id"],
        ["INTEGER"],
        [_col("ID", "NUMBER"), _col("CREATED_BY", nullable=False, has_default=False)],
    )
    assert not plan.ok
    assert any("CREATED_BY" in error for error in plan.errors)


def test_plan_allows_unsupplied_column_with_a_default():
    plan = build_export_plan(
        ["id"],
        ["INTEGER"],
        [_col("ID", "NUMBER"), _col("CREATED_AT", "DATE", nullable=False, has_default=True)],
    )
    assert plan.ok
    assert plan.unmapped_target_columns == ["CREATED_AT"]


def test_plan_allows_unsupplied_nullable_column():
    plan = build_export_plan(["id"], ["INTEGER"], [_col("ID", "NUMBER"), _col("NOTE")])
    assert plan.ok
    assert plan.unmapped_target_columns == ["NOTE"]


@pytest.mark.parametrize("duckdb_type", ["INTERVAL", "TIME", "INTEGER[]", "STRUCT(k INTEGER)", "MAP(VARCHAR, INTEGER)"])
def test_plan_rejects_types_with_no_oracle_equivalent(duckdb_type):
    plan = build_export_plan(["value"], [duckdb_type], [_col("VALUE")])
    assert not plan.ok
    assert any("no Oracle equivalent" in error for error in plan.errors)


def test_plan_accepts_timestamp_despite_the_time_prefix():
    plan = build_export_plan(["ts"], ["TIMESTAMP"], [_col("TS", "TIMESTAMP(6)", char_length=None)])
    assert plan.ok


def test_plan_warns_on_risky_conversions_without_blocking():
    plan = build_export_plan(["amount"], ["VARCHAR"], [_col("AMOUNT", "NUMBER", char_length=None)])
    assert plan.ok
    assert plan.warnings
    assert plan.columns[0].status == "type_warning"


def test_plan_rejects_duplicate_source_columns():
    plan = build_export_plan(["id", "ID"], ["INTEGER", "INTEGER"], [_col("ID", "NUMBER")])
    assert not plan.ok
    assert any("duplicate" in error.lower() for error in plan.errors)


# ---------------------------------------------------------------------------
# Oracle writer
# ---------------------------------------------------------------------------


def _plans(*pairs):
    return [
        ColumnPlan(source_index=i, source_column=src, source_type=typ, target=target)
        for i, (src, typ, target) in enumerate(pairs)
    ]


def test_insert_sql_quotes_identifiers_and_uses_positional_binds():
    writer = OracleTableWriter()
    sql = writer.insert_sql("SALES", "ORDERS", _plans(("id", "INTEGER", _col("ID", "NUMBER"))))
    assert sql == 'INSERT INTO "SALES"."ORDERS" ("ID") VALUES (:1)'


def test_input_sizes_come_from_declared_target_types():
    writer = OracleTableWriter()
    sizes = writer.input_sizes(
        _plans(
            ("name", "VARCHAR", _col("NAME", "VARCHAR2", char_length=64)),
            ("amount", "DECIMAL(10,2)", _col("AMOUNT", "NUMBER", char_length=None)),
        )
    )
    # A narrow or all-NULL first batch must not be what sizes the binds.
    assert sizes[0] == 64
    assert sizes[1] is not None


def test_append_rows_batches_and_commits_once():
    connection = StubOracleConnection()
    writer = OracleTableWriter()
    plans = _plans(("id", "INTEGER", _col("ID", "NUMBER", char_length=None)))
    batches = [[(1,), (2,)], [(3,)]]

    written = writer.append_rows(connection, "S", "T", plans, batches)

    assert written == 3
    assert [rows for _, rows in connection.batches] == [[(1,), (2,)], [(3,)]]
    assert connection.committed is True
    assert connection.rolled_back is False


def test_append_rows_rolls_back_and_does_not_commit_on_failure():
    connection = StubOracleConnection(fail_on_batch=1)
    writer = OracleTableWriter()
    plans = _plans(("id", "INTEGER", _col("ID", "NUMBER", char_length=None)))

    with pytest.raises(RuntimeError):
        writer.append_rows(connection, "S", "T", plans, [[(1,)], [(2,)]])

    assert connection.committed is False
    assert connection.rolled_back is True


def test_append_rows_aborts_between_batches_and_rolls_back():
    connection = StubOracleConnection()
    writer = OracleTableWriter()
    plans = _plans(("id", "INTEGER", _col("ID", "NUMBER", char_length=None)))
    seen: list[int] = []

    with pytest.raises(ExportAborted):
        writer.append_rows(
            connection,
            "S",
            "T",
            plans,
            [[(1,)], [(2,)]],
            on_progress=seen.append,
            should_abort=lambda: len(seen) >= 1,
        )

    assert connection.committed is False
    assert connection.rolled_back is True


def test_append_rows_converts_booleans_to_numbers():
    connection = StubOracleConnection()
    writer = OracleTableWriter()
    plans = _plans(("flag", "BOOLEAN", _col("FLAG", "NUMBER", char_length=None)))

    writer.append_rows(connection, "S", "T", plans, [[(True,), (False,), (None,)]])

    assert connection.batches[0][1] == [(1,), (0,), (None,)]


def test_describe_target_reads_the_data_dictionary():
    connection = StubOracleConnection(target_columns=[_col("ID", "NUMBER", nullable=False)])
    columns = OracleTableWriter().describe_target(connection, "SALES", "ORDERS")

    assert [c.name for c in columns] == ["ID"]
    assert columns[0].nullable is False
    sql, params = connection.executed[0]
    assert "all_tab_columns" in sql
    assert params == {"owner": "SALES", "table_name": "ORDERS"}


# ---------------------------------------------------------------------------
# End-to-end against a real DuckDB
# ---------------------------------------------------------------------------


def test_append_query_streams_duckdb_rows_into_the_target(mgr):
    mgr.register_dataframe(
        "src", pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]}),
        node_id="s", cache_key="k", into_memory=True,
    )
    connection = StubOracleConnection(
        target_columns=[_col("ID", "NUMBER", char_length=None), _col("NAME")]
    )

    result = append_query_to_table_sync(
        mgr, connection, "SELECT id, name FROM src ORDER BY id", {"src": LOCATION_MEMORY}, "S", "T",
    )

    assert result["row_count"] == 3
    assert result["columns"] == ["ID", "NAME"]
    assert connection.batches[0][1] == [(1, "a"), (2, "b"), (3, "c")]
    assert connection.committed is True


def test_append_query_reads_the_resolved_copy_not_the_search_path(mgr):
    # A stale materialized copy and a fresh in-memory one under the same name:
    # exporting the wrong one would silently push stale rows to a live table.
    mgr.register_dataframe("u", pd.DataFrame({"v": ["disk"]}), node_id="u", cache_key="old", into_memory=False)
    mgr.register_dataframe("u", pd.DataFrame({"v": ["mem"]}), node_id="u", cache_key="new", into_memory=True)
    connection = StubOracleConnection(target_columns=[_col("V")])

    append_query_to_table_sync(
        mgr, connection, "SELECT v FROM u", {"u": LOCATION_MEMORY}, "S", "T"
    )

    assert connection.batches[0][1] == [("mem",)]


def test_append_query_refuses_a_missing_target_table(mgr):
    mgr.register_dataframe("src", pd.DataFrame({"id": [1]}), node_id="s", cache_key="k", into_memory=True)
    connection = StubOracleConnection(target_columns=[])

    with pytest.raises(DatabaseExportError, match="not found"):
        append_query_to_table_sync(mgr, connection, "SELECT id FROM src", {"src": LOCATION_MEMORY}, "S", "T")

    assert connection.batches == []


def test_append_query_refuses_a_mismatched_column_before_writing(mgr):
    mgr.register_dataframe("src", pd.DataFrame({"ghost": [1]}), node_id="s", cache_key="k", into_memory=True)
    connection = StubOracleConnection(target_columns=[_col("ID", "NUMBER", char_length=None)])

    with pytest.raises(DatabaseExportError, match="ghost"):
        append_query_to_table_sync(mgr, connection, "SELECT ghost FROM src", {"src": LOCATION_MEMORY}, "S", "T")

    assert connection.batches == []
    assert connection.committed is False


def test_append_query_splits_large_results_into_batches(mgr):
    rows = BATCH_ROWS + 7
    mgr.register_dataframe(
        "big", pd.DataFrame({"id": range(rows)}), node_id="b", cache_key="k", into_memory=True
    )
    connection = StubOracleConnection(target_columns=[_col("ID", "NUMBER", char_length=None)])
    progress: list[int] = []

    result = append_query_to_table_sync(
        mgr, connection, "SELECT id FROM big", {"big": LOCATION_MEMORY}, "S", "T",
        on_progress=progress.append,
    )

    assert result["row_count"] == rows
    assert len(connection.batches) == 2
    assert progress == [BATCH_ROWS, rows]


def test_validate_export_reports_mismatches_without_writing(mgr):
    mgr.register_dataframe(
        "src", pd.DataFrame({"id": [1], "ghost": ["x"]}), node_id="s", cache_key="k", into_memory=True
    )
    connection = StubOracleConnection(target_columns=[_col("ID", "NUMBER", char_length=None)])

    report = validate_export_sync(
        mgr, connection, "SELECT id, ghost FROM src", {"src": LOCATION_MEMORY}, "S", "T"
    )

    assert report["ok"] is False
    assert report["target_exists"] is True
    assert any(c["status"] == "missing_in_target" for c in report["columns"])
    assert connection.batches == []


def test_validate_export_flags_a_missing_table(mgr):
    connection = StubOracleConnection(target_columns=[])
    report = validate_export_sync(mgr, connection, "SELECT 1 AS id", {}, "S", "T")

    assert report["target_exists"] is False
    assert report["ok"] is False


# ---------------------------------------------------------------------------
# Effective SQL
# ---------------------------------------------------------------------------


def _export_node(config: dict) -> NodeDefinition:
    return NodeDefinition(
        id="e", type=NodeType.EXPORT, table_name="e", label="Export",
        position=Position(x=0, y=0), config=config,
    )


def test_effective_sql_defaults_to_the_whole_upstream_table():
    node = _export_node({"destination": "database"})
    assert effective_export_sql(node, "customers") == 'SELECT * FROM "customers"'


def test_effective_sql_uses_the_node_query_when_the_toggle_is_on():
    node = _export_node({"destination": "database", "use_sql": True, "sql": "SELECT 1"})
    assert effective_export_sql(node, "customers") == "SELECT 1"


def test_effective_sql_rejects_an_empty_query_with_the_toggle_on():
    node = _export_node({"destination": "database", "use_sql": True, "sql": "   "})
    with pytest.raises(DatabaseExportError):
        effective_export_sql(node, "customers")


def test_effective_sql_rejects_a_disconnected_node():
    node = _export_node({"destination": "database"})
    with pytest.raises(DatabaseExportError, match="Connect a source"):
        effective_export_sql(node, None)
