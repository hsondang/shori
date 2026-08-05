"""Append a pipeline result to a table in an external database.

Reading is universal — stream batches out of DuckDB — but writing is not: bind
placeholder style (``:1`` vs ``$1``), identifier case-folding (Oracle
uppercases, Postgres lowercases) and DDL/truncate syntax all differ per engine.
So the engine-specific half sits behind ``DatabaseExportWriter`` while the
streaming driver above it stays shared. Oracle is the only implementation
today; Postgres can either add one or take DuckDB's ATTACH fast path without
touching the node config, the API, or the UI.

Semantics are deliberately narrow: **append only**. No CREATE, no DROP, no
DELETE, no TRUNCATE — the target table and its grants, indexes and constraints
belong to whoever owns the database. The whole append runs in one transaction
and commits once, so a failure or an abort leaves the target exactly as it was
rather than partially loaded.
"""

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Protocol

import oracledb

from app.models.pipeline import NodeDefinition
from app.services.connection_resolution import saved_connection_to_config
from app.storage.pipeline_store import PipelineStore

logger = logging.getLogger(__name__)

# Matches the reference loader's batch size (replicate_table.copy_rows), which
# is both the DuckDB fetchmany chunk and the Oracle executemany array size.
BATCH_ROWS = 1000

# Unquoted Oracle identifier: letter, then letters/digits/_/$/#, max 30 chars on
# older releases and 128 on 12.2+. Quoted identifiers are not accepted — the
# target box is a plain SCHEMA.TABLE box, and allowing quotes there would make
# the case-folding rules invisible to the user.
_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")


class DatabaseExportError(ValueError):
    """Anything the user can fix: bad target, missing column, no permission."""


class ExportAborted(Exception):
    """The user aborted the run; the transaction is rolled back."""


@dataclass(frozen=True)
class TargetColumn:
    """One column of the destination table, as the database describes it."""

    name: str
    data_type: str
    nullable: bool
    has_default: bool
    char_length: int | None = None
    data_length: int | None = None
    precision: int | None = None
    scale: int | None = None


@dataclass(frozen=True)
class ColumnPlan:
    """A source column bound to the target column it will be written into."""

    source_index: int
    source_column: str
    source_type: str
    target: TargetColumn


@dataclass
class ColumnCheck:
    source_column: str
    source_type: str
    target_column: str | None
    target_type: str | None
    status: str  # "ok" | "type_warning" | "missing_in_target"
    message: str | None = None


@dataclass
class ExportPlan:
    """The result of matching a query's columns against a target table."""

    columns: list[ColumnCheck]
    plans: list[ColumnPlan]
    unmapped_target_columns: list[str]
    errors: list[str]
    warnings: list[str]

    @property
    def ok(self) -> bool:
        return not self.errors


class DatabaseExportWriter(Protocol):
    """The per-engine half of an export."""

    def describe_target(self, connection, schema: str, table: str) -> list[TargetColumn]:
        ...

    def append_rows(
        self,
        connection,
        schema: str,
        table: str,
        plans: list[ColumnPlan],
        batches: Iterable[list[tuple]],
        *,
        on_progress: Callable[[int], None] | None = None,
        should_abort: Callable[[], bool] | None = None,
    ) -> int:
        ...


# ----------------------------------------------------------------------------
# Target identification
# ----------------------------------------------------------------------------


def parse_target_table(raw: str | None) -> tuple[str, str]:
    """Split "SCHEMA.TABLE" into its two unquoted Oracle identifiers.

    Both are upper-cased, because that is what Oracle stores for an identifier
    written without quotes — matching what the user would get typing the same
    name into SQL*Plus.
    """
    value = (raw or "").strip()
    if not value:
        raise DatabaseExportError("A target table is required, in the form SCHEMA.TABLE_NAME")

    parts = value.split(".")
    if len(parts) != 2:
        raise DatabaseExportError(
            f"Target table '{value}' must be in the form SCHEMA.TABLE_NAME"
        )

    schema, table = (part.strip() for part in parts)
    for part, label in ((schema, "Schema"), (table, "Table")):
        if not _IDENTIFIER.match(part):
            raise DatabaseExportError(
                f"{label} name '{part}' is not a valid Oracle identifier "
                "(letters, digits, _, $ and # only, starting with a letter)"
            )
    return schema.upper(), table.upper()


def resolve_export_connection(connection_source_id: str | None, store: PipelineStore):
    """Load the destination connection and enforce the export permission.

    This is the actual gate. The destination dropdown filters client-side for
    UX, but a node config is just persisted JSON — a stale or hand-edited one
    must not be able to write to a database whose permission was never granted
    or has since been revoked.
    """
    if not connection_source_id:
        raise DatabaseExportError("This export node has no database connection selected")

    try:
        connection = store.load_global_connection(connection_source_id)
    except FileNotFoundError as exc:
        raise DatabaseExportError("The selected database connection no longer exists") from exc

    if connection.db_type != "oracle":
        raise DatabaseExportError(
            f"Exporting to {connection.db_type} databases is not supported yet"
        )
    if not getattr(connection, "allow_export", False):
        raise DatabaseExportError(
            f'Exports to "{connection.name}" are not enabled. Turn on "Allow exports" for this '
            "connection in Platform Settings first."
        )
    return connection


def export_connection_config(connection) -> dict:
    return saved_connection_to_config(connection)


def effective_export_sql(node: NodeDefinition, upstream_table: str | None) -> str:
    """The SQL an export node reads: its own when the SQL toggle is on, else
    the whole upstream table."""
    config = node.config or {}
    if config.get("use_sql"):
        sql = str(config.get("sql") or "").strip()
        if not sql:
            raise DatabaseExportError("The SQL query is empty. Turn the toggle off to export the whole table.")
        return sql
    if not upstream_table:
        raise DatabaseExportError("Connect a source to this export node first")
    from app.services.duckdb_manager import _quote_identifier

    return f"SELECT * FROM {_quote_identifier(upstream_table)}"


# ----------------------------------------------------------------------------
# Type classification
# ----------------------------------------------------------------------------

# DuckDB type names with no sensible Oracle bind. Checked by prefix/substring
# because parameterized names carry their arguments (INTEGER[], STRUCT(...)).
_UNSUPPORTED_SOURCE_TYPES = ("INTERVAL", "TIME", "STRUCT", "MAP", "UNION")


def _source_kind(duckdb_type: str) -> str:
    t = duckdb_type.upper()
    if t.endswith("[]") or t.startswith(("LIST", "ARRAY")):
        return "unsupported"
    # TIMESTAMP must win over the TIME prefix check below.
    if t.startswith("TIMESTAMP"):
        return "timestamp"
    for unsupported in _UNSUPPORTED_SOURCE_TYPES:
        if t.startswith(unsupported):
            return "unsupported"
    if t in ("VARCHAR", "CHAR", "BPCHAR", "TEXT", "STRING", "UUID", "JSON"):
        return "text"
    if t == "BOOLEAN":
        return "bool"
    if t == "DATE":
        return "date"
    if t in ("BLOB", "BYTEA", "BINARY", "VARBINARY"):
        return "binary"
    if t.startswith("DECIMAL") or t.startswith("NUMERIC") or t in (
        "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
        "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
        "FLOAT", "DOUBLE", "REAL",
    ):
        return "number"
    return "other"


def _target_kind(oracle_type: str) -> str:
    t = oracle_type.upper()
    if t.startswith("TIMESTAMP"):
        return "timestamp"
    if t in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "VARCHAR", "LONG"):
        return "text"
    if t in ("CLOB", "NCLOB"):
        return "text"
    if t in ("NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE", "INTEGER"):
        return "number"
    if t == "DATE":
        return "date"
    if t in ("BLOB", "RAW", "LONG RAW"):
        return "binary"
    if t == "BOOLEAN":
        return "bool"
    return "other"


# Pairs worth flagging: the write may still succeed via Oracle implicit
# conversion, but it depends on NLS settings or value content, so the user
# should know before running it. Everything else is either a clean match or
# an outright error.
_RISKY_PAIRS = {
    ("text", "number"): "text will be converted to a number by Oracle; non-numeric values will fail",
    ("text", "date"): "text will be parsed as a date using the session NLS format",
    ("text", "timestamp"): "text will be parsed as a timestamp using the session NLS format",
    ("number", "text"): "numbers will be converted to text by Oracle",
    ("number", "date"): "numbers cannot be converted to a date",
    ("number", "timestamp"): "numbers cannot be converted to a timestamp",
    ("date", "text"): "dates will be converted to text using the session NLS format",
    ("timestamp", "text"): "timestamps will be converted to text using the session NLS format",
    ("bool", "text"): "booleans will be written as the text '1'/'0'",
    ("binary", "text"): "binary data will not survive a text column",
    ("date", "number"): "dates cannot be converted to a number",
    ("timestamp", "number"): "timestamps cannot be converted to a number",
}


def build_export_plan(
    source_columns: list[str],
    source_types: list[str],
    target_columns: list[TargetColumn],
) -> ExportPlan:
    """Match query columns to target columns and collect problems.

    Matching is case-insensitive because Oracle stores unquoted identifiers
    upper-cased while DuckDB preserves whatever the query produced.
    """
    by_name = {column.name.upper(): column for column in target_columns}
    checks: list[ColumnCheck] = []
    plans: list[ColumnPlan] = []
    errors: list[str] = []
    warnings: list[str] = []
    matched: set[str] = set()

    for index, (name, source_type) in enumerate(zip(source_columns, source_types)):
        target = by_name.get(name.upper())
        if target is None:
            checks.append(
                ColumnCheck(
                    source_column=name,
                    source_type=source_type,
                    target_column=None,
                    target_type=None,
                    status="missing_in_target",
                    message="no column with this name in the target table",
                )
            )
            errors.append(f'Column "{name}" does not exist in the target table')
            continue

        matched.add(target.name.upper())
        source_kind = _source_kind(source_type)
        if source_kind == "unsupported":
            checks.append(
                ColumnCheck(
                    source_column=name,
                    source_type=source_type,
                    target_column=target.name,
                    target_type=target.data_type,
                    status="type_warning",
                    message=f"{source_type} has no Oracle equivalent",
                )
            )
            errors.append(
                f'Column "{name}" is {source_type}, which has no Oracle equivalent. '
                "Cast it in the SQL query first."
            )
            continue

        risk = _RISKY_PAIRS.get((source_kind, _target_kind(target.data_type)))
        if risk:
            checks.append(
                ColumnCheck(
                    source_column=name,
                    source_type=source_type,
                    target_column=target.name,
                    target_type=_render_target_type(target),
                    status="type_warning",
                    message=risk,
                )
            )
            warnings.append(f'Column "{name}" -> {target.name}: {risk}')
        else:
            checks.append(
                ColumnCheck(
                    source_column=name,
                    source_type=source_type,
                    target_column=target.name,
                    target_type=_render_target_type(target),
                    status="ok",
                )
            )
        plans.append(
            ColumnPlan(
                source_index=index,
                source_column=name,
                source_type=source_type,
                target=target,
            )
        )

    unmapped = [column.name for column in target_columns if column.name.upper() not in matched]
    for column in target_columns:
        if column.name.upper() in matched:
            continue
        if not column.nullable and not column.has_default:
            errors.append(
                f'Target column "{column.name}" is NOT NULL with no default, but the query '
                "does not provide it"
            )

    duplicates = _duplicate_names(source_columns)
    if duplicates:
        errors.append(
            "The query returns duplicate column names: " + ", ".join(sorted(duplicates))
        )

    return ExportPlan(
        columns=checks,
        plans=plans,
        unmapped_target_columns=unmapped,
        errors=errors,
        warnings=warnings,
    )


def _duplicate_names(names: list[str]) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for name in names:
        upper = name.upper()
        if upper in seen:
            duplicates.add(name)
        seen.add(upper)
    return duplicates


def _render_target_type(column: TargetColumn) -> str:
    """The declared type as a DBA would write it, for the validation report."""
    t = column.data_type.upper()
    if t in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR") and column.char_length:
        return f"{t}({column.char_length})"
    if t == "NUMBER" and column.precision is not None:
        if column.scale:
            return f"NUMBER({column.precision},{column.scale})"
        return f"NUMBER({column.precision})"
    if t == "RAW" and column.data_length:
        return f"RAW({column.data_length})"
    return t


# ----------------------------------------------------------------------------
# Oracle writer
# ----------------------------------------------------------------------------


def _quote_oracle(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


class OracleTableWriter:
    """Streams DuckDB batches into an existing Oracle table with array binds.

    Modeled on replicate_table.copy_rows, with two fixes that matter here:
    binds are pre-sized from the target's dictionary metadata rather than
    inferred from the first batch (an all-NULL or narrow first batch otherwise
    mis-sizes every batch after it), and the whole append is one transaction.
    """

    def describe_target(self, connection, schema: str, table: str) -> list[TargetColumn]:
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                SELECT column_name, data_type, nullable, data_default,
                       char_length, data_length, data_precision, data_scale
                FROM all_tab_columns
                WHERE owner = :owner AND table_name = :table_name
                ORDER BY column_id
                """,
                owner=schema,
                table_name=table,
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()

        return [
            TargetColumn(
                name=row[0],
                data_type=row[1],
                nullable=row[2] == "Y",
                # data_default is a LONG; its mere presence is all we need, and
                # reading the value would force a LONG fetch per column.
                has_default=row[3] is not None,
                char_length=row[4] or None,
                data_length=row[5] or None,
                precision=row[6],
                scale=row[7],
            )
            for row in rows
        ]

    def insert_sql(self, schema: str, table: str, plans: list[ColumnPlan]) -> str:
        columns = ", ".join(_quote_oracle(plan.target.name) for plan in plans)
        binds = ", ".join(f":{index}" for index in range(1, len(plans) + 1))
        return (
            f"INSERT INTO {_quote_oracle(schema)}.{_quote_oracle(table)} "
            f"({columns}) VALUES ({binds})"
        )

    def input_sizes(self, plans: list[ColumnPlan]) -> list[Any]:
        """Bind sizes from the target's declared types, so a narrow or all-NULL
        first batch can't under-size the binds for every batch after it.

        None means "let the driver infer", which is right for types where the
        declared metadata adds nothing.
        """
        sizes: list[Any] = []
        for plan in plans:
            data_type = plan.target.data_type.upper()
            if data_type in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "VARCHAR"):
                # An int means "string of at most this many characters".
                sizes.append(max(plan.target.char_length or plan.target.data_length or 1, 1))
            elif data_type in ("CLOB", "NCLOB"):
                sizes.append(oracledb.DB_TYPE_CLOB)
            elif data_type == "BLOB":
                sizes.append(oracledb.DB_TYPE_BLOB)
            elif data_type in ("RAW", "LONG RAW"):
                sizes.append(oracledb.DB_TYPE_RAW)
            elif data_type in ("NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE", "INTEGER"):
                sizes.append(oracledb.DB_TYPE_NUMBER)
            elif data_type == "DATE":
                sizes.append(oracledb.DB_TYPE_DATE)
            elif data_type.startswith("TIMESTAMP"):
                sizes.append(oracledb.DB_TYPE_TIMESTAMP)
            else:
                sizes.append(None)
        return sizes

    def append_rows(
        self,
        connection,
        schema: str,
        table: str,
        plans: list[ColumnPlan],
        batches: Iterable[list[tuple]],
        *,
        on_progress: Callable[[int], None] | None = None,
        should_abort: Callable[[], bool] | None = None,
    ) -> int:
        """Append every batch inside a single transaction.

        One commit at the end: with append-only semantics a partially written
        table is the worst outcome, because there is no way to tell from the
        outside how much landed. It also makes abort correct for free — the
        rollback in the except branch undoes whatever streamed in so far.
        """
        sql = self.insert_sql(schema, table, plans)
        indexes = [plan.source_index for plan in plans]
        adapters = [_value_adapter(plan) for plan in plans]
        rows_written = 0

        cursor = connection.cursor()
        try:
            cursor.setinputsizes(*self.input_sizes(plans))
            for batch in batches:
                if should_abort is not None and should_abort():
                    raise ExportAborted()
                if not batch:
                    continue
                bind_rows = [
                    tuple(adapter(row[index]) for index, adapter in zip(indexes, adapters))
                    for row in batch
                ]
                cursor.executemany(sql, bind_rows)
                rows_written += len(bind_rows)
                if on_progress is not None:
                    on_progress(rows_written)
            if should_abort is not None and should_abort():
                raise ExportAborted()
            connection.commit()
            return rows_written
        except BaseException:
            try:
                connection.rollback()
            except Exception:
                logger.warning("Rollback failed after a database export error", exc_info=True)
            raise
        finally:
            cursor.close()


def _value_adapter(plan: ColumnPlan) -> Callable[[Any], Any]:
    """Per-column conversion from a DuckDB value to an Oracle bind value.

    DuckDB hands back Python natives that oracledb binds directly (Decimal,
    datetime, date, bytes, str, None); booleans are the exception, since Oracle
    has no BOOLEAN column type before 23c.
    """
    if _source_kind(plan.source_type) == "bool":
        return lambda value: None if value is None else (1 if value else 0)
    return lambda value: value


# ----------------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------------


def describe_source(manager, sql: str, resolution: dict[str, str] | None) -> tuple[list[str], list[str]]:
    """Column names and DuckDB types of the export query, without reading rows."""
    with manager.pinned_query(f"SELECT * FROM ({sql}) LIMIT 0", resolution) as cur:
        return [d[0] for d in cur.description], [str(d[1]) for d in cur.description]


def _iter_batches(cursor, batch_rows: int) -> Iterable[list[tuple]]:
    while True:
        rows = cursor.fetchmany(batch_rows)
        if not rows:
            return
        yield [tuple(row) for row in rows]


def append_query_to_table_sync(
    manager,
    connection,
    sql: str,
    resolution: dict[str, str] | None,
    schema: str,
    table: str,
    *,
    writer: DatabaseExportWriter | None = None,
    batch_rows: int = BATCH_ROWS,
    on_progress: Callable[[int], None] | None = None,
    should_abort: Callable[[], bool] | None = None,
) -> dict:
    """Plan the append, then stream DuckDB → the target table. Blocking."""
    writer = writer or OracleTableWriter()
    target_columns = writer.describe_target(connection, schema, table)
    if not target_columns:
        raise DatabaseExportError(
            f"Table {schema}.{table} was not found, or this user cannot see it"
        )

    with manager.pinned_query(sql, resolution) as cur:
        source_columns = [d[0] for d in cur.description]
        source_types = [str(d[1]) for d in cur.description]
        plan = build_export_plan(source_columns, source_types, target_columns)
        if not plan.ok:
            raise DatabaseExportError("; ".join(plan.errors))

        rows_written = writer.append_rows(
            connection,
            schema,
            table,
            plan.plans,
            _iter_batches(cur, batch_rows),
            on_progress=on_progress,
            should_abort=should_abort,
        )

    return {
        "row_count": rows_written,
        "target_table": f"{schema}.{table}",
        "columns": [p.target.name for p in plan.plans],
        "warnings": plan.warnings,
    }


def validate_export_sync(
    manager,
    connection,
    sql: str,
    resolution: dict[str, str] | None,
    schema: str,
    table: str,
    *,
    writer: DatabaseExportWriter | None = None,
) -> dict:
    """Compare the query's columns against the live target. Blocking, no rows."""
    writer = writer or OracleTableWriter()
    target_columns = writer.describe_target(connection, schema, table)
    target_table = f"{schema}.{table}"
    if not target_columns:
        return {
            "target_table": target_table,
            "target_exists": False,
            "columns": [],
            "unmapped_target_columns": [],
            "errors": [f"Table {target_table} was not found, or this user cannot see it"],
            "warnings": [],
            "ok": False,
        }

    source_columns, source_types = describe_source(manager, sql, resolution)
    plan = build_export_plan(source_columns, source_types, target_columns)
    return {
        "target_table": target_table,
        "target_exists": True,
        "columns": [vars(check) for check in plan.columns],
        "unmapped_target_columns": plan.unmapped_target_columns,
        "errors": plan.errors,
        "warnings": plan.warnings,
        "ok": plan.ok,
    }


async def append_query_to_table(*args, **kwargs) -> dict:
    return await asyncio.to_thread(lambda: append_query_to_table_sync(*args, **kwargs))


async def validate_export(*args, **kwargs) -> dict:
    return await asyncio.to_thread(lambda: validate_export_sync(*args, **kwargs))
