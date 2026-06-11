from datetime import date, datetime, time, timedelta
from decimal import Decimal
import logging
import math
import threading

import duckdb

logger = logging.getLogger(__name__)


class DuckDBManager:
    def __init__(self):
        self.conn = duckdb.connect(":memory:")
        self._lock = threading.Lock()

    def register_csv(self, table_name: str, file_path: str) -> dict:
        with self._lock:
            quoted_table_name = _quote_identifier(table_name)
            self.conn.execute(f"DROP TABLE IF EXISTS {quoted_table_name}")
            self.conn.execute(
                # Scan the full CSV before inferring types so late mixed-type IDs stay text.
                f"CREATE TABLE {quoted_table_name} AS SELECT * FROM read_csv_auto(?, sample_size=-1)",
                [file_path],
            )
            return self._table_stats(table_name)

    def register_dataframe(self, table_name: str, df) -> dict:
        with self._lock:
            quoted_table_name = _quote_identifier(table_name)
            self.conn.execute(f"DROP TABLE IF EXISTS {quoted_table_name}")
            self.conn.execute(
                f"CREATE TABLE {quoted_table_name} AS SELECT * FROM df"
            )
            return self._table_stats(table_name)

    def append_dataframe(self, table_name: str, df) -> dict:
        with self._lock:
            quoted_table_name = _quote_identifier(table_name)
            self.conn.execute(
                f"INSERT INTO {quoted_table_name} SELECT * FROM df"
            )
            return self._table_stats(table_name)

    def execute_transform(self, table_name: str, sql: str) -> dict:
        with self._lock:
            quoted_table_name = _quote_identifier(table_name)
            self.conn.execute(f"DROP TABLE IF EXISTS {quoted_table_name}")
            self.conn.execute(f"CREATE TABLE {quoted_table_name} AS ({sql})")
            return self._table_stats(table_name)

    def preview(self, table_name: str, offset: int = 0, limit: int = 100) -> dict:
        with self._lock:
            quoted_table_name = _quote_identifier(table_name)
            cols_result = self.conn.execute(
                f"DESCRIBE {quoted_table_name}"
            ).fetchall()
            columns = [row[0] for row in cols_result]
            col_types = [row[1] for row in cols_result]

            rows = self._fetch_preview_rows(quoted_table_name, columns, offset, limit)

            total = self.conn.execute(
                f"SELECT COUNT(*) FROM {quoted_table_name}"
            ).fetchone()[0]

            return {
                "kind": "table",
                "columns": columns,
                "column_types": col_types,
                "rows": [[_json_safe_value(value) for value in row] for row in rows],
                "total_rows": total,
                "offset": offset,
                "limit": limit,
            }

    def export_to_csv(self, table_name: str, output_path: str):
        with self._lock:
            self.conn.execute(
                f"COPY {_quote_identifier(table_name)} TO '{output_path}' (HEADER, DELIMITER ',')"
            )

    def drop_table(self, table_name: str):
        with self._lock:
            self.conn.execute(f"DROP TABLE IF EXISTS {_quote_identifier(table_name)}")

    def table_exists(self, table_name: str) -> bool:
        with self._lock:
            result = self.conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()
            return result[0] > 0

    def table_stats(self, table_name: str) -> dict:
        with self._lock:
            return self._table_stats(table_name)

    def _table_stats(self, table_name: str) -> dict:
        quoted_table_name = _quote_identifier(table_name)
        count = self.conn.execute(
            f"SELECT COUNT(*) FROM {quoted_table_name}"
        ).fetchone()[0]
        cols = self.conn.execute(f"DESCRIBE {quoted_table_name}").fetchall()
        return {
            "row_count": count,
            "column_count": len(cols),
            "columns": [c[0] for c in cols],
        }

    def _fetch_preview_rows(
        self,
        quoted_table_name: str,
        columns: list[str],
        offset: int,
        limit: int,
    ) -> list[tuple]:
        try:
            return self.conn.execute(
                f"SELECT * FROM {quoted_table_name} LIMIT ? OFFSET ?",
                [limit, offset],
            ).fetchall()
        except duckdb.Error:
            # Some column types (e.g. TIMESTAMP WITH TIME ZONE) cannot be fetched
            # natively into Python; retry with every column cast to text.
            logger.warning(
                "Native preview fetch for %s failed; retrying with all columns "
                "cast to VARCHAR.",
                quoted_table_name,
                exc_info=True,
            )
            select_list = ", ".join(
                f"CAST({_quote_identifier(column)} AS VARCHAR) AS {_quote_identifier(column)}"
                for column in columns
            )
            if not select_list:
                return []
            return self.conn.execute(
                f"SELECT {select_list} FROM {quoted_table_name} LIMIT ? OFFSET ?",
                [limit, offset],
            ).fetchall()

    def close(self):
        self.conn.close()


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _json_safe_value(value):
    if value is None or isinstance(value, (bool, str, int)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    return str(value)
