import threading

import duckdb


class DuckDBManager:
    def __init__(self):
        self.conn = duckdb.connect(":memory:")
        self._lock = threading.Lock()

    def register_csv(self, table_name: str, file_path: str) -> dict:
        with self._lock:
            self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            self.conn.execute(
                f'CREATE TABLE "{table_name}" AS SELECT * FROM read_csv_auto(?)',
                [file_path],
            )
            return self._table_stats(table_name)

    def register_dataframe(self, table_name: str, df) -> dict:
        with self._lock:
            self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            self.conn.execute(
                f'CREATE TABLE "{table_name}" AS SELECT * FROM df'
            )
            return self._table_stats(table_name)

    def execute_transform(self, table_name: str, sql: str) -> dict:
        with self._lock:
            self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            self.conn.execute(f'CREATE TABLE "{table_name}" AS ({sql})')
            return self._table_stats(table_name)

    def preview(self, table_name: str, offset: int = 0, limit: int = 100) -> dict:
        with self._lock:
            cols_result = self.conn.execute(
                f'DESCRIBE "{table_name}"'
            ).fetchall()
            columns = [row[0] for row in cols_result]
            col_types = [row[1] for row in cols_result]

            rows = self.conn.execute(
                f'SELECT * FROM "{table_name}" LIMIT ? OFFSET ?',
                [limit, offset],
            ).fetchall()

            total = self.conn.execute(
                f'SELECT COUNT(*) FROM "{table_name}"'
            ).fetchone()[0]

            return {
                "columns": columns,
                "column_types": col_types,
                "rows": [list(r) for r in rows],
                "total_rows": total,
                "offset": offset,
                "limit": limit,
            }

    def export_to_csv(self, table_name: str, output_path: str):
        with self._lock:
            self.conn.execute(
                f"COPY \"{table_name}\" TO '{output_path}' (HEADER, DELIMITER ',')"
            )

    def drop_table(self, table_name: str):
        with self._lock:
            self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')

    def table_exists(self, table_name: str) -> bool:
        with self._lock:
            result = self.conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()
            return result[0] > 0

    def _table_stats(self, table_name: str) -> dict:
        count = self.conn.execute(
            f'SELECT COUNT(*) FROM "{table_name}"'
        ).fetchone()[0]
        cols = self.conn.execute(f'DESCRIBE "{table_name}"').fetchall()
        return {
            "row_count": count,
            "column_count": len(cols),
            "columns": [c[0] for c in cols],
        }

    def close(self):
        self.conn.close()
