import asyncio
import os
import threading

import pandas as pd
import oracledb

DEFAULT_FETCH_MODE = "fetchall"
DEFAULT_ARRAYSIZE = 100
DEFAULT_PREFETCHROWS = 2


def normalize_fetch_config(fetch_config: dict | None) -> dict:
    mode = fetch_config.get("mode") if isinstance(fetch_config, dict) else None
    arraysize = fetch_config.get("arraysize") if isinstance(fetch_config, dict) else None
    prefetchrows = fetch_config.get("prefetchrows") if isinstance(fetch_config, dict) else None

    return {
        "mode": "fetchmany" if mode == "fetchmany" else DEFAULT_FETCH_MODE,
        "arraysize": arraysize if isinstance(arraysize, int) and arraysize >= 1 else DEFAULT_ARRAYSIZE,
        "prefetchrows": prefetchrows if isinstance(prefetchrows, int) and prefetchrows >= 0 else DEFAULT_PREFETCHROWS,
    }


class OracleService:
    _client_initialized = False
    _client_lock = threading.Lock()

    @classmethod
    def _ensure_thick_mode(cls) -> None:
        if cls._client_initialized:
            return

        with cls._client_lock:
            if cls._client_initialized:
                return

            init_kwargs = {}
            lib_dir = os.getenv("ORACLE_CLIENT_LIB_DIR")
            config_dir = os.getenv("ORACLE_CLIENT_CONFIG_DIR")
            if lib_dir:
                init_kwargs["lib_dir"] = lib_dir
            if config_dir:
                init_kwargs["config_dir"] = config_dir

            try:
                oracledb.init_oracle_client(**init_kwargs)
            except oracledb.ProgrammingError as exc:
                if "already been initialized" not in str(exc):
                    raise RuntimeError(
                        "Oracle Thick mode initialization failed. Install Oracle Instant Client "
                        "and set ORACLE_CLIENT_LIB_DIR to the client library directory. "
                        "If you use tnsnames.ora/sqlnet.ora, set ORACLE_CLIENT_CONFIG_DIR too."
                    ) from exc
            except Exception as exc:
                raise RuntimeError(
                    "Oracle Thick mode initialization failed. Install Oracle Instant Client "
                    "and set ORACLE_CLIENT_LIB_DIR to the client library directory. "
                    "If you use tnsnames.ora/sqlnet.ora, set ORACLE_CLIENT_CONFIG_DIR too."
                ) from exc

            if oracledb.is_thin_mode():
                raise RuntimeError(
                    "python-oracledb is still running in Thin mode. Restart the backend and make "
                    "sure Thick mode is initialized before any Oracle connection is opened."
                )

            cls._client_initialized = True

    def _connect(self, conn_config: dict):
        self._ensure_thick_mode()
        dsn = oracledb.makedsn(
            conn_config["host"],
            conn_config["port"],
            service_name=conn_config["service_name"],
        )
        return oracledb.connect(
            user=conn_config["user"],
            password=conn_config["password"],
            dsn=dsn,
        )

    def _apply_fetch_config(self, cursor, fetch_config: dict | None) -> dict:
        normalized = normalize_fetch_config(fetch_config)
        cursor.arraysize = normalized["arraysize"]
        cursor.prefetchrows = normalized["prefetchrows"]
        return normalized

    def _fetch_query_sync(self, connection, query: str, fetch_config: dict | None = None) -> pd.DataFrame:
        cursor = connection.cursor()
        try:
            normalized = self._apply_fetch_config(cursor, fetch_config)
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            if normalized["mode"] == "fetchmany":
                frames: list[pd.DataFrame] = []
                while True:
                    rows = cursor.fetchmany()
                    if not rows:
                        break
                    frames.append(pd.DataFrame(rows, columns=columns))

                if not frames:
                    return pd.DataFrame(columns=columns)
                return pd.concat(frames, ignore_index=True)

            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        finally:
            cursor.close()

    def _load_query_to_duckdb_sync(self, connection, query: str, table_name: str, duckdb_manager, fetch_config: dict | None = None) -> dict:
        cursor = connection.cursor()
        try:
            normalized = self._apply_fetch_config(cursor, fetch_config)
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]

            if normalized["mode"] != "fetchmany":
                rows = cursor.fetchall()
                return duckdb_manager.register_dataframe(table_name, pd.DataFrame(rows, columns=columns))

            table_created = False
            while True:
                rows = cursor.fetchmany()
                if not rows:
                    if table_created:
                        return duckdb_manager.table_stats(table_name)
                    empty_df = pd.DataFrame({column: pd.Series(dtype="object") for column in columns})
                    return duckdb_manager.register_dataframe(table_name, empty_df)

                chunk_df = pd.DataFrame(rows, columns=columns)
                if not table_created:
                    duckdb_manager.register_dataframe(table_name, chunk_df)
                    table_created = True
                else:
                    duckdb_manager.append_dataframe(table_name, chunk_df)
        finally:
            cursor.close()

    async def connect(self, config: dict):
        return await asyncio.to_thread(self._connect, config["connection"])

    async def fetch_query(self, connection, query: str, fetch_config: dict | None = None) -> pd.DataFrame:
        return await asyncio.to_thread(self._fetch_query_sync, connection, query, fetch_config)

    async def load_query_to_duckdb(self, connection, query: str, table_name: str, duckdb_manager, fetch_config: dict | None = None) -> dict:
        return await asyncio.to_thread(
            self._load_query_to_duckdb_sync,
            connection,
            query,
            table_name,
            duckdb_manager,
            fetch_config,
        )

    def _execute_query_sync(self, config: dict) -> pd.DataFrame:
        connection = self._connect(config["connection"])
        try:
            return self._fetch_query_sync(connection, config["query"], config.get("fetch_config"))
        finally:
            connection.close()

    async def execute_query(self, config: dict) -> pd.DataFrame:
        return await asyncio.to_thread(self._execute_query_sync, config)

    def abort_query(self, connection) -> None:
        connection.cancel()

    def _test_connection_sync(self, config: dict) -> None:
        connection = self._connect(config)
        connection.close()

    async def test_connection(self, config: dict) -> bool:
        await asyncio.to_thread(self._test_connection_sync, config)
        return True
