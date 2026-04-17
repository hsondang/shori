import asyncio
import os
import threading

import pandas as pd
import oracledb


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

    def _fetch_query_sync(self, connection, query: str) -> pd.DataFrame:
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        finally:
            cursor.close()

    async def connect(self, config: dict):
        return await asyncio.to_thread(self._connect, config["connection"])

    async def fetch_query(self, connection, query: str) -> pd.DataFrame:
        return await asyncio.to_thread(self._fetch_query_sync, connection, query)

    def _execute_query_sync(self, config: dict) -> pd.DataFrame:
        connection = self._connect(config["connection"])
        try:
            return self._fetch_query_sync(connection, config["query"])
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
