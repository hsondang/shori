import pytest

from app.services.oracle_service import OracleService


class FakeCursor:
    def __init__(self, rows=None):
        self.description = [("ID",), ("NAME",)]
        self.rows = rows if rows is not None else [(1, "Alice"), (2, "Bob")]
        self.executed = []
        self.closed = False
        self.arraysize = None
        self.prefetchrows = None
        self.fetchmany_calls = 0

    def execute(self, query):
        self.executed.append(query)

    def fetchall(self):
        return self.rows

    def fetchmany(self):
        start = self.fetchmany_calls * (self.arraysize or len(self.rows) or 1)
        end = start + (self.arraysize or len(self.rows) or 1)
        self.fetchmany_calls += 1
        return self.rows[start:end]

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, rows=None):
        self.cursor_instance = FakeCursor(rows=rows)
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


class FakeStagingLoad:
    def __init__(self, manager, node_id, table_name, cache_key):
        self.manager = manager
        self.node_id = node_id
        self.table_name = table_name
        self.cache_key = cache_key
        self.appended_frames = []

    def mark_loading(self):
        pass

    def append(self, df):
        self.appended_frames.append(df.copy())

    def commit(self, record_meta=True):
        frames = self.appended_frames
        row_count = sum(len(frame.index) for frame in frames)
        columns = list(frames[0].columns) if frames else []
        self.manager.committed_loads.append(self)
        return {
            "row_count": row_count,
            "column_count": len(columns),
            "columns": columns,
        }

    def abort(self, error=None, record_meta=True):
        self.manager.aborted_loads.append((self, error))


class FakeDuckDBManager:
    def __init__(self):
        self.committed_loads = []
        self.aborted_loads = []

    def begin_load(self, node_id, table_name, cache_key=None):
        return FakeStagingLoad(self, node_id, table_name, cache_key)


@pytest.mark.asyncio
async def test_execute_query_initializes_thick_mode(monkeypatch):
    OracleService._client_initialized = False

    init_calls = []
    connect_calls = []
    connection = FakeConnection()

    monkeypatch.setenv("ORACLE_CLIENT_LIB_DIR", "/opt/oracle/instantclient")
    monkeypatch.delenv("ORACLE_CLIENT_CONFIG_DIR", raising=False)

    import app.services.oracle_service as oracle_module

    monkeypatch.setattr(
        oracle_module.oracledb,
        "init_oracle_client",
        lambda **kwargs: init_calls.append(kwargs),
    )
    monkeypatch.setattr(oracle_module.oracledb, "is_thin_mode", lambda: False)
    monkeypatch.setattr(
        oracle_module.oracledb,
        "makedsn",
        lambda host, port, service_name: f"{host}:{port}/{service_name}",
    )

    def fake_connect(**kwargs):
        connect_calls.append(kwargs)
        return connection

    monkeypatch.setattr(oracle_module.oracledb, "connect", fake_connect)

    service = OracleService()
    df = await service.execute_query(
        {
            "connection": {
                "host": "db.example.com",
                "port": 1521,
                "service_name": "xe",
                "user": "scott",
                "password": "tiger",
            },
            "query": "select * from dual",
        }
    )

    assert init_calls == [{"lib_dir": "/opt/oracle/instantclient"}]
    assert connect_calls == [
        {
            "user": "scott",
            "password": "tiger",
            "dsn": "db.example.com:1521/xe",
        }
    ]
    assert list(df.columns) == ["ID", "NAME"]
    assert df.to_dict(orient="records") == [
        {"ID": 1, "NAME": "Alice"},
        {"ID": 2, "NAME": "Bob"},
    ]
    assert connection.cursor_instance.closed is True
    assert connection.closed is True
    assert connection.cursor_instance.arraysize == 100
    assert connection.cursor_instance.prefetchrows == 2


@pytest.mark.asyncio
async def test_test_connection_reports_thick_mode_setup_errors(monkeypatch):
    OracleService._client_initialized = False

    import app.services.oracle_service as oracle_module

    def fail_init(**kwargs):
        raise RuntimeError("DPI-1047: Cannot locate a 64-bit Oracle Client library")

    monkeypatch.setattr(oracle_module.oracledb, "init_oracle_client", fail_init)

    service = OracleService()

    with pytest.raises(RuntimeError, match="ORACLE_CLIENT_LIB_DIR"):
        await service.test_connection(
            {
                "host": "db.example.com",
                "port": 1521,
                "service_name": "xe",
                "user": "scott",
                "password": "tiger",
            }
        )


@pytest.mark.asyncio
async def test_fetch_query_uses_fetchmany_with_configured_arraysize_and_prefetchrows(monkeypatch):
    OracleService._client_initialized = False

    connection = FakeConnection(rows=[(1, "Alice"), (2, "Bob"), (3, "Carol")])

    import app.services.oracle_service as oracle_module

    monkeypatch.setattr(oracle_module.oracledb, "init_oracle_client", lambda **_kwargs: None)
    monkeypatch.setattr(oracle_module.oracledb, "is_thin_mode", lambda: False)
    monkeypatch.setattr(
        oracle_module.oracledb,
        "makedsn",
        lambda host, port, service_name: f"{host}:{port}/{service_name}",
    )
    monkeypatch.setattr(oracle_module.oracledb, "connect", lambda **_kwargs: connection)

    service = OracleService()
    df = await service.execute_query(
        {
            "connection": {
                "host": "db.example.com",
                "port": 1521,
                "service_name": "xe",
                "user": "scott",
                "password": "tiger",
            },
            "query": "select * from dual",
            "fetch_config": {
                "mode": "fetchmany",
                "arraysize": 2,
                "prefetchrows": 5,
            },
        }
    )

    assert connection.cursor_instance.arraysize == 2
    assert connection.cursor_instance.prefetchrows == 5
    assert connection.cursor_instance.fetchmany_calls >= 2
    assert df.to_dict(orient="records") == [
        {"ID": 1, "NAME": "Alice"},
        {"ID": 2, "NAME": "Bob"},
        {"ID": 3, "NAME": "Carol"},
    ]


@pytest.mark.asyncio
async def test_load_query_to_duckdb_streams_fetchmany_chunks(monkeypatch):
    OracleService._client_initialized = False

    connection = FakeConnection(rows=[(1, "Alice"), (2, "Bob"), (3, "Carol")])
    duckdb_manager = FakeDuckDBManager()

    import app.services.oracle_service as oracle_module

    monkeypatch.setattr(oracle_module.oracledb, "init_oracle_client", lambda **_kwargs: None)
    monkeypatch.setattr(oracle_module.oracledb, "is_thin_mode", lambda: False)
    monkeypatch.setattr(
        oracle_module.oracledb,
        "makedsn",
        lambda host, port, service_name: f"{host}:{port}/{service_name}",
    )
    monkeypatch.setattr(oracle_module.oracledb, "connect", lambda **_kwargs: connection)

    service = OracleService()
    stats = await service.load_query_to_duckdb(
        connection,
        "select * from dual",
        "oracle_rows",
        duckdb_manager,
        {"mode": "fetchmany", "arraysize": 2, "prefetchrows": 4},
    )

    assert stats == {
        "row_count": 3,
        "column_count": 2,
        "columns": ["ID", "NAME"],
    }
    assert len(duckdb_manager.committed_loads) == 1
    load = duckdb_manager.committed_loads[0]
    assert load.table_name == "oracle_rows"
    assert len(load.appended_frames) == 2
    assert load.appended_frames[0].to_dict(orient="records") == [
        {"ID": 1, "NAME": "Alice"},
        {"ID": 2, "NAME": "Bob"},
    ]
    assert load.appended_frames[1].to_dict(orient="records") == [
        {"ID": 3, "NAME": "Carol"},
    ]


@pytest.mark.asyncio
async def test_load_query_to_duckdb_creates_empty_table_for_empty_fetchmany(monkeypatch):
    OracleService._client_initialized = False

    connection = FakeConnection(rows=[])
    duckdb_manager = FakeDuckDBManager()

    import app.services.oracle_service as oracle_module

    monkeypatch.setattr(oracle_module.oracledb, "init_oracle_client", lambda **_kwargs: None)
    monkeypatch.setattr(oracle_module.oracledb, "is_thin_mode", lambda: False)
    monkeypatch.setattr(
        oracle_module.oracledb,
        "makedsn",
        lambda host, port, service_name: f"{host}:{port}/{service_name}",
    )
    monkeypatch.setattr(oracle_module.oracledb, "connect", lambda **_kwargs: connection)

    service = OracleService()
    stats = await service.load_query_to_duckdb(
        connection,
        "select * from dual where 1 = 0",
        "oracle_empty",
        duckdb_manager,
        {"mode": "fetchmany", "arraysize": 1000, "prefetchrows": 2},
    )

    assert stats == {
        "row_count": 0,
        "column_count": 2,
        "columns": ["ID", "NAME"],
    }
    assert len(duckdb_manager.committed_loads) == 1
    load = duckdb_manager.committed_loads[0]
    assert load.table_name == "oracle_empty"
    assert len(load.appended_frames) == 1
    assert list(load.appended_frames[0].columns) == ["ID", "NAME"]
    assert load.appended_frames[0].empty is True
