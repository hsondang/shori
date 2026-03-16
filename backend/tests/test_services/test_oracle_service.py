import pytest

from app.services.oracle_service import OracleService


class FakeCursor:
    def __init__(self):
        self.description = [("ID",), ("NAME",)]
        self.executed = []
        self.closed = False

    def execute(self, query):
        self.executed.append(query)

    def fetchall(self):
        return [(1, "Alice"), (2, "Bob")]

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.cursor_instance = FakeCursor()
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


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
