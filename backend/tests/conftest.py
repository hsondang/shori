import csv
import pathlib

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import app.config as config_module
from app.main import app
from app.services.duckdb_manager import DuckDBManager


@pytest.fixture(autouse=True)
def tmp_dirs(monkeypatch, tmp_path):
    """Redirect all data dirs to isolated tmp dirs for each test."""
    pipeline_dir = tmp_path / "pipelines"
    upload_dir = tmp_path / "uploads"
    export_dir = tmp_path / "exports"
    for d in [pipeline_dir, upload_dir, export_dir]:
        d.mkdir()

    monkeypatch.setattr(config_module, "PIPELINE_DIR", pipeline_dir)
    monkeypatch.setattr(config_module, "UPLOAD_DIR", upload_dir)
    monkeypatch.setattr(config_module, "EXPORT_DIR", export_dir)

    # Also patch the storage module that has already imported PIPELINE_DIR
    import app.storage.pipeline_store as ps_mod
    monkeypatch.setattr(ps_mod, "PIPELINE_DIR", pipeline_dir)

    # Patch the upload router's csv_service reference
    import app.services.csv_service as csv_mod
    monkeypatch.setattr(csv_mod, "UPLOAD_DIR", upload_dir)

    # Patch data router's EXPORT_DIR
    import app.routers.data as data_mod
    monkeypatch.setattr(data_mod, "EXPORT_DIR", export_dir)


@pytest_asyncio.fixture
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        # Manually initialise app state so DuckDB is available without full lifespan
        app.state.duckdb = DuckDBManager()
        yield ac
        app.state.duckdb.close()


@pytest.fixture
def duckdb_mgr():
    mgr = DuckDBManager()
    yield mgr
    mgr.close()


@pytest.fixture
def sample_csv_file(tmp_path) -> str:
    """Write a small 5-row CSV and return its absolute path."""
    path = tmp_path / "sample.csv"
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "value"])
        writer.writerows([
            [1, "Alice", 10.5],
            [2, "Bob", 20.0],
            [3, "Carol", 30.75],
            [4, "Dave", 40.0],
            [5, "Eve", 50.25],
        ])
    return str(path)


@pytest.fixture
def pipeline_def(sample_csv_file):
    """A minimal valid pipeline definition dict (one CSV_SOURCE node, no edges)."""
    return {
        "id": "test-pipeline-1",
        "name": "Test Pipeline",
        "nodes": [
            {
                "id": "node-1",
                "type": "csv_source",
                "table_name": "my_table",
                "label": "My CSV",
                "position": {"x": 0, "y": 0},
                "config": {
                    "file_path": sample_csv_file,
                    "original_filename": "sample.csv",
                },
            }
        ],
        "edges": [],
    }
