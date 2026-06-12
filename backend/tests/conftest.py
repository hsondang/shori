import csv
from html import escape
import pathlib
import zipfile

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import app.config as config_module
from app.main import app
from app.services.connection_pools import ConnectionPoolRegistry
from app.services.csv_service import CsvPreprocessArtifactStore
from app.services.duckdb_manager import DuckDBManager
from app.services.execution_registry import ExecutionRegistry
from app.services.project_db_registry import ProjectDuckDBRegistry


@pytest.fixture(autouse=True)
def tmp_dirs(monkeypatch, tmp_path):
    """Redirect all data dirs to isolated tmp dirs for each test."""
    pipeline_dir = tmp_path / "pipelines"
    upload_dir = tmp_path / "uploads"
    export_dir = tmp_path / "exports"
    projects_dir = tmp_path / "projects"
    project_db_path = tmp_path / "projects.sqlite3"
    for d in [pipeline_dir, upload_dir, export_dir, projects_dir]:
        d.mkdir()

    monkeypatch.setattr(config_module, "PIPELINE_DIR", pipeline_dir)
    monkeypatch.setattr(config_module, "UPLOAD_DIR", upload_dir)
    monkeypatch.setattr(config_module, "EXPORT_DIR", export_dir)
    monkeypatch.setattr(config_module, "PROJECTS_DIR", projects_dir)
    monkeypatch.setattr(config_module, "PROJECT_DB_PATH", project_db_path)

    # Also patch the storage module that has already imported config values.
    import app.storage.pipeline_store as ps_mod
    monkeypatch.setattr(ps_mod, "PROJECT_DB_PATH", project_db_path)

    # Patch the upload router's csv_service reference
    import app.services.csv_service as csv_mod
    monkeypatch.setattr(csv_mod, "UPLOAD_DIR", upload_dir)
    import app.services.excel_service as excel_mod
    monkeypatch.setattr(excel_mod, "UPLOAD_DIR", upload_dir)

    # Patch data router's EXPORT_DIR
    import app.routers.data as data_mod
    monkeypatch.setattr(data_mod, "EXPORT_DIR", export_dir)


@pytest_asyncio.fixture
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        # Manually initialise app state so DuckDB is available without full lifespan
        app.state.project_dbs = ProjectDuckDBRegistry()
        app.state.connection_pools = ConnectionPoolRegistry()
        app.state.csv_preprocess_artifacts = CsvPreprocessArtifactStore()
        app.state.execution_registry = ExecutionRegistry()
        yield ac
        app.state.execution_registry.close()
        app.state.csv_preprocess_artifacts.close()
        await app.state.connection_pools.close_all()
        app.state.project_dbs.close_all()


@pytest.fixture
def duckdb_mgr():
    mgr = DuckDBManager()
    yield mgr
    mgr.close()


@pytest.fixture
def csv_artifact_store():
    store = CsvPreprocessArtifactStore()
    yield store
    store.close()


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
def office365_csv_file(tmp_path) -> str:
    path = tmp_path / "office365.csv"
    path.write_text(
        "\n".join([
            "Created by: user x",
            "Created Time: 2026-03-13 17:03:20",
            "id,name,value",
            "1,Alice,10.5",
            "2,Bob,20.0",
        ]),
        encoding="utf-8",
    )
    return str(path)


@pytest.fixture
def excel_style_csv_file(tmp_path) -> str:
    path = tmp_path / "excel-style.csv"
    path.write_bytes(
        (
            "\ufeff,MONTHLY DATA ALLOCATION,,\r\n"
            "Notes,Synthetic spreadsheet-style export for CSV preview regression testing,,\r\n"
            ",,,\r\n"
            ",,,\r\n"
            "Employee ID,Agent Name,User,Quota\r\n"
            "EMP001,Agent One,user.one,\" 1,120   \"\r\n"
            "EMP002,Agent Two,user.two,\" 1,120   \"\r\n"
            "EMP003,Agent Three,user.three, 770   \r\n"
            "EMP004,Agent Four,user.four, 770   \r\n"
            ",,,\" 3,780   \"\r\n"
        ).encode("utf-8")
    )
    return str(path)


@pytest.fixture
def sample_excel_file(tmp_path) -> str:
    path = tmp_path / "sample.xlsx"
    _write_xlsx(
        path,
        {
            "Orders": [
                ["id", "name", "value"],
                [1, "Alice", 10.5],
                [2, "Bob", 20],
            ],
            "Summary": [
                ["metric", "value"],
                ["total", 2],
            ],
        },
    )
    return str(path)


def _write_xlsx(path: pathlib.Path, sheets: dict[str, list[list[object]]]) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr(
            "[Content_Types].xml",
            """<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
""" + "\n".join(
                f'  <Override PartName="/xl/worksheets/sheet{index}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
                for index in range(1, len(sheets) + 1)
            ) + "\n</Types>",
        )
        workbook.writestr(
            "_rels/.rels",
            """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>""",
        )
        workbook.writestr(
            "xl/workbook.xml",
            """<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
""" + "\n".join(
                f'    <sheet name="{escape(name)}" sheetId="{index}" r:id="rId{index}"/>'
                for index, name in enumerate(sheets.keys(), start=1)
            ) + """
  </sheets>
</workbook>""",
        )
        workbook.writestr(
            "xl/_rels/workbook.xml.rels",
            """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
""" + "\n".join(
                f'  <Relationship Id="rId{index}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{index}.xml"/>'
                for index in range(1, len(sheets) + 1)
            ) + "\n</Relationships>",
        )
        for index, rows in enumerate(sheets.values(), start=1):
            workbook.writestr(f"xl/worksheets/sheet{index}.xml", _worksheet_xml(rows))


def _worksheet_xml(rows: list[list[object]]) -> str:
    return """<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
""" + "\n".join(
        f'    <row r="{row_index}">' + "".join(
            _cell_xml(_column_name(column_index) + str(row_index), value)
            for column_index, value in enumerate(row, start=1)
        ) + "</row>"
        for row_index, row in enumerate(rows, start=1)
    ) + """
  </sheetData>
</worksheet>"""


def _cell_xml(reference: str, value: object) -> str:
    if isinstance(value, (int, float)):
        return f'<c r="{reference}"><v>{value}</v></c>'
    return f'<c r="{reference}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>'


def _column_name(index: int) -> str:
    name = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        name = chr(65 + remainder) + name
    return name


@pytest.fixture
def pipeline_def(sample_csv_file):
    """A minimal valid pipeline definition dict (one CSV_SOURCE node, no edges)."""
    return {
        "id": "test-pipeline-1",
        "name": "Test Pipeline",
        "database_connections": [],
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
