"""Best-effort sheet dimensions from the zip (docs/excel-node-model.md §4)."""

from tests.conftest import _write_xlsx

from app.services.excel_service import read_sheet_dimensions


def test_reads_dimensions_when_present(tmp_path):
    path = tmp_path / "dims.xlsx"
    _write_xlsx(
        path,
        {
            "Orders": [["id", "name", "value"], [1, "Alice", 10.5]],
            "Summary": [["metric", "value"]],
        },
        dimensions={"Orders": "A1:C1204", "Summary": "A1:B1"},
    )

    assert read_sheet_dimensions(path) == {
        "Orders": {"rows": 1204, "cols": 3},
        "Summary": {"rows": 1, "cols": 2},
    }


def test_missing_dimension_degrades_to_none_per_sheet(tmp_path):
    path = tmp_path / "partial.xlsx"
    _write_xlsx(
        path,
        {
            "WithDim": [["a"]],
            "WithoutDim": [["b"]],
        },
        dimensions={"WithDim": "A1:D25"},
    )

    assert read_sheet_dimensions(path) == {
        "WithDim": {"rows": 25, "cols": 4},
        "WithoutDim": None,
    }


def test_single_cell_and_garbage_refs(tmp_path):
    path = tmp_path / "odd.xlsx"
    _write_xlsx(
        path,
        {
            "SingleCell": [["a"]],
            "Garbage": [["b"]],
        },
        dimensions={"SingleCell": "A1", "Garbage": "not-a-range"},
    )

    assert read_sheet_dimensions(path) == {
        "SingleCell": {"rows": 1, "cols": 1},
        "Garbage": None,
    }


def test_unreadable_file_degrades_to_empty(tmp_path):
    path = tmp_path / "broken.xlsx"
    path.write_bytes(b"this is not a zip")

    assert read_sheet_dimensions(path) == {}
