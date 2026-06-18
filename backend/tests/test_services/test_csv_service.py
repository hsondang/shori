from pathlib import Path

import pytest

from app.services import csv_service


def test_preview_csv_text_returns_first_parsed_rows(office365_csv_file):
    preview = csv_service.preview_csv_text(office365_csv_file, limit=3)

    assert preview["kind"] == "csv_text"
    assert preview["csv_stage"] == "raw"
    assert preview["rows"] == [
        ["Created by: user x"],
        ["Created Time: 2026-03-13 17:03:20"],
        ["id", "name", "value"],
    ]
    assert preview["limit"] == 3
    assert preview["truncated"] is True
    assert preview["artifact_ready"] is False


def test_preview_csv_text_falls_back_to_legacy_csv_encodings(tmp_path):
    csv_file = tmp_path / "legacy.csv"
    csv_file.write_bytes("Código,Nome\n1,João\n".encode("cp1252"))

    preview = csv_service.preview_csv_text(str(csv_file), limit=2)

    assert preview["rows"] == [
        ["Código", "Nome"],
        ["1", "João"],
    ]


def test_preview_csv_text_handles_excel_style_commas_and_notes(excel_style_csv_file):
    preview = csv_service.preview_csv_text(excel_style_csv_file, limit=20)

    assert preview["rows"] == [
        ["", "MONTHLY DATA ALLOCATION", "", ""],
        ["Notes", "Synthetic spreadsheet-style export for CSV preview regression testing", "", ""],
        ["", "", "", ""],
        ["", "", "", ""],
        ["Employee ID", "Agent Name", "User", "Quota"],
        ["EMP001", "Agent One", "user.one", " 1,120   "],
        ["EMP002", "Agent Two", "user.two", " 1,120   "],
        ["EMP003", "Agent Three", "user.three", " 770   "],
        ["EMP004", "Agent Four", "user.four", " 770   "],
        ["", "", "", " 3,780   "],
    ]
    assert all(len(row) == 4 for row in preview["rows"])


def _write_preprocess_script(tmp_path, body: str) -> str:
    path = tmp_path / "preprocess.py"
    path.write_text(body, encoding="utf-8")
    return str(path)


# Skips the two metadata header lines, then parses the rest into a DataFrame.
SKIP_TWO_LINES_SCRIPT = (
    "import pandas as pd\n"
    "def preprocess(file):\n"
    "    return pd.read_csv(file, skiprows=2)\n"
)


def test_preview_preprocessed_csv_text_stores_artifact(csv_artifact_store, office365_csv_file, tmp_path):
    preprocessing = {
        "enabled": True,
        "script_path": _write_preprocess_script(tmp_path, SKIP_TWO_LINES_SCRIPT),
    }

    preview = csv_service.preview_preprocessed_csv_text(
        csv_artifact_store,
        "node-1",
        office365_csv_file,
        preprocessing,
        limit=3,
    )

    assert preview["csv_stage"] == "preprocessed"
    assert preview["artifact_ready"] is True
    assert preview["rows"] == [
        ["id", "name", "value"],
        ["1", "Alice", "10.5"],
        ["2", "Bob", "20.0"],
    ]

    fingerprint = csv_service.preprocessing_fingerprint(office365_csv_file, preprocessing)
    artifact_path = csv_artifact_store.get("node-1", fingerprint)
    assert artifact_path is not None
    assert Path(artifact_path).exists()
    assert artifact_path.endswith(".parquet")


def test_run_preprocess_to_arrow_returns_dataframe(office365_csv_file, tmp_path):
    script_path = _write_preprocess_script(tmp_path, SKIP_TWO_LINES_SCRIPT)

    table = csv_service._run_preprocess_to_arrow(office365_csv_file, script_path)

    assert table.column_names == ["id", "name", "value"]
    assert table.num_rows == 2


def test_run_preprocess_to_arrow_raises_for_script_failure(sample_csv_file, tmp_path):
    script_path = _write_preprocess_script(
        tmp_path,
        "def preprocess(file):\n    raise ValueError('boom')\n",
    )

    with pytest.raises(RuntimeError, match="boom"):
        csv_service._run_preprocess_to_arrow(sample_csv_file, script_path)


def test_run_preprocess_to_arrow_raises_when_entrypoint_missing(sample_csv_file, tmp_path):
    script_path = _write_preprocess_script(tmp_path, "x = 1\n")

    with pytest.raises(RuntimeError, match="preprocess"):
        csv_service._run_preprocess_to_arrow(sample_csv_file, script_path)


def test_run_preprocess_to_arrow_raises_for_timeout(sample_csv_file, tmp_path, monkeypatch):
    monkeypatch.setattr(csv_service, "PREPROCESS_TIMEOUT_SECONDS", 0.01)
    script_path = _write_preprocess_script(
        tmp_path,
        "import time\ndef preprocess(file):\n    time.sleep(0.5)\n",
    )

    with pytest.raises(RuntimeError, match="timed out"):
        csv_service._run_preprocess_to_arrow(sample_csv_file, script_path)


def test_artifact_store_invalidate_deletes_temp_file(csv_artifact_store, office365_csv_file, tmp_path):
    preprocessing = {
        "enabled": True,
        "script_path": _write_preprocess_script(tmp_path, SKIP_TWO_LINES_SCRIPT),
    }

    csv_service.preview_preprocessed_csv_text(
        csv_artifact_store,
        "node-1",
        office365_csv_file,
        preprocessing,
    )
    fingerprint = csv_service.preprocessing_fingerprint(office365_csv_file, preprocessing)
    artifact_path = csv_artifact_store.get("node-1", fingerprint)
    assert artifact_path is not None

    deleted = csv_artifact_store.invalidate("node-1")

    assert deleted is True
    assert not Path(artifact_path).exists()
