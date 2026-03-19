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


def test_preview_preprocessed_csv_text_stores_artifact(csv_artifact_store, office365_csv_file):
    preprocessing = {
        "enabled": True,
        "runtime": "python",
        "script": "import sys; from pathlib import Path; lines = Path(sys.argv[1]).read_text().splitlines()[2:]; sys.stdout.write('\\n'.join(lines))",
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


def test_prepared_csv_path_runs_python_preprocessing(office365_csv_file):
    preprocessing = {
        "enabled": True,
        "runtime": "python",
        "script": "import sys; from pathlib import Path; lines = Path(sys.argv[1]).read_text().splitlines()[2:]; sys.stdout.write('\\n'.join(lines))",
    }

    with csv_service.prepared_csv_path(office365_csv_file, preprocessing) as processed_path:
        processed = Path(processed_path)
        assert processed.exists()
        assert processed.read_text().splitlines() == [
            "id,name,value",
            "1,Alice,10.5",
            "2,Bob,20.0",
        ]

    assert not processed.exists()


def test_prepared_csv_path_runs_bash_preprocessing(office365_csv_file):
    preprocessing = {
        "enabled": True,
        "runtime": "bash",
        "script": "tail -n +3 \"$1\"",
    }

    with csv_service.prepared_csv_path(office365_csv_file, preprocessing) as processed_path:
        assert Path(processed_path).read_text().splitlines()[0] == "id,name,value"


def test_prepared_csv_path_raises_for_script_failure(sample_csv_file):
    preprocessing = {
        "enabled": True,
        "runtime": "python",
        "script": "import sys; sys.stderr.write('boom'); raise SystemExit(2)",
    }

    with pytest.raises(RuntimeError, match="boom"):
        with csv_service.prepared_csv_path(sample_csv_file, preprocessing):
            pass


def test_prepared_csv_path_raises_for_timeout(sample_csv_file, monkeypatch):
    monkeypatch.setattr(csv_service, "PREPROCESS_TIMEOUT_SECONDS", 0.01)
    preprocessing = {
        "enabled": True,
        "runtime": "python",
        "script": "import time; time.sleep(0.1)",
    }

    with pytest.raises(RuntimeError, match="timed out"):
        with csv_service.prepared_csv_path(sample_csv_file, preprocessing):
            pass


def test_artifact_store_invalidate_deletes_temp_file(csv_artifact_store, office365_csv_file):
    preprocessing = {
        "enabled": True,
        "runtime": "bash",
        "script": "tail -n +3 \"$1\"",
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
