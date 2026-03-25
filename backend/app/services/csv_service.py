import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Mapping
import threading

from fastapi import UploadFile

from app.config import UPLOAD_DIR

PREPROCESS_TIMEOUT_SECONDS = 60
CSV_PREVIEW_LIMIT = 100
CSV_PREVIEW_SAMPLE_SIZE = 4096
CSV_PREVIEW_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "iso-8859-1")


@dataclass
class PreprocessedCsvArtifact:
    fingerprint: str
    path: str


class CsvPreprocessArtifactStore:
    def __init__(self):
        self._artifacts: dict[str, PreprocessedCsvArtifact] = {}
        self._lock = threading.Lock()

    def store(self, node_id: str, fingerprint: str, path: str) -> None:
        with self._lock:
            self._delete_unlocked(node_id)
            self._artifacts[node_id] = PreprocessedCsvArtifact(
                fingerprint=fingerprint,
                path=path,
            )

    def get(self, node_id: str, fingerprint: str) -> str | None:
        with self._lock:
            artifact = self._artifacts.get(node_id)
            if artifact is None:
                return None
            if artifact.fingerprint != fingerprint:
                self._delete_unlocked(node_id)
                return None
            if not Path(artifact.path).exists():
                self._delete_unlocked(node_id)
                return None
            return artifact.path

    def invalidate(self, node_id: str) -> bool:
        with self._lock:
            return self._delete_unlocked(node_id)

    def close(self) -> None:
        with self._lock:
            for node_id in list(self._artifacts.keys()):
                self._delete_unlocked(node_id)

    def _delete_unlocked(self, node_id: str) -> bool:
        artifact = self._artifacts.pop(node_id, None)
        if artifact is None:
            return False
        Path(artifact.path).unlink(missing_ok=True)
        return True


async def save_uploaded_csv(file: UploadFile) -> str:
    dest = UPLOAD_DIR / file.filename
    with dest.open("wb") as f:
        shutil.copyfileobj(file.file, f)
    return str(dest)


def preview_csv_text(
    file_path: str,
    limit: int = CSV_PREVIEW_LIMIT,
    *,
    stage: str = "raw",
    artifact_ready: bool = False,
) -> dict:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file '{file_path}' not found")

    rows, truncated = _read_preview_rows(path, limit)

    return {
        "kind": "csv_text",
        "csv_stage": stage,
        "rows": rows,
        "limit": limit,
        "truncated": truncated,
        "artifact_ready": artifact_ready,
    }


def preview_preprocessed_csv_text(
    artifact_store: CsvPreprocessArtifactStore,
    node_id: str,
    file_path: str,
    preprocessing: object | None,
    limit: int = CSV_PREVIEW_LIMIT,
) -> dict:
    config = _normalize_preprocessing(preprocessing)
    if config is None:
        raise ValueError("Preprocessing must be enabled before running Preprocess")

    processed_path = _run_preprocessing_to_temp_file(file_path, config["runtime"], config["script"])
    fingerprint = preprocessing_fingerprint(file_path, preprocessing)
    if fingerprint is None:
        raise ValueError("Unable to fingerprint preprocessing configuration")

    artifact_store.store(node_id, fingerprint, processed_path)
    return preview_csv_text(
        processed_path,
        limit=limit,
        stage="preprocessed",
        artifact_ready=True,
    )


def register_csv_source(
    duckdb,
    node_id: str,
    table_name: str,
    config: Mapping[str, object],
    artifact_store: CsvPreprocessArtifactStore,
) -> dict:
    file_path = _materialization_file_path(config)
    preprocessing = config.get("preprocessing")
    normalized = _normalize_preprocessing(preprocessing)

    if normalized is None:
        return duckdb.register_csv(table_name, file_path)

    fingerprint = preprocessing_fingerprint(file_path, preprocessing)
    if fingerprint is None:
        raise RuntimeError("Unable to validate preprocessing configuration")

    artifact_path = artifact_store.get(node_id, fingerprint)
    if artifact_path is None:
        raise RuntimeError(
            "Preprocessing is enabled for this CSV source. Click Preprocess and review the output before loading data."
        )

    return duckdb.register_csv(table_name, artifact_path)


@contextmanager
def prepared_csv_path(
    file_path: str,
    preprocessing: object | None,
) -> Iterator[str]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file '{file_path}' not found")

    config = _normalize_preprocessing(preprocessing)
    if config is None:
        yield str(path)
        return

    fd, temp_path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)

    try:
        Path(temp_path).write_bytes(
            _run_preprocessing_output(str(path), config["runtime"], config["script"])
        )
        yield temp_path
    finally:
        Path(temp_path).unlink(missing_ok=True)


def preprocessing_fingerprint(file_path: str, preprocessing: object | None) -> str | None:
    config = _normalize_preprocessing(preprocessing)
    if config is None:
        return None

    payload = json.dumps(
        {
            "file_path": file_path,
            "runtime": config["runtime"],
            "script": config["script"],
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _read_preview_rows(path: Path, limit: int) -> tuple[list[list[str]], bool]:
    last_error: csv.Error | ValueError | None = None

    for encoding in CSV_PREVIEW_ENCODINGS:
        try:
            with path.open("r", encoding=encoding, newline="") as handle:
                sample = handle.read(CSV_PREVIEW_SAMPLE_SIZE)
                dialects: list[type[csv.Dialect] | csv.Dialect] = [csv.excel]

                try:
                    dialects.insert(0, csv.Sniffer().sniff(sample))
                except csv.Error:
                    pass

                for dialect in dialects:
                    try:
                        return _collect_preview_rows(handle, dialect, limit)
                    except (csv.Error, ValueError) as exc:
                        last_error = exc
                        continue
        except UnicodeDecodeError:
            continue

    if last_error is not None:
        raise last_error

    raise ValueError(
        "CSV file could not be decoded using the supported encodings: "
        + ", ".join(CSV_PREVIEW_ENCODINGS)
    )


def _collect_preview_rows(
    handle,
    dialect: type[csv.Dialect] | csv.Dialect,
    limit: int,
) -> tuple[list[list[str]], bool]:
    handle.seek(0)
    reader = csv.reader(handle, dialect)
    rows: list[list[str]] = []
    truncated = False
    for index, row in enumerate(reader):
        if index >= limit:
            truncated = True
            break
        rows.append(row)
    return rows, truncated


def _materialization_file_path(config: Mapping[str, object]) -> str:
    file_path = str(config.get("file_path", "")).strip()
    if not file_path:
        raise ValueError("CSV source is missing a file_path")
    return file_path


def _normalize_preprocessing(preprocessing: object | None) -> dict[str, str] | None:
    if not isinstance(preprocessing, Mapping):
        return None

    enabled = bool(preprocessing.get("enabled"))
    if not enabled:
        return None

    runtime = str(preprocessing.get("runtime", "")).strip()
    script = str(preprocessing.get("script", "")).strip()

    if runtime not in {"python", "bash"}:
        raise ValueError("Preprocessing runtime must be either 'python' or 'bash'")
    if not script:
        raise ValueError("Preprocessing is enabled but no script was provided")

    return {"runtime": runtime, "script": script}


def _run_preprocessing_to_temp_file(file_path: str, runtime: str, script: str) -> str:
    fd, temp_path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    Path(temp_path).write_bytes(_run_preprocessing_output(file_path, runtime, script))
    return temp_path


def _run_preprocessing_output(file_path: str, runtime: str, script: str) -> bytes:
    result = _run_preprocessing_script(file_path, runtime, script)
    stdout = result.stdout
    if not stdout:
        raise RuntimeError("Preprocessing script did not emit any CSV content to stdout")
    return stdout


def _run_preprocessing_script(file_path: str, runtime: str, script: str) -> subprocess.CompletedProcess[bytes]:
    env = os.environ.copy()
    env["SHORI_INPUT_CSV"] = file_path

    if runtime == "python":
        command = [sys.executable, "-c", script, file_path]
    else:
        command = ["bash", "-c", script, "shori_csv", file_path]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            env=env,
            timeout=PREPROCESS_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"Preprocessing script timed out after {PREPROCESS_TIMEOUT_SECONDS} seconds"
        ) from exc

    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        if stderr:
            raise RuntimeError(
                f"Preprocessing script failed with exit code {result.returncode}: {stderr}"
            )
        raise RuntimeError(
            f"Preprocessing script failed with exit code {result.returncode}"
        )

    return result
