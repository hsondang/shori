import csv
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
import threading

import pyarrow as pa
import pyarrow.ipc
import pyarrow.parquet
from fastapi import UploadFile

from app.config import UPLOAD_DIR

PREPROCESS_TIMEOUT_SECONDS = 60
PREPROCESS_ENTRYPOINT = "preprocess"
CSV_PREVIEW_LIMIT = 100
CSV_PREVIEW_SAMPLE_SIZE = 4096
CSV_PREVIEW_SAMPLE_ROWS = 25
CSV_PREVIEW_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "iso-8859-1")
CSV_PREVIEW_DELIMITERS = (",", ";", "\t", "|")


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

    table = _run_preprocess_to_arrow(file_path, config["script_path"])
    fingerprint = preprocessing_fingerprint(file_path, preprocessing)
    if fingerprint is None:
        raise ValueError("Unable to fingerprint preprocessing configuration")

    # Cache the result as a typed Parquet artifact so the later load skips re-running.
    parquet_path = _write_parquet_artifact(table)
    artifact_store.store(node_id, fingerprint, parquet_path)
    return _arrow_table_preview(table, limit)


def register_csv_source(
    duckdb,
    node_id: str,
    table_name: str,
    config: Mapping[str, object],
    artifact_store: CsvPreprocessArtifactStore,
    cache_key: str | None = None,
    into_memory: bool = False,
) -> dict:
    file_path = _materialization_file_path(config)
    preprocessing = config.get("preprocessing")
    normalized = _normalize_preprocessing(preprocessing)

    if normalized is None:
        return duckdb.register_csv(
            table_name, file_path, node_id=node_id, cache_key=cache_key, into_memory=into_memory
        )

    fingerprint = preprocessing_fingerprint(file_path, preprocessing)
    if fingerprint is None:
        raise RuntimeError("Unable to validate preprocessing configuration")

    artifact_path = artifact_store.get(node_id, fingerprint)
    if artifact_path is None:
        raise RuntimeError(
            "Preprocessing is enabled for this CSV source. Click Preprocess and review the output before loading data."
        )

    return duckdb.register_parquet(
        table_name, artifact_path, node_id=node_id, cache_key=cache_key, into_memory=into_memory
    )


def preprocessing_fingerprint(file_path: str, preprocessing: object | None) -> str | None:
    config = _normalize_preprocessing(preprocessing)
    if config is None:
        return None

    script_path = config["script_path"]
    payload = json.dumps(
        {
            "file_path": file_path,
            "script_path": script_path,
            # Hash the script body so editing the .py invalidates a cached artifact.
            "script_sha256": _file_sha256(script_path),
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
                dialects = _rank_preview_dialects(sample)

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


def _rank_preview_dialects(sample: str) -> list[csv.Dialect]:
    candidates: list[str] = []
    seen_delimiters: set[str] = set()

    sniffed_delimiter = _sniff_preview_delimiter(sample)
    if sniffed_delimiter is not None:
        candidates.append(sniffed_delimiter)
        seen_delimiters.add(sniffed_delimiter)

    for delimiter in CSV_PREVIEW_DELIMITERS:
        if delimiter in seen_delimiters:
            continue
        candidates.append(delimiter)

    scores = {delimiter: _score_preview_delimiter(sample, delimiter) for delimiter in candidates}
    ranked = sorted(
        enumerate(candidates),
        key=lambda item: (scores[item[1]], -item[0]),
        reverse=True,
    )

    ordered_delimiters = [candidates[index] for index, _ in ranked]
    best_score = scores[ordered_delimiters[0]]
    if best_score == (0, 0, 0):
        ordered_delimiters = list(CSV_PREVIEW_DELIMITERS)

    return [_build_preview_dialect(delimiter) for delimiter in ordered_delimiters]


def _sniff_preview_delimiter(sample: str) -> str | None:
    if not sample:
        return None

    try:
        sniffed = csv.Sniffer().sniff(sample, delimiters="".join(CSV_PREVIEW_DELIMITERS))
    except csv.Error:
        return None

    delimiter = getattr(sniffed, "delimiter", None)
    if delimiter not in CSV_PREVIEW_DELIMITERS:
        return None
    return delimiter


def _score_preview_delimiter(sample: str, delimiter: str) -> tuple[int, int, int]:
    if not sample:
        return (0, 0, 0)

    try:
        rows, _ = _collect_preview_rows(
            io.StringIO(sample),
            _build_preview_dialect(delimiter),
            CSV_PREVIEW_SAMPLE_ROWS,
        )
    except (csv.Error, ValueError):
        return (0, 0, 0)

    widths = [len(row) for row in rows if any(cell != "" for cell in row)]
    multi_column_widths = [width for width in widths if width > 1]
    if not multi_column_widths:
        return (0, 0, 0)

    width_counts = Counter(multi_column_widths)
    dominant_width, dominant_count = max(
        width_counts.items(),
        key=lambda item: (item[1], item[0]),
    )
    return (dominant_count, len(multi_column_widths), dominant_width)


def _build_preview_dialect(delimiter: str) -> csv.Dialect:
    return type(
        "PreviewDialect",
        (csv.Dialect,),
        {
            "delimiter": delimiter,
            "quotechar": '"',
            "doublequote": True,
            "escapechar": None,
            "skipinitialspace": False,
            "lineterminator": "\r\n",
            "quoting": csv.QUOTE_MINIMAL,
        },
    )()


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

    script_path = str(preprocessing.get("script_path", "")).strip()
    if not script_path:
        raise ValueError("Preprocessing is enabled but no script_path was provided")
    if not Path(script_path).is_file():
        raise ValueError(f"Preprocessing script '{script_path}' was not found")

    return {"script_path": script_path}


def _file_sha256(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _write_parquet_artifact(table: "pa.Table") -> str:
    fd, temp_path = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    pa.parquet.write_table(table, temp_path)  # type: ignore[attr-defined]
    return temp_path


def _arrow_table_preview(table: "pa.Table", limit: int) -> dict:
    truncated = table.num_rows > limit
    head = table.slice(0, limit).to_pylist()
    columns = table.column_names
    rows: list[list[str]] = [list(columns)]
    for record in head:
        rows.append(["" if record[c] is None else str(record[c]) for c in columns])
    return {
        "kind": "csv_text",
        "csv_stage": "preprocessed",
        "rows": rows,
        "limit": limit,
        "truncated": truncated,
        "artifact_ready": True,
    }


# Subprocess runner: load the user's .py module, call preprocess(file) with an
# open text handle to the source, and stream the returned DataFrame back as an
# Arrow IPC stream on stdout. Isolating it in a child process keeps user code
# from crashing or leaking into the API process.
_PREPROCESS_RUNNER = """
import runpy, sys
import pandas as pd
import pyarrow as pa

script_path, input_path = sys.argv[1], sys.argv[2]
namespace = runpy.run_path(script_path)
fn = namespace.get("preprocess")
if not callable(fn):
    sys.stderr.write("Preprocessing script must define a callable 'preprocess(file)'.")
    raise SystemExit(2)
with open(input_path, "r") as handle:
    result = fn(handle)
if isinstance(result, pd.DataFrame):
    table = pa.Table.from_pandas(result, preserve_index=False)
elif isinstance(result, pa.Table):
    table = result
else:
    sys.stderr.write("preprocess(file) must return a pandas.DataFrame.")
    raise SystemExit(3)
with pa.ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)
"""


def _run_preprocess_to_arrow(file_path: str, script_path: str) -> "pa.Table":
    if not Path(file_path).exists():
        raise FileNotFoundError(f"CSV file '{file_path}' not found")
    if not Path(script_path).is_file():
        raise ValueError(f"Preprocessing script '{script_path}' was not found")

    try:
        result = subprocess.run(
            [sys.executable, "-c", _PREPROCESS_RUNNER, script_path, file_path],
            capture_output=True,
            timeout=PREPROCESS_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"Preprocessing script timed out after {PREPROCESS_TIMEOUT_SECONDS} seconds"
        ) from exc

    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        detail = f": {stderr}" if stderr else ""
        raise RuntimeError(
            f"Preprocessing script failed with exit code {result.returncode}{detail}"
        )
    if not result.stdout:
        raise RuntimeError("Preprocessing script did not emit any data")

    reader = pa.ipc.open_stream(pa.BufferReader(result.stdout))
    return reader.read_all()
