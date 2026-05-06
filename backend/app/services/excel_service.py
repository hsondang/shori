import csv
import re
import shutil
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from uuid import uuid4

from fastapi import UploadFile
from python_calamine import CalamineWorkbook, WorksheetNotFound

from app.config import UPLOAD_DIR

EXCEL_PREVIEW_ROW_LIMIT = 10
EXCEL_PREVIEW_COLUMN_LIMIT = 12
EXCEL_EXTENSIONS = {".xls", ".xlsx", ".xlsm", ".xlsb", ".ods"}


@dataclass
class ExcelSheetPreview:
    name: str
    rows: list[list[str]]
    truncated_rows: bool
    truncated_columns: bool


async def save_uploaded_excel(file: UploadFile) -> dict:
    if not file.filename:
        raise ValueError("Excel upload is missing a filename")

    dest = UPLOAD_DIR / file.filename
    with dest.open("wb") as handle:
        shutil.copyfileobj(file.file, handle)

    workbook = _open_workbook(dest)
    sheet_names = list(workbook.sheet_names)
    previews = [_preview_sheet(workbook, sheet_name) for sheet_name in sheet_names]

    return {
        "file_path": str(dest),
        "filename": file.filename,
        "sheet_names": sheet_names,
        "sheets": [
            {
                "name": preview.name,
                "rows": preview.rows,
                "truncated_rows": preview.truncated_rows,
                "truncated_columns": preview.truncated_columns,
            }
            for preview in previews
        ],
    }


def materialize_excel_sheet(file_path: str, sheet_name: str) -> dict:
    source_path = Path(file_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Excel file '{file_path}' not found")
    if not sheet_name.strip():
        raise ValueError("sheet_name is required")

    workbook = _open_workbook(source_path)
    if sheet_name not in workbook.sheet_names:
        raise ValueError(f"Sheet '{sheet_name}' was not found in the workbook")

    sheet = workbook.get_sheet_by_name(sheet_name)
    rows = sheet.to_python(skip_empty_area=False)
    output_path = _materialized_csv_path(source_path, sheet_name)

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        for row in rows:
            writer.writerow([_cell_to_text(cell) for cell in row])

    return {
        "file_path": str(output_path),
        "filename": output_path.name,
        "sheet_name": sheet_name,
    }


def _open_workbook(path: Path):
    try:
        return CalamineWorkbook.from_path(str(path))
    except AttributeError:
        return CalamineWorkbook.from_object(path)


def _preview_sheet(workbook, sheet_name: str) -> ExcelSheetPreview:
    try:
        sheet = workbook.get_sheet_by_name(sheet_name)
    except WorksheetNotFound:
        return ExcelSheetPreview(name=sheet_name, rows=[], truncated_rows=False, truncated_columns=False)

    rows = sheet.to_python(skip_empty_area=False)
    preview_rows = rows[:EXCEL_PREVIEW_ROW_LIMIT]
    truncated_columns = any(len(row) > EXCEL_PREVIEW_COLUMN_LIMIT for row in preview_rows)

    return ExcelSheetPreview(
        name=sheet_name,
        rows=[
            [_cell_to_text(cell) for cell in row[:EXCEL_PREVIEW_COLUMN_LIMIT]]
            for row in preview_rows
        ],
        truncated_rows=len(rows) > EXCEL_PREVIEW_ROW_LIMIT,
        truncated_columns=truncated_columns,
    )


def _cell_to_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return str(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _materialized_csv_path(source_path: Path, sheet_name: str) -> Path:
    source_stem = _safe_filename_part(source_path.stem) or "workbook"
    sheet_stem = _safe_filename_part(sheet_name) or "sheet"
    return UPLOAD_DIR / f"{source_stem}_{sheet_stem}_{uuid4().hex[:8]}.csv"


def _safe_filename_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._")
