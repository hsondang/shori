"""Excel sources, loaded natively by DuckDB's read_xlsx.

Only the modern OOXML formats (.xlsx/.xlsm) are supported — that's everything
read_xlsx can open. Sheet names are read straight out of the file's zip
(xl/workbook.xml) with the standard library, so there's no heavyweight Excel
dependency and no preview roundtrip; the actual load happens in DuckDB.
"""

import shutil
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

from fastapi import UploadFile

from app.config import UPLOAD_DIR

EXCEL_EXTENSIONS = {".xlsx", ".xlsm"}

_OOXML_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"


async def save_uploaded_excel(file: UploadFile) -> dict:
    if not file.filename:
        raise ValueError("Excel upload is missing a filename")
    _validate_extension(file.filename)

    dest = UPLOAD_DIR / file.filename
    with dest.open("wb") as handle:
        shutil.copyfileobj(file.file, handle)

    return {
        "file_path": str(dest),
        "filename": file.filename,
        "sheet_names": list_sheet_names(dest),
    }


def list_sheet_names(file_path: str | Path) -> list[str]:
    """Return the workbook's sheet names in tab order, parsed from the zip.

    An .xlsx/.xlsm is a zip whose xl/workbook.xml lists every sheet as
    `<sheet name="..."/>` in display order — no spreadsheet engine required.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Excel file '{file_path}' not found")
    _validate_extension(path.name)

    try:
        with zipfile.ZipFile(path) as archive:
            workbook_xml = archive.read("xl/workbook.xml")
    except (zipfile.BadZipFile, KeyError) as exc:
        raise ValueError(f"'{path.name}' is not a readable .xlsx/.xlsm workbook") from exc

    root = ET.fromstring(workbook_xml)
    names = [
        sheet.get("name", "")
        for sheet in root.iter(f"{{{_OOXML_MAIN_NS}}}sheet")
        if sheet.get("name")
    ]
    if not names:
        raise ValueError(f"No sheets found in '{path.name}'")
    return names


def _validate_extension(filename: str) -> None:
    suffix = Path(filename).suffix.lower()
    if suffix not in EXCEL_EXTENSIONS:
        raise ValueError(
            f"Unsupported Excel format '{suffix}'. Only {', '.join(sorted(EXCEL_EXTENSIONS))} are supported."
        )
