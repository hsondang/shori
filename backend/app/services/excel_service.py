"""Excel sources, loaded natively by DuckDB's read_xlsx.

Only the modern OOXML formats (.xlsx/.xlsm) are supported — that's everything
read_xlsx can open. Sheet names are read straight out of the file's zip
(xl/workbook.xml) with the standard library, so there's no heavyweight Excel
dependency and no preview roundtrip; the actual load happens in DuckDB.
"""

import posixpath
import re
import shutil
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

from fastapi import UploadFile

from app.config import UPLOAD_DIR

EXCEL_EXTENSIONS = {".xlsx", ".xlsm"}

_OOXML_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_OOXML_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"

_A1_RANGE = re.compile(r"^([A-Z]+)([0-9]+)(?::([A-Z]+)([0-9]+))?$")


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
        "sheet_dimensions": read_sheet_dimensions(dest),
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


def read_sheet_dimensions(file_path: str | Path) -> dict[str, dict | None]:
    """Best-effort {sheet name: {"rows": n, "cols": n} | None}, from the zip only.

    Each worksheet part *may* carry a `<dimension ref="A1:L1204"/>` before its
    data; some writers omit it or leave it stale, so every failure degrades to
    None for that sheet. Streams stop at the first `dimension` or at
    `sheetData` — deliberately never a full parse (docs/excel-node-model.md §4).
    """
    path = Path(file_path)
    try:
        with zipfile.ZipFile(path) as archive:
            targets = _worksheet_targets(archive)
            return {
                sheet: _read_dimension(archive, target)
                for sheet, target in targets.items()
            }
    except Exception:
        return {}


def _worksheet_targets(archive: zipfile.ZipFile) -> dict[str, str]:
    """Sheet name → worksheet zip member, resolved via the workbook rels."""
    rels_by_id: dict[str, str] = {}
    rels_root = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    for rel in rels_root.iter(f"{{{_PKG_REL_NS}}}Relationship"):
        rel_id, target = rel.get("Id"), rel.get("Target")
        if rel_id and target:
            member = target.lstrip("/")
            if not member.startswith("xl/"):
                member = posixpath.normpath(posixpath.join("xl", member))
            rels_by_id[rel_id] = member

    targets: dict[str, str] = {}
    workbook_root = ET.fromstring(archive.read("xl/workbook.xml"))
    for sheet in workbook_root.iter(f"{{{_OOXML_MAIN_NS}}}sheet"):
        name = sheet.get("name")
        rel_id = sheet.get(f"{{{_OOXML_REL_NS}}}id")
        if name and rel_id and rel_id in rels_by_id:
            targets[name] = rels_by_id[rel_id]
    return targets


def _read_dimension(archive: zipfile.ZipFile, member: str) -> dict | None:
    try:
        with archive.open(member) as stream:
            for _, element in ET.iterparse(stream, events=("start",)):
                tag = element.tag
                if tag == f"{{{_OOXML_MAIN_NS}}}dimension":
                    return _parse_a1_range(element.get("ref", ""))
                if tag == f"{{{_OOXML_MAIN_NS}}}sheetData":
                    return None
    except Exception:
        return None
    return None


def _parse_a1_range(ref: str) -> dict | None:
    match = _A1_RANGE.match(ref.strip().upper())
    if not match:
        return None
    start_col, start_row, end_col, end_row = match.groups()
    if end_col is None:  # single-cell ref like "A1" — no real extent recorded
        return {"rows": 1, "cols": 1}
    rows = int(end_row) - int(start_row) + 1
    cols = _column_index(end_col) - _column_index(start_col) + 1
    if rows <= 0 or cols <= 0:
        return None
    return {"rows": rows, "cols": cols}


def _column_index(column: str) -> int:
    index = 0
    for char in column:
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index


def _validate_extension(filename: str) -> None:
    suffix = Path(filename).suffix.lower()
    if suffix not in EXCEL_EXTENSIONS:
        raise ValueError(
            f"Unsupported Excel format '{suffix}'. Only {', '.join(sorted(EXCEL_EXTENSIONS))} are supported."
        )
