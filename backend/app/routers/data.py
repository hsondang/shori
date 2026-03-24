import csv
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.config import EXPORT_DIR
from app.services.csv_service import (
    CSV_PREVIEW_LIMIT,
    preview_csv_text,
    preview_preprocessed_csv_text,
)

router = APIRouter()


class CsvSourcePreviewRequest(BaseModel):
    file_path: str
    limit: int = Field(default=CSV_PREVIEW_LIMIT, ge=1, le=CSV_PREVIEW_LIMIT)


class CsvSourcePreprocessedPreviewRequest(CsvSourcePreviewRequest):
    node_id: str
    preprocessing: dict


@router.post("/preview/csv-source")
def preview_csv_source(payload: CsvSourcePreviewRequest):
    if not Path(payload.file_path).exists():
        raise HTTPException(status_code=404, detail=f"CSV file '{payload.file_path}' not found")
    try:
        return preview_csv_text(payload.file_path, limit=payload.limit, stage="raw")
    except (csv.Error, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"Unable to preview CSV: {exc}") from exc


@router.post("/preview/csv-source/preprocessed")
def preview_preprocessed_csv_source(payload: CsvSourcePreprocessedPreviewRequest, request: Request):
    if not Path(payload.file_path).exists():
        raise HTTPException(status_code=404, detail=f"CSV file '{payload.file_path}' not found")
    try:
        return preview_preprocessed_csv_text(
            request.app.state.csv_preprocess_artifacts,
            payload.node_id,
            payload.file_path,
            payload.preprocessing,
            limit=payload.limit,
        )
    except (csv.Error, ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=f"Unable to preview CSV: {exc}") from exc


@router.delete("/preview/csv-source/preprocessed/{node_id}")
def delete_preprocessed_csv_source(node_id: str, request: Request):
    deleted = request.app.state.csv_preprocess_artifacts.invalidate(node_id)
    return {"deleted": deleted}


@router.get("/preview/{table_name}")
def preview_data(table_name: str, request: Request, offset: int = 0, limit: int = 100):
    db = request.app.state.duckdb
    if not db.table_exists(table_name):
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    return db.preview(table_name, offset=offset, limit=limit)


@router.get("/export/{table_name}")
def export_data(table_name: str, request: Request):
    db = request.app.state.duckdb
    if not db.table_exists(table_name):
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    output_path = EXPORT_DIR / f"{table_name}.csv"
    db.export_to_csv(table_name, str(output_path))
    return FileResponse(
        path=str(output_path),
        filename=f"{table_name}.csv",
        media_type="text/csv",
    )


@router.get("/schema/{table_name}")
def get_schema(table_name: str, request: Request):
    db = request.app.state.duckdb
    if not db.table_exists(table_name):
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    preview = db.preview(table_name, offset=0, limit=0)
    return {
        "table_name": table_name,
        "columns": preview["columns"],
        "column_types": preview["column_types"],
        "total_rows": preview["total_rows"],
    }


@router.delete("/table/{table_name}")
def delete_table(table_name: str, request: Request):
    db = request.app.state.duckdb
    existed = db.table_exists(table_name)
    db.drop_table(table_name)
    return {"deleted": existed}
