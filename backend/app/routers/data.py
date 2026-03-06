from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse

from app.config import EXPORT_DIR

router = APIRouter()


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
