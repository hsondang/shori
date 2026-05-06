from fastapi import APIRouter, HTTPException, UploadFile

from app.models.pipeline import OracleConnectionConfig, PostgresConnectionConfig
from app.services.csv_service import save_uploaded_csv
from app.services.excel_service import EXCEL_EXTENSIONS, materialize_excel_sheet, save_uploaded_excel
from app.services.oracle_service import OracleService
from app.services.postgres_service import PostgresService
from pydantic import BaseModel

router = APIRouter()
oracle = OracleService()
postgres = PostgresService()


@router.post("/upload/csv")
async def upload_csv(file: UploadFile):
    if not file.filename or not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted")

    path = await save_uploaded_csv(file)
    return {"file_path": path, "filename": file.filename}


class ExcelSheetMaterializeRequest(BaseModel):
    file_path: str
    sheet_name: str


@router.post("/upload/excel")
async def upload_excel(file: UploadFile):
    suffix = "" if not file.filename else file.filename.lower().rsplit(".", 1)[-1]
    if not file.filename or f".{suffix}" not in EXCEL_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Only Excel workbook files are accepted")

    try:
        return await save_uploaded_excel(file)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Unable to read Excel workbook: {exc}") from exc


@router.post("/upload/excel/materialize-sheet")
async def materialize_excel_sheet_endpoint(payload: ExcelSheetMaterializeRequest):
    try:
        return materialize_excel_sheet(payload.file_path, payload.sheet_name)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/oracle/test-connection")
async def test_oracle_connection(config: OracleConnectionConfig):
    try:
        await oracle.test_connection(config.model_dump())
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/postgres/test-connection")
async def test_postgres_connection(config: PostgresConnectionConfig):
    try:
        await postgres.test_connection(config.model_dump())
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}
