import shutil
from pathlib import Path

from fastapi import UploadFile

from app.config import UPLOAD_DIR


async def save_uploaded_csv(file: UploadFile) -> str:
    dest = UPLOAD_DIR / file.filename
    with dest.open("wb") as f:
        shutil.copyfileobj(file.file, f)
    return str(dest)
