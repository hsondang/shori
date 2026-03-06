from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
EXPORT_DIR = DATA_DIR / "exports"
PIPELINE_DIR = DATA_DIR / "pipelines"

for d in [UPLOAD_DIR, EXPORT_DIR, PIPELINE_DIR]:
    d.mkdir(parents=True, exist_ok=True)
