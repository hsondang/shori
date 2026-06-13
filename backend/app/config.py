import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
EXPORT_DIR = DATA_DIR / "exports"
PIPELINE_DIR = DATA_DIR / "pipelines"
PROJECT_DB_PATH = DATA_DIR / "projects.sqlite3"
PROJECTS_DIR = DATA_DIR / "projects"

for d in [UPLOAD_DIR, EXPORT_DIR, PIPELINE_DIR, PROJECTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

_PROJECT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def validate_project_id(project_id: str) -> str:
    # Project ids become directory names; reject anything that could escape PROJECTS_DIR.
    if not isinstance(project_id, str) or not _PROJECT_ID_PATTERN.fullmatch(project_id) or ".." in project_id:
        raise ValueError(f"Invalid project id: {project_id!r}")
    return project_id


def project_data_dir(project_id: str) -> Path:
    return PROJECTS_DIR / validate_project_id(project_id)


def project_duckdb_path(project_id: str) -> Path:
    return project_data_dir(project_id) / "project.duckdb"
