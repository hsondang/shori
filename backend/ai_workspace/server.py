"""FastAPI sub-app for the AI workspace.

Mounted at /ai by app/main.py, so every route here is served under /ai/api/*.
The MCP shim (shori_mcp.py) and the workspace UI are the only intended clients.

Phase 1 scope (docs/ai-workspace-model.md §8): project identity, inbox sweep
(clone-by-export ingestion), table listing and schema. The permissions stub
remains until Phase 2 reshapes it into workspace state.
"""

import os
import re
import sqlite3
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request

from ai_workspace.workspace import AIWorkspaceRegistry

# Matches app/config.py's project-id rule by value, not by import: ids become
# directory names, so anything else is rejected before touching the filesystem.
_SAFE_PROJECT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _default_data_dir() -> Path:
    env = os.environ.get("SHORI_DATA_DIR")
    if env:
        return Path(env)
    # backend/ai_workspace/server.py -> backend -> repo root -> data/
    return Path(__file__).resolve().parent.parent.parent / "data"


def _read_projects(projects_db: Path) -> list[dict]:
    """Read project identity (id, name) from the main app's SQLite store.

    Read-only by URI so this package can never write the main app's store.
    A missing store simply means no projects exist yet.
    """
    if not projects_db.exists():
        return []
    conn = sqlite3.connect(f"file:{projects_db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT id, name FROM projects ORDER BY updated_at DESC"
        ).fetchall()
    finally:
        conn.close()
    return [{"id": row[0], "name": row[1]} for row in rows]


def build_ai_app(data_dir: Path | None = None) -> FastAPI:
    data_dir = data_dir or _default_data_dir()
    projects_db = data_dir / "projects.sqlite3"

    ai_app = FastAPI(title="Shori AI Workspace", version="0.1.0")
    ai_app.state.data_dir = data_dir
    ai_app.state.projects_db = projects_db
    ai_app.state.workspaces = AIWorkspaceRegistry(data_dir)

    def _find_project(request: Request, project_id: str) -> dict:
        if _SAFE_PROJECT_ID.fullmatch(project_id):
            for project in _read_projects(request.app.state.projects_db):
                if project["id"] == project_id:
                    return project
        raise HTTPException(status_code=404, detail=f"Unknown project: {project_id}")

    def _workspace(request: Request, project_id: str):
        _find_project(request, project_id)
        workspace = request.app.state.workspaces.get(project_id)
        # Lazy ingestion: any read of the workspace first drains the spool.
        workspace.sweep_inbox()
        return workspace

    @ai_app.get("/api/health")
    def health():
        return {"status": "ok", "component": "ai_workspace"}

    @ai_app.get("/api/projects")
    def list_projects(request: Request):
        registry = request.app.state.workspaces
        projects = _read_projects(request.app.state.projects_db)
        for project in projects:
            has_workspace = bool(
                _SAFE_PROJECT_ID.fullmatch(project["id"])
                and registry.get(project["id"]).exists()
            )
            project["has_ai_workspace"] = has_workspace
        return projects

    @ai_app.get("/api/projects/{project_id}/tables")
    def list_tables(project_id: str, request: Request):
        return _workspace(request, project_id).list_tables()

    @ai_app.get("/api/projects/{project_id}/tables/{table_name}/schema")
    def get_table_schema(project_id: str, table_name: str, request: Request):
        workspace = _workspace(request, project_id)
        try:
            return workspace.get_table_schema(table_name)
        except KeyError:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Table '{table_name}' is not in this project's AI workspace. "
                    "The user can export it there via an export node in the main app."
                ),
            ) from None

    @ai_app.get("/api/projects/{project_id}/permissions")
    def get_permissions(project_id: str, request: Request):
        project = _find_project(request, project_id)
        # Phase 1 stub: becomes get_workspace_state in Phase 2 (editor status,
        # toggles, pending requests). Table visibility needs no gate — the
        # clone boundary is the consent (docs/ai-workspace-model.md §4).
        return {
            "project_id": project["id"],
            "project_name": project["name"],
            "tables": [],
            "note": (
                "Consent model v2: cloned tables are always visible (schema + drafting). "
                "Execution and result visibility controls arrive in later phases."
            ),
        }

    return ai_app
