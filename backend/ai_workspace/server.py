"""FastAPI sub-app for the AI workspace.

Mounted at /ai by app/main.py, so API routes are served under /ai/api/* and the
workspace UI at /ai/<project_id>. The MCP shim (shori_mcp.py) and that UI are
the only intended clients.

Phase 2 scope (docs/ai-workspace-model.md §8): the shared editor (agent writes
stage as drafts when the user has local edits), bind-only SQL validation,
workspace state, the activity feed, and the static UI page. Agent-originated
calls (X-Shori-Client: mcp) are audited into the per-project activity log.
"""

import os
import re
import sqlite3
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from ai_workspace.workspace import AIWorkspace, AIWorkspaceRegistry

# Matches app/config.py's project-id rule by value, not by import: ids become
# directory names, so anything else is rejected before touching the filesystem.
_SAFE_PROJECT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

_MAX_SQL_LENGTH = 100_000

_STATIC_DIR = Path(__file__).resolve().parent / "static"


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


class EditorWrite(BaseModel):
    sql: str = Field(max_length=_MAX_SQL_LENGTH)


class AgentEditorWrite(BaseModel):
    sql: str = Field(max_length=_MAX_SQL_LENGTH)
    note: str | None = Field(default=None, max_length=2000)


class ValidateRequest(BaseModel):
    sql: str = Field(max_length=_MAX_SQL_LENGTH)


class SettingsUpdate(BaseModel):
    autonomous_execute: bool | None = None
    auto_share_results: bool | None = None


def build_ai_app(data_dir: Path | None = None) -> FastAPI:
    data_dir = data_dir or _default_data_dir()
    projects_db = data_dir / "projects.sqlite3"

    ai_app = FastAPI(title="Shori AI Workspace", version="0.2.0")
    ai_app.state.data_dir = data_dir
    ai_app.state.projects_db = projects_db
    ai_app.state.workspaces = AIWorkspaceRegistry(data_dir)

    def _find_project(request: Request, project_id: str) -> dict:
        if _SAFE_PROJECT_ID.fullmatch(project_id):
            for project in _read_projects(request.app.state.projects_db):
                if project["id"] == project_id:
                    return project
        raise HTTPException(status_code=404, detail=f"Unknown project: {project_id}")

    def _workspace(request: Request, project_id: str) -> AIWorkspace:
        _find_project(request, project_id)
        workspace = request.app.state.workspaces.get(project_id)
        # Lazy ingestion: any read of the workspace first drains the spool.
        workspace.sweep_inbox()
        return workspace

    def _is_agent(request: Request) -> bool:
        return request.headers.get("x-shori-client") == "mcp"

    def _audit(request: Request, workspace: AIWorkspace, tool: str, detail: dict,
               decision: str = "allowed") -> None:
        # Every agent-originated call lands in the activity feed (§4). UI and
        # internal calls are not agent actions and are not logged here.
        if _is_agent(request):
            workspace.log_activity(tool, detail, decision)

    # -- health / projects -----------------------------------------------------

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

    # -- tables / schema ---------------------------------------------------------

    @ai_app.get("/api/projects/{project_id}/tables")
    def list_tables(project_id: str, request: Request):
        workspace = _workspace(request, project_id)
        tables = workspace.list_tables()
        _audit(request, workspace, "list_tables", {"count": len(tables)})
        return tables

    @ai_app.get("/api/projects/{project_id}/tables/{table_name}/schema")
    def get_table_schema(project_id: str, table_name: str, request: Request):
        workspace = _workspace(request, project_id)
        try:
            schema = workspace.get_table_schema(table_name)
        except KeyError:
            _audit(request, workspace, "get_table_schema", {"table": table_name}, "error")
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Table '{table_name}' is not in this project's AI workspace. "
                    "The user can export it there via an export node in the main app."
                ),
            ) from None
        _audit(request, workspace, "get_table_schema", {"table": table_name})
        return schema

    # -- shared editor -----------------------------------------------------------

    @ai_app.get("/api/projects/{project_id}/editor")
    def get_editor(project_id: str, request: Request):
        workspace = _workspace(request, project_id)
        editor = workspace.get_editor()
        _audit(request, workspace, "read_editor", {"last_editor": editor["last_editor"]})
        return editor

    @ai_app.put("/api/projects/{project_id}/editor")
    def put_editor(project_id: str, payload: EditorWrite, request: Request):
        if _is_agent(request):
            raise HTTPException(
                status_code=403,
                detail="Agents must use the agent-write path (shori_write_editor).",
            )
        return _workspace(request, project_id).user_set_editor(payload.sql)

    @ai_app.post("/api/projects/{project_id}/editor/agent-write")
    def agent_write_editor(project_id: str, payload: AgentEditorWrite, request: Request):
        workspace = _workspace(request, project_id)
        result = workspace.agent_write_editor(payload.sql, payload.note)
        _audit(
            request,
            workspace,
            "write_editor",
            {"mode": result["mode"], "note": payload.note, "sql": payload.sql[:2000]},
        )
        return result

    @ai_app.post("/api/projects/{project_id}/editor/drafts/{draft_id}/load")
    def load_draft(project_id: str, draft_id: int, request: Request):
        workspace = _workspace(request, project_id)
        try:
            return workspace.load_draft(draft_id)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"No staged draft {draft_id}") from None

    # -- execution ---------------------------------------------------------------

    @ai_app.post("/api/projects/{project_id}/execute")
    def agent_execute(project_id: str, request: Request):
        # Agent-only path. Runs the editor's content if the user has granted
        # autonomous execution; otherwise registers a request the user approves
        # by clicking Run (docs/ai-workspace-model.md §4).
        if not _is_agent(request):
            raise HTTPException(status_code=403, detail="Use /execute/run from the UI.")
        workspace = _workspace(request, project_id)
        editor_sql = workspace.get_editor()["sql"]
        if not editor_sql.strip():
            _audit(request, workspace, "execute", {"reason": "empty_editor"}, "error")
            return {
                "status": "error",
                "error": "The editor is empty. Put a query in it with shori_write_editor first.",
            }
        if workspace.get_settings()["autonomous_execute"]:
            result = workspace.execute_editor(initiated_by="agent")
            _audit(
                request, workspace, "execute",
                {"mode": "autonomous", "status": result["status"], "sql": editor_sql[:2000]},
                "allowed" if result["status"] == "ok" else "error",
            )
            return {
                "status": result["status"],
                "result_id": result["result_id"],
                "error": result["error"],
            }
        request_id = workspace.create_execution_request(editor_sql)
        _audit(request, workspace, "execute", {"mode": "pending", "sql": editor_sql[:2000]}, "pending")
        return {
            "status": "pending_approval",
            "request_id": request_id,
            "message": (
                "Autonomous execution is off. The query is queued for the user to run with the "
                "Run button in the AI workspace. Poll shori_get_workspace_state for the outcome."
            ),
        }

    @ai_app.post("/api/projects/{project_id}/execute/run")
    def user_run(project_id: str, request: Request):
        if _is_agent(request):
            raise HTTPException(status_code=403, detail="Agents use /execute.")
        workspace = _workspace(request, project_id)
        try:
            result = workspace.execute_editor(initiated_by="user")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
        workspace.resolve_pending_execute(result["result_id"])
        return result

    @ai_app.get("/api/projects/{project_id}/results/{result_id}/rows")
    def result_rows(project_id: str, result_id: int, request: Request, limit: int = 100):
        # Phase 3: user/UI only. Gated agent access arrives in Phase 4.
        if _is_agent(request):
            raise HTTPException(
                status_code=403,
                detail="Result rows are not available to the agent yet (result disclosure is Phase 4).",
            )
        workspace = _workspace(request, project_id)
        try:
            return workspace.read_result_rows(result_id, limit=max(1, min(limit, 500)))
        except KeyError:
            raise HTTPException(status_code=404, detail=f"No result {result_id}") from None

    @ai_app.put("/api/projects/{project_id}/settings")
    def put_settings(project_id: str, payload: SettingsUpdate, request: Request):
        # Consent toggles are user-only — the agent can never widen its own access.
        if _is_agent(request):
            raise HTTPException(status_code=403, detail="Consent settings are set by the user only.")
        workspace = _workspace(request, project_id)
        for key in ("autonomous_execute", "auto_share_results"):
            value = getattr(payload, key)
            if value is not None:
                workspace.set_setting(key, value)
        return workspace.get_settings()

    # -- validation / state / activity --------------------------------------------

    @ai_app.post("/api/projects/{project_id}/validate")
    def validate_sql(project_id: str, payload: ValidateRequest, request: Request):
        workspace = _workspace(request, project_id)
        result = workspace.validate_sql(payload.sql)
        _audit(
            request,
            workspace,
            "validate_sql",
            {"valid": result["valid"], "sql": payload.sql[:2000]},
        )
        return result

    @ai_app.get("/api/projects/{project_id}/state")
    def get_state(project_id: str, request: Request):
        project = _find_project(request, project_id)
        workspace = request.app.state.workspaces.get(project_id)
        workspace.sweep_inbox()
        state = workspace.workspace_state()
        state["project_id"] = project["id"]
        state["project_name"] = project["name"]
        if _is_agent(request) and state.get("latest_result"):
            # The agent learns only that its query ran and whether it errored —
            # never row counts or result columns (docs §4). Rows come via the
            # gated Phase 4 disclosure path, not here.
            latest = state["latest_result"]
            state["latest_result"] = {
                key: latest.get(key)
                for key in ("result_id", "status", "error", "request_id", "shared", "ran_at")
            }
        _audit(request, workspace, "get_workspace_state", {})
        return state

    @ai_app.get("/api/projects/{project_id}/activity")
    def get_activity(project_id: str, request: Request, limit: int = 50):
        workspace = _workspace(request, project_id)
        return workspace.list_activity(limit=max(1, min(limit, 200)))

    # -- workspace UI (registered last so /api/* always wins) ---------------------

    @ai_app.get("/{project_id}", response_class=HTMLResponse)
    def workspace_page(project_id: str, request: Request):
        _find_project(request, project_id)
        return HTMLResponse((_STATIC_DIR / "index.html").read_text())

    return ai_app
