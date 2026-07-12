# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "mcp>=1.2",
#     "httpx>=0.27",
# ]
# ///
"""Shori AI workspace — stdio MCP shim.

This file is deliberately the AI agent's ENTIRE attack surface on Shori:
every tool below is one HTTP call to the Shori backend's /ai sub-app, which
performs all permission checks and owns all databases. This process holds no
DuckDB connection, no credentials, and exactly one piece of state — the
pinned project. See docs/ai-workspace-model.md (canonical spec).

Run (registered per agent CLI):
    claude mcp add shori -- uv run /path/to/shori_mcp.py
    claude mcp add shori --env SHORI_PROJECT_ID=<id> -- uv run /path/to/shori_mcp.py

Environment:
    SHORI_BACKEND_URL   Shori backend base URL (default http://127.0.0.1:8000)
    SHORI_PROJECT_ID    Config pin: locks the session to one project and
                        removes the project-selection tools entirely.
"""

import argparse
import os

import httpx
from mcp.server.fastmcp import FastMCP

BACKEND_URL = os.environ.get("SHORI_BACKEND_URL", "http://127.0.0.1:8000").rstrip("/")

_parser = argparse.ArgumentParser(description="Shori AI workspace MCP shim")
_parser.add_argument(
    "--project",
    default=os.environ.get("SHORI_PROJECT_ID") or None,
    help="Config pin: lock this session to one project id (overrides SHORI_PROJECT_ID)",
)
_args = _parser.parse_args()

CONFIG_PIN: str | None = _args.project

# Session pin: the only mutable state in this process. Lives here (one shim
# process per agent session) so two terminals on two projects never share a
# pin. Deliberately not persisted — a restart fails closed rather than
# silently resuming a possibly-wrong project.
_session_pin: dict | None = None  # {"id": ..., "name": ...}

mcp = FastMCP(
    "shori",
    instructions=(
        "Tools for exploring data in a Shori project's sandboxed AI workspace. "
        "All tools operate on the currently pinned project. "
        + (
            f"This session is locked to project '{CONFIG_PIN}' by configuration."
            if CONFIG_PIN
            else "Pin a project first: shori_list_projects, then shori_use_project. "
            "Only re-pin if the user explicitly asks to switch projects."
        )
    ),
)


def _get(path: str) -> dict | list:
    """GET from the backend's /ai sub-app, with actionable failure messages."""
    try:
        response = httpx.get(f"{BACKEND_URL}/ai{path}", timeout=15.0)
    except httpx.ConnectError as exc:
        raise RuntimeError(
            f"The Shori backend is not reachable at {BACKEND_URL}. "
            "Ask the user to start the Shori application, then retry."
        ) from exc
    if response.status_code == 404:
        detail = response.json().get("detail", "not found")
        raise RuntimeError(str(detail))
    response.raise_for_status()
    return response.json()


def _current_project_id() -> str:
    if CONFIG_PIN:
        return CONFIG_PIN
    if _session_pin is not None:
        return _session_pin["id"]
    raise RuntimeError(
        "No project selected. Call shori_list_projects to see the user's "
        "projects, then shori_use_project to pin one. If unsure which project "
        "the user wants, ask them."
    )


if CONFIG_PIN is None:
    # Project-selection tools exist only in session-pin mode. Under a config
    # pin the session is born locked and these are not registered at all.

    @mcp.tool()
    def shori_list_projects() -> list[dict]:
        """List the user's Shori projects (id, name, whether an AI workspace exists).

        Use this only to help the user pick a project to pin with
        shori_use_project. All other tools operate on the pinned project.
        """
        return _get("/api/projects")

    @mcp.tool()
    def shori_use_project(project_id: str) -> dict:
        """Pin this session to one Shori project. All subsequent tool calls target it.

        Call this once, after the user tells you which project to work on.
        Do NOT call it again unless the user explicitly asks to switch projects.
        """
        global _session_pin
        projects = _get("/api/projects")
        match = next((p for p in projects if p["id"] == project_id), None)
        if match is None:
            known = ", ".join(p["id"] for p in projects) or "(none)"
            raise RuntimeError(
                f"Unknown project id '{project_id}'. Known projects: {known}. "
                "Use shori_list_projects and confirm with the user."
            )
        _session_pin = {"id": match["id"], "name": match["name"]}
        return {
            "pinned_project_id": match["id"],
            "pinned_project_name": match["name"],
            "note": "Session pinned. All shori_* tools now operate on this project.",
        }


@mcp.tool()
def shori_list_tables() -> list[dict]:
    """List the tables in the pinned project's AI workspace.

    These are snapshots the user exported for you — the only data you can see
    or query. Each entry carries provenance (`cloned_from`) and `cloned_at`;
    mention staleness to the user when a clone is old. If a table you need is
    missing, ask the user to export it via an export node in the main app.
    """
    return _get(f"/api/projects/{_current_project_id()}/tables")


@mcp.tool()
def shori_get_table_schema(table_name: str) -> dict:
    """Get column names and types for one table in the AI workspace.

    Always check schemas with this tool before drafting SQL — do not guess
    column names or ask the user to type them out.
    """
    return _get(f"/api/projects/{_current_project_id()}/tables/{table_name}/schema")


@mcp.tool()
def shori_get_permissions() -> dict:
    """Show the per-table permission toggles the user has granted in this project's AI workspace.

    Call this early to learn what you may do: 'schema' lets you see table
    structure, 'execute' lets you run SQL blind (results go to the user's
    workspace UI, not to you), 'preview' lets you read rows of your own query
    results. If a capability you need is off, tell the user they can enable it
    in the AI workspace settings — you cannot change permissions yourself.
    """
    return _get(f"/api/projects/{_current_project_id()}/permissions")


if __name__ == "__main__":
    mcp.run()
