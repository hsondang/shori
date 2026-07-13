"""AI workspace sub-app — see docs/ai-workspace-model.md (canonical spec).

Self-contained package: imports nothing from `app/`. Its interfaces with the
main application are limited to (a) the parquet spool directory under
data/ai/<project_id>/inbox/ and (b) a read-only view of data/projects.sqlite3
for project identity. The main app's only reference to this package is the
guarded mount in app/main.py.
"""

from ai_workspace.server import build_ai_app

__all__ = ["build_ai_app"]
