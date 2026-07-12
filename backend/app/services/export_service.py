"""Export a node's table to a local path.

Only the local-filesystem destination exists today; the structure (an explicit
destination + format) leaves room for email or other sinks without reshaping the
call sites.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from app.config import DATA_DIR, validate_project_id
from app.services.duckdb_manager import is_reserved_table_name

SUPPORTED_FORMATS = {"csv", "parquet", "xlsx"}
_EXTENSION = {"csv": ".csv", "parquet": ".parquet", "xlsx": ".xlsx"}


def export_table_to_local(manager, table_name: str, output_path: str, fmt: str) -> dict:
    fmt = fmt.lower()
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(
            f"Unsupported export format '{fmt}'. Choose one of {', '.join(sorted(SUPPORTED_FORMATS))}."
        )
    if is_reserved_table_name(table_name) or not manager.table_exists(table_name):
        raise ValueError(f"Table '{table_name}' not found")
    if not output_path.strip():
        raise ValueError("An output path is required")

    target = Path(output_path).expanduser()
    if target.suffix.lower() != _EXTENSION[fmt]:
        target = target.with_suffix(_EXTENSION[fmt])
    target.parent.mkdir(parents=True, exist_ok=True)

    manager.copy_table_to(table_name, str(target), fmt)
    stats = manager.table_stats(table_name)
    return {
        "output_path": str(target),
        "format": fmt,
        "row_count": stats["row_count"],
    }


def export_table_to_ai_workspace(
    manager, project_id: str, table_name: str, source_node_id: str | None = None
) -> dict:
    """Clone a table into the project's AI workspace spool.

    Writes <table>.parquet + <table>.json into data/ai/<project_id>/inbox/; the
    ai_workspace sub-app ingests and deletes them on its next sweep. This is the
    only data path from the user workspace into the AI workspace, and the
    sidecar is written last so a half-written parquet is never ingested
    (docs/ai-workspace-model.md §3).
    """
    inbox = DATA_DIR / "ai" / validate_project_id(project_id) / "inbox"
    result = export_table_to_local(manager, table_name, str(inbox / table_name), "parquet")
    sidecar = {
        "table_name": table_name,
        "source_node_id": source_node_id,
        "source_table": table_name,
        "row_count": result["row_count"],
        "exported_at": datetime.now(timezone.utc).isoformat(),
    }
    (inbox / f"{table_name}.json").write_text(json.dumps(sidecar, indent=2))
    return {
        "destination": "ai_workspace",
        "table_name": table_name,
        "row_count": result["row_count"],
    }
