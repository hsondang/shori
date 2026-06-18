"""Export a node's table to a local path.

Only the local-filesystem destination exists today; the structure (an explicit
destination + format) leaves room for email or other sinks without reshaping the
call sites.
"""

from pathlib import Path

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
