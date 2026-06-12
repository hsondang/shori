from collections import OrderedDict
import logging
import shutil
import threading

from app.config import project_data_dir, project_duckdb_path
from app.services.duckdb_manager import DuckDBManager

logger = logging.getLogger(__name__)

DEFAULT_MAX_OPEN = 8


class ProjectDuckDBRegistry:
    """Maps project ids to their DuckDBManager, opening files lazily.

    Keeps at most `max_open` files open, evicting the least-recently-used
    manager that has no in-flight operations. All loads/reads for a project
    must go through the manager returned by `get` — never open the file
    directly, or the single-writer-per-process invariant breaks.
    """

    def __init__(self, max_open: int = DEFAULT_MAX_OPEN):
        self._max_open = max_open
        self._managers: OrderedDict[str, DuckDBManager] = OrderedDict()
        self._settings: dict[str, dict] = {}
        self._lock = threading.Lock()

    def get(self, project_id: str, *, settings: dict | None = None) -> DuckDBManager:
        with self._lock:
            manager = self._managers.get(project_id)
            current_settings = self._settings.get(project_id)
            if manager is not None and settings is not None and settings != current_settings:
                if manager._active_ops == 0:
                    manager.close()
                    manager = None
                else:
                    logger.warning(
                        "Project %s settings changed while operations are running; "
                        "new storage settings apply on next open.",
                        project_id,
                    )
            if manager is None:
                effective = settings if settings is not None else current_settings or {}
                data_dir = project_data_dir(project_id)
                manager = DuckDBManager(
                    project_duckdb_path(project_id),
                    memory_limit=effective.get("duckdb_memory_limit"),
                    temp_directory=data_dir / "tmp",
                )
                self._managers[project_id] = manager
                if settings is not None:
                    self._settings[project_id] = settings
            elif settings is not None:
                self._settings[project_id] = settings
            self._managers.move_to_end(project_id)
            self._evict_excess_unlocked()
            return manager

    def _evict_excess_unlocked(self):
        while len(self._managers) > self._max_open:
            evicted = None
            for project_id, manager in self._managers.items():
                if manager._active_ops == 0:
                    evicted = project_id
                    break
            if evicted is None:
                return  # everything is busy; try again on a later get()
            manager = self._managers.pop(evicted)
            try:
                manager.close()
            except Exception:
                logger.warning("Failed to close evicted project db %s", evicted, exc_info=True)

    def close_project(self, project_id: str) -> None:
        with self._lock:
            manager = self._managers.pop(project_id, None)
            self._settings.pop(project_id, None)
        if manager is not None:
            manager.close()

    def close_and_delete(self, project_id: str) -> None:
        """Close the project's database and remove its entire data directory."""
        self.close_project(project_id)
        data_dir = project_data_dir(project_id)
        if data_dir.exists():
            shutil.rmtree(data_dir)

    def close_all(self) -> None:
        with self._lock:
            managers = list(self._managers.values())
            self._managers.clear()
            self._settings.clear()
        for manager in managers:
            try:
                manager.close()
            except Exception:
                logger.warning("Failed to close project db on shutdown", exc_info=True)
