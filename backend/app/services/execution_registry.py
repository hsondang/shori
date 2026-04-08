import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Literal

from app.models.pipeline import ExecutionRunStatus, NodeExecutionResult, NodeStatus


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ExecutionRegistry:
    def __init__(self, retention_seconds: int = 15 * 60):
        self._retention_seconds = retention_seconds
        self._runs: dict[str, ExecutionRunStatus] = {}
        self._tasks = {}
        self._node_ids: dict[str, list[str]] = {}
        self._completed_at: dict[str, float] = {}
        self._lock = threading.Lock()

    def create_run(
        self,
        kind: Literal["node", "pipeline"],
        node_ids: list[str],
    ) -> ExecutionRunStatus:
        with self._lock:
            self._cleanup_expired_unlocked()
            execution_id = uuid.uuid4().hex
            run = ExecutionRunStatus(
                execution_id=execution_id,
                kind=kind,
                status=NodeStatus.RUNNING,
                started_at=utc_now_iso(),
            )
            self._runs[execution_id] = run
            self._node_ids[execution_id] = list(node_ids)
            return run.model_copy(deep=True)

    def attach_task(self, execution_id: str, task) -> None:
        with self._lock:
            if execution_id in self._runs:
                self._tasks[execution_id] = task

    def get_run(self, execution_id: str) -> ExecutionRunStatus | None:
        with self._lock:
            self._cleanup_expired_unlocked()
            run = self._runs.get(execution_id)
            if run is None:
                return None
            task = self._tasks.get(execution_id)
            if run.status == NodeStatus.RUNNING and task is not None and task.done():
                try:
                    exc = task.exception()
                except Exception as err:  # pragma: no cover - defensive fallback
                    exc = err
                if exc is not None:
                    self._fail_run_unlocked(execution_id, f"Execution crashed: {exc}")
                    run = self._runs.get(execution_id)
            return run.model_copy(deep=True) if run is not None else None

    def mark_node_running(self, execution_id: str, node_id: str, started_at: str) -> None:
        with self._lock:
            run = self._runs.get(execution_id)
            if run is None or run.status != NodeStatus.RUNNING:
                return
            run.node_results[node_id] = NodeExecutionResult(
                node_id=node_id,
                status=NodeStatus.RUNNING,
                started_at=started_at,
            )

    def set_node_result(self, execution_id: str, result: NodeExecutionResult) -> None:
        with self._lock:
            run = self._runs.get(execution_id)
            if run is None:
                return
            previous = run.node_results.get(result.node_id)
            run.node_results[result.node_id] = result.model_copy(
                update={
                    "started_at": result.started_at or (previous.started_at if previous else None),
                    "finished_at": result.finished_at or utc_now_iso(),
                }
            )

    def update_node_result(self, execution_id: str, result: NodeExecutionResult) -> None:
        with self._lock:
            run = self._runs.get(execution_id)
            if run is None:
                return
            previous = run.node_results.get(result.node_id)
            run.node_results[result.node_id] = result.model_copy(
                update={
                    "started_at": result.started_at or (previous.started_at if previous else None),
                    "finished_at": result.finished_at or (previous.finished_at if previous else None),
                }
            )

    def finalize_run(self, execution_id: str) -> None:
        with self._lock:
            run = self._runs.get(execution_id)
            if run is None:
                return
            has_error = any(result.status == NodeStatus.ERROR for result in run.node_results.values())
            run.status = NodeStatus.ERROR if has_error else NodeStatus.SUCCESS
            run.finished_at = utc_now_iso()
            self._completed_at[execution_id] = time.monotonic()

    def fail_run(self, execution_id: str, error: str) -> None:
        with self._lock:
            self._fail_run_unlocked(execution_id, error)

    def close(self) -> None:
        with self._lock:
            self._runs.clear()
            self._tasks.clear()
            self._node_ids.clear()
            self._completed_at.clear()

    def _fail_run_unlocked(self, execution_id: str, error: str) -> None:
        run = self._runs.get(execution_id)
        if run is None:
            return
        finished_at = utc_now_iso()
        for node_id in self._node_ids.get(execution_id, []):
            existing = run.node_results.get(node_id)
            if existing and existing.status == NodeStatus.SUCCESS:
                continue
            run.node_results[node_id] = NodeExecutionResult(
                node_id=node_id,
                status=NodeStatus.ERROR,
                error=existing.error if existing and existing.error else error,
                started_at=existing.started_at if existing else run.started_at,
                finished_at=finished_at,
            )
        run.status = NodeStatus.ERROR
        run.finished_at = finished_at
        self._completed_at[execution_id] = time.monotonic()

    def _cleanup_expired_unlocked(self) -> None:
        cutoff = time.monotonic() - self._retention_seconds
        expired_ids = [
            execution_id
            for execution_id, completed_at in self._completed_at.items()
            if completed_at <= cutoff
        ]
        for execution_id in expired_ids:
            self._runs.pop(execution_id, None)
            self._tasks.pop(execution_id, None)
            self._node_ids.pop(execution_id, None)
            self._completed_at.pop(execution_id, None)
