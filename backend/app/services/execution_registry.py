import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Callable
from typing import Literal

from app.models.pipeline import ExecutionRunStatus, NodeExecutionResult, NodeStatus


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ExecutionCancelled(Exception):
    pass


class ExecutionController:
    def __init__(self, registry: "ExecutionRegistry", execution_id: str):
        self._registry = registry
        self.execution_id = execution_id

    def is_cancelled(self) -> bool:
        return self._registry.is_cancelled(self.execution_id)

    def raise_if_cancelled(self) -> None:
        if self.is_cancelled():
            raise ExecutionCancelled()

    def set_abort_callback(self, callback: Callable[[], None]) -> None:
        self._registry.set_abort_callback(self.execution_id, callback)

    def clear_abort_callback(self) -> None:
        self._registry.clear_abort_callback(self.execution_id)


class ExecutionRegistry:
    def __init__(self, retention_seconds: int = 15 * 60):
        self._retention_seconds = retention_seconds
        self._runs: dict[str, ExecutionRunStatus] = {}
        self._tasks = {}
        self._abort_callbacks: dict[str, Callable[[], None]] = {}
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

    def create_controller(self, execution_id: str) -> ExecutionController:
        return ExecutionController(self, execution_id)

    def attach_task(self, execution_id: str, task) -> None:
        with self._lock:
            if execution_id in self._runs:
                self._tasks[execution_id] = task

    def set_abort_callback(self, execution_id: str, callback: Callable[[], None]) -> None:
        with self._lock:
            run = self._runs.get(execution_id)
            if run is None or run.status != NodeStatus.RUNNING:
                return
            self._abort_callbacks[execution_id] = callback

    def clear_abort_callback(self, execution_id: str) -> None:
        with self._lock:
            self._abort_callbacks.pop(execution_id, None)

    def is_cancelled(self, execution_id: str) -> bool:
        with self._lock:
            run = self._runs.get(execution_id)
            return run is not None and run.status == NodeStatus.CANCELLED

    def abort_run(self, execution_id: str) -> ExecutionRunStatus | None:
        task = None
        abort_callback: Callable[[], None] | None = None

        with self._lock:
            run = self._runs.get(execution_id)
            if run is None:
                return None

            if run.status == NodeStatus.CANCELLED:
                return run.model_copy(deep=True)

            if run.status in {NodeStatus.SUCCESS, NodeStatus.ERROR}:
                return run.model_copy(deep=True)

            finished_at = utc_now_iso()
            for node_id in self._node_ids.get(execution_id, []):
                existing = run.node_results.get(node_id)
                if existing and existing.status == NodeStatus.SUCCESS:
                    continue
                run.node_results[node_id] = NodeExecutionResult(
                    node_id=node_id,
                    status=NodeStatus.CANCELLED,
                    error=existing.error if existing and existing.error else "Execution aborted by user.",
                    started_at=existing.started_at if existing else run.started_at,
                    finished_at=finished_at,
                )

            run.status = NodeStatus.CANCELLED
            run.finished_at = finished_at
            self._completed_at[execution_id] = time.monotonic()
            abort_callback = self._abort_callbacks.pop(execution_id, None)
            task = self._tasks.get(execution_id)
            snapshot = run.model_copy(deep=True)

        if abort_callback is not None:
            try:
                abort_callback()
            except Exception:
                pass

        if task is not None:
            task.cancel()

        return snapshot

    def get_run(self, execution_id: str) -> ExecutionRunStatus | None:
        with self._lock:
            self._cleanup_expired_unlocked()
            run = self._runs.get(execution_id)
            if run is None:
                return None
            task = self._tasks.get(execution_id)
            if run.status == NodeStatus.RUNNING and task is not None and task.done():
                if getattr(task, "cancelled", lambda: False)():
                    return run.model_copy(deep=True)
                try:
                    exc = task.exception()
                except Exception as err:  # pragma: no cover - defensive fallback
                    exc = err
                if exc is not None:
                    self._fail_run_unlocked(execution_id, f"Execution crashed: {exc}")
                    run = self._runs.get(execution_id)
                elif self._can_finalize_run_unlocked(run):
                    self._finalize_run_unlocked(execution_id)
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
            if run is None or run.status == NodeStatus.CANCELLED:
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
            if run is None or run.status == NodeStatus.CANCELLED:
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
            self._finalize_run_unlocked(execution_id)

    def fail_run(self, execution_id: str, error: str) -> None:
        with self._lock:
            self._fail_run_unlocked(execution_id, error)

    def close(self) -> None:
        with self._lock:
            self._runs.clear()
            self._tasks.clear()
            self._abort_callbacks.clear()
            self._node_ids.clear()
            self._completed_at.clear()

    def _fail_run_unlocked(self, execution_id: str, error: str) -> None:
        run = self._runs.get(execution_id)
        if run is None or run.status == NodeStatus.CANCELLED:
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
        self._abort_callbacks.pop(execution_id, None)

    def _can_finalize_run_unlocked(self, run: ExecutionRunStatus) -> bool:
        if not self._node_ids.get(run.execution_id):
            return False
        if len(run.node_results) < len(self._node_ids[run.execution_id]):
            return False
        return all(
            result.status in {NodeStatus.SUCCESS, NodeStatus.ERROR, NodeStatus.CANCELLED}
            for result in run.node_results.values()
        )

    def _finalize_run_unlocked(self, execution_id: str) -> None:
        run = self._runs.get(execution_id)
        if run is None or run.status == NodeStatus.CANCELLED:
            return
        has_cancelled = any(result.status == NodeStatus.CANCELLED for result in run.node_results.values())
        has_error = any(result.status == NodeStatus.ERROR for result in run.node_results.values())
        if has_cancelled:
            run.status = NodeStatus.CANCELLED
        else:
            run.status = NodeStatus.ERROR if has_error else NodeStatus.SUCCESS
        run.finished_at = utc_now_iso()
        self._completed_at[execution_id] = time.monotonic()
        self._abort_callbacks.pop(execution_id, None)

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
            self._abort_callbacks.pop(execution_id, None)
            self._node_ids.pop(execution_id, None)
            self._completed_at.pop(execution_id, None)
