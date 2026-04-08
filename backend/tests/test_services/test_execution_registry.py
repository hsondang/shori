import time

from app.models.pipeline import NodeExecutionResult, NodeStatus
from app.services.execution_registry import ExecutionRegistry


def test_completed_runs_remain_available_until_ttl_expires():
    registry = ExecutionRegistry(retention_seconds=60)
    run = registry.create_run("node", ["node-1"])
    registry.mark_node_running(run.execution_id, "node-1", run.started_at)
    registry.set_node_result(
        run.execution_id,
        NodeExecutionResult(
            node_id="node-1",
            status=NodeStatus.SUCCESS,
            started_at=run.started_at,
            finished_at=run.started_at,
        ),
    )
    registry.finalize_run(run.execution_id)

    snapshot = registry.get_run(run.execution_id)

    assert snapshot is not None
    assert snapshot.status == NodeStatus.SUCCESS


def test_expired_runs_are_removed():
    registry = ExecutionRegistry(retention_seconds=0)
    run = registry.create_run("node", ["node-1"])
    registry.fail_run(run.execution_id, "boom")

    time.sleep(0.01)

    assert registry.get_run(run.execution_id) is None


def test_running_run_switches_to_error_if_task_crashes():
    class CrashedTask:
        def done(self):
            return True

        def exception(self):
            return RuntimeError("task crashed")

    registry = ExecutionRegistry(retention_seconds=60)
    run = registry.create_run("node", ["node-1"])
    registry.attach_task(run.execution_id, CrashedTask())

    snapshot = registry.get_run(run.execution_id)

    assert snapshot is not None
    assert snapshot.status == NodeStatus.ERROR


def test_completed_successful_task_finalizes_running_snapshot():
    class SuccessfulTask:
        def done(self):
            return True

        def exception(self):
            return None

    registry = ExecutionRegistry(retention_seconds=60)
    run = registry.create_run("node", ["node-1"])
    registry.attach_task(run.execution_id, SuccessfulTask())
    registry.set_node_result(
        run.execution_id,
        NodeExecutionResult(
            node_id="node-1",
            status=NodeStatus.SUCCESS,
            started_at=run.started_at,
            finished_at=run.started_at,
        ),
    )

    snapshot = registry.get_run(run.execution_id)

    assert snapshot is not None
    assert snapshot.status == NodeStatus.SUCCESS
