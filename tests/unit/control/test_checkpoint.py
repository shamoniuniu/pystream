"""停流 Checkpoint 协调顺序、超时和未知结果测试。"""

from __future__ import annotations

import asyncio

import pytest

from pystream.api import OperatorType, StreamGraph
from pystream.checkpoint import LocalCheckpointStore, TaskSnapshotDescriptor
from pystream.control.checkpoint import (
    CheckpointCoordinationError,
    CheckpointCoordinator,
)
from pystream.control.execution import ExecutionGraph, build_execution_graph
from pystream.control.models import TaskStatus, WorkerNode


class RecordingCheckpointGateway:
    """写入真实快照文件并记录控制调用。"""

    def __init__(
        self,
        store: LocalCheckpointStore,
        *,
        wait_delay: float = 0,
        fail_complete_task: str | None = None,
        fail_arm_task: str | None = None,
    ) -> None:
        self.store = store
        self.wait_delay = wait_delay
        self.fail_complete_task = fail_complete_task
        self.fail_arm_task = fail_arm_task
        self.calls: list[tuple[str, str, int]] = []

    async def arm_checkpoint(self, worker, task_id: str, checkpoint_id: int) -> None:
        del worker
        self.calls.append(("arm", task_id, checkpoint_id))
        if task_id == self.fail_arm_task:
            raise ConnectionError("arm response lost")

    async def trigger_checkpoint(
        self,
        worker,
        task_id: str,
        checkpoint_id: int,
    ) -> TaskSnapshotDescriptor:
        del worker
        self.calls.append(("trigger", task_id, checkpoint_id))
        return self._snapshot(task_id, checkpoint_id)

    async def wait_checkpoint(
        self,
        worker,
        task_id: str,
        checkpoint_id: int,
    ) -> TaskSnapshotDescriptor:
        del worker
        self.calls.append(("wait", task_id, checkpoint_id))
        if self.wait_delay:
            await asyncio.sleep(self.wait_delay)
        return self._snapshot(task_id, checkpoint_id)

    async def complete_checkpoint(
        self,
        worker,
        task_id: str,
        checkpoint_id: int,
    ) -> None:
        del worker
        self.calls.append(("complete", task_id, checkpoint_id))
        if task_id == self.fail_complete_task:
            raise ConnectionError("complete response lost")

    async def abort_checkpoint(
        self,
        worker,
        task_id: str,
        checkpoint_id: int,
    ) -> None:
        del worker
        self.calls.append(("abort", task_id, checkpoint_id))

    def _snapshot(self, task_id: str, checkpoint_id: int) -> TaskSnapshotDescriptor:
        operator_id = task_id.rsplit(":", 2)[1]
        return self.store.write_task_snapshot(
            job_id="job-1",
            checkpoint_id=checkpoint_id,
            attempt_id=0,
            task_id=task_id,
            operator_id=operator_id,
            state={"kind": "test", "snapshot": "{}"},
        )


def running_graph(
    graph: StreamGraph,
) -> tuple[ExecutionGraph, dict[str, WorkerNode]]:
    execution = build_execution_graph("job-1", graph)
    workers: dict[str, WorkerNode] = {}
    for index, task in enumerate(execution.tasks.values()):
        worker_id = f"worker-{index}"
        workers[worker_id] = WorkerNode.create(
            worker_id,
            f"http://{worker_id}:8081",
            worker_id,
            9000 + index,
            1,
        )
        task.assign(worker_id, 0)
        task.transition(TaskStatus.DEPLOYING)
        task.transition(TaskStatus.RUNNING)
    return execution, workers


@pytest.mark.asyncio
async def test_checkpoint_success_按下游到上游完成manifest(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    store = LocalCheckpointStore(tmp_path / "checkpoints")
    graph, workers = running_graph(two_task_graph)
    gateway = RecordingCheckpointGateway(store)
    coordinator = CheckpointCoordinator(store, gateway, workers.__getitem__)

    manifest = await coordinator.run(
        graph,
        checkpoint_id=1,
        attempt_id=0,
        timeout=2,
    )

    order = [task.task_id for task in graph.deployment_order()]
    source = next(
        task.task_id for task in graph.tasks.values() if task.operator_type is OperatorType.SOURCE
    )
    assert manifest.checkpoint_id == 1
    assert [task_id for action, task_id, _ in gateway.calls if action == "arm"] == order
    assert [task_id for action, task_id, _ in gateway.calls if action == "trigger"] == [source]
    assert [task_id for action, task_id, _ in gateway.calls if action == "complete"] == order
    assert store.latest_manifest("job-1", expected_task_ids=set(graph.tasks)) == manifest


@pytest.mark.asyncio
async def test_checkpoint_wait超时会abort全部task并删除不完整attempt(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    store = LocalCheckpointStore(tmp_path / "checkpoints")
    graph, workers = running_graph(two_task_graph)
    gateway = RecordingCheckpointGateway(store, wait_delay=1)
    coordinator = CheckpointCoordinator(store, gateway, workers.__getitem__)

    with pytest.raises(CheckpointCoordinationError, match="TimeoutError"):
        await coordinator.run(
            graph,
            checkpoint_id=2,
            attempt_id=0,
            timeout=0.05,
        )

    assert {task_id for action, task_id, _ in gateway.calls if action == "abort"} == set(
        graph.tasks
    )
    assert store.latest_manifest("job-1", expected_task_ids=set(graph.tasks)) is None


@pytest.mark.asyncio
async def test_arm响应丢失仍abort可能已arm的task(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    store = LocalCheckpointStore(tmp_path / "checkpoints")
    graph, workers = running_graph(two_task_graph)
    uncertain_task = graph.deployment_order()[0].task_id
    gateway = RecordingCheckpointGateway(store, fail_arm_task=uncertain_task)
    coordinator = CheckpointCoordinator(store, gateway, workers.__getitem__)

    with pytest.raises(CheckpointCoordinationError, match="arm response lost"):
        await coordinator.run(
            graph,
            checkpoint_id=3,
            attempt_id=0,
            timeout=2,
        )

    assert ("abort", uncertain_task, 3) in gateway.calls


@pytest.mark.asyncio
async def test_manifest完成后complete响应丢失仍保留合法恢复点(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    store = LocalCheckpointStore(tmp_path / "checkpoints")
    graph, workers = running_graph(two_task_graph)
    failing_task = graph.deployment_order()[0].task_id
    gateway = RecordingCheckpointGateway(
        store,
        fail_complete_task=failing_task,
    )
    coordinator = CheckpointCoordinator(store, gateway, workers.__getitem__)

    with pytest.raises(CheckpointCoordinationError, match="complete response lost"):
        await coordinator.run(
            graph,
            checkpoint_id=4,
            attempt_id=0,
            timeout=2,
        )

    manifest = store.latest_manifest("job-1", expected_task_ids=set(graph.tasks))
    assert manifest is not None and manifest.checkpoint_id == 4
    assert {task_id for action, task_id, _ in gateway.calls if action == "abort"} == set(
        graph.tasks
    )
