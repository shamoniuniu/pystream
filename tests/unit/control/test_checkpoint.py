"""停流 Checkpoint 协调顺序、超时和未知结果测试。"""

from __future__ import annotations

import asyncio
import hashlib
import os
from pathlib import Path

import pytest

from pystream.api import ExecutionConfig, FileSinkConfig, OperatorType, StreamGraph
from pystream.checkpoint import (
    LocalCheckpointStore,
    TaskSnapshotDescriptor,
    TransactionDescriptor,
)
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
        fail_wait_after_snapshot_task: str | None = None,
        transactional: bool = False,
        output_root: Path | None = None,
    ) -> None:
        self.store = store
        self.wait_delay = wait_delay
        self.fail_complete_task = fail_complete_task
        self.fail_arm_task = fail_arm_task
        self.fail_wait_after_snapshot_task = fail_wait_after_snapshot_task
        self.transactional = transactional
        self.output_root = output_root
        self.calls: list[tuple[str, str, int]] = []
        self.transactions: dict[tuple[str, int], TransactionDescriptor] = {}

    async def arm_checkpoint(
        self,
        worker,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> None:
        del worker, attempt_id, coordinator_epoch
        self.calls.append(("arm", task_id, checkpoint_id))
        if task_id == self.fail_arm_task:
            raise ConnectionError("arm response lost")

    async def trigger_checkpoint(
        self,
        worker,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> TaskSnapshotDescriptor:
        del worker, attempt_id
        self.calls.append(("trigger", task_id, checkpoint_id))
        return self._snapshot(task_id, checkpoint_id, coordinator_epoch)

    async def wait_checkpoint(
        self,
        worker,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> TaskSnapshotDescriptor:
        del worker, attempt_id
        self.calls.append(("wait", task_id, checkpoint_id))
        if self.wait_delay:
            await asyncio.sleep(self.wait_delay)
        snapshot = self._snapshot(task_id, checkpoint_id, coordinator_epoch)
        if task_id == self.fail_wait_after_snapshot_task:
            raise ConnectionError("prepared response lost")
        return snapshot

    async def complete_checkpoint(
        self,
        worker,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> None:
        del worker, attempt_id, coordinator_epoch
        self.calls.append(("complete", task_id, checkpoint_id))
        transaction = self.transactions.get((task_id, checkpoint_id))
        if transaction is not None:
            if self.output_root is None:
                raise AssertionError("transactional gateway 缺少 output root")
            pending = self.output_root / transaction.pending_path
            target = (
                self.output_root
                / transaction.job_id
                / transaction.operator_id
                / "committed"
                / f"checkpoint-{checkpoint_id:020d}"
                / pending.name
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.exists():
                os.replace(pending, target)
        if task_id == self.fail_complete_task:
            raise ConnectionError("complete response lost")

    async def abort_checkpoint(
        self,
        worker,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> None:
        del worker, attempt_id, coordinator_epoch
        self.calls.append(("abort", task_id, checkpoint_id))
        transaction = self.transactions.get((task_id, checkpoint_id))
        if transaction is not None and self.output_root is not None:
            (self.output_root / transaction.pending_path).unlink(missing_ok=True)

    def _snapshot(
        self,
        task_id: str,
        checkpoint_id: int,
        coordinator_epoch: int,
    ) -> TaskSnapshotDescriptor:
        operator_id = task_id.rsplit(":", 2)[1]
        transactions: tuple[TransactionDescriptor, ...] = ()
        if self.transactional and operator_id == "output":
            if self.output_root is None:
                raise AssertionError("transactional gateway 缺少 output root")
            transaction_id = f"checkpoint-{checkpoint_id}"
            relative_path = (
                f"job-1/output/pending/attempt-00000000/tx-{transaction_id}/part-00000.csv"
            )
            pending = self.output_root / relative_path
            pending.parent.mkdir(parents=True, exist_ok=True)
            content = f"checkpoint-{checkpoint_id}\n".encode()
            pending.write_bytes(content)
            transaction = TransactionDescriptor(
                job_id="job-1",
                checkpoint_id=checkpoint_id,
                attempt_id=0,
                coordinator_epoch=coordinator_epoch,
                task_id=task_id,
                operator_id=operator_id,
                transaction_id=transaction_id,
                pending_path=relative_path,
                sha256=hashlib.sha256(content).hexdigest(),
                size=len(content),
            )
            self.transactions[(task_id, checkpoint_id)] = transaction
            transactions = (transaction,)
        return self.store.write_task_snapshot(
            job_id="job-1",
            checkpoint_id=checkpoint_id,
            attempt_id=0,
            coordinator_epoch=coordinator_epoch,
            task_id=task_id,
            operator_id=operator_id,
            state={"kind": "test", "snapshot": "{}"},
            transactions=transactions,
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


def exactly_once_graph(graph: StreamGraph, output_root: Path) -> StreamGraph:
    """把最小图转换为使用临时 output root 的 Exactly-once 图。"""
    operators = [
        (
            operator.model_copy(
                update={
                    "config": operator.config.model_copy(update={"output_path": str(output_root)})
                }
            )
            if operator.type is OperatorType.SINK and isinstance(operator.config, FileSinkConfig)
            else operator
        )
        for operator in graph.definition.operators
    ]
    definition = graph.definition.model_copy(
        update={
            "execution": ExecutionConfig(),
            "operators": operators,
        }
    )
    return StreamGraph(definition)


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


@pytest.mark.asyncio
async def test_exactly_once_checkpoint_writes_decision_manifest_and_finalized(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    output_root = tmp_path / "output"
    store = LocalCheckpointStore(tmp_path / "checkpoints")
    graph, workers = running_graph(exactly_once_graph(two_task_graph, output_root))
    gateway = RecordingCheckpointGateway(
        store,
        transactional=True,
        output_root=output_root,
    )
    coordinator = CheckpointCoordinator(store, gateway, workers.__getitem__)

    manifest = await coordinator.run(
        graph,
        checkpoint_id=5,
        attempt_id=0,
        coordinator_epoch=3,
        timeout=2,
    )

    decision = store.read_decision(
        "job-1",
        5,
        expected_task_ids=set(graph.tasks),
        expected_transaction_task_ids={
            task.task_id for task in graph.tasks.values() if task.operator_type is OperatorType.SINK
        },
    )
    finalization = store.read_finalization("job-1", 5)
    assert manifest.snapshots == decision.snapshots
    assert len(finalization.output_manifests) == 1
    assert Path(finalization.output_manifests[0]).is_file()
    assert store.unfinalized_decisions("job-1") == ()


@pytest.mark.asyncio
async def test_prepared后decision前失败仍abort且不发布输出(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    output_root = tmp_path / "output"
    store = LocalCheckpointStore(tmp_path / "checkpoints")
    graph, workers = running_graph(exactly_once_graph(two_task_graph, output_root))
    sink_task = next(
        task.task_id for task in graph.tasks.values() if task.operator_type is OperatorType.SINK
    )
    gateway = RecordingCheckpointGateway(
        store,
        fail_wait_after_snapshot_task=sink_task,
        transactional=True,
        output_root=output_root,
    )
    coordinator = CheckpointCoordinator(store, gateway, workers.__getitem__)

    with pytest.raises(CheckpointCoordinationError, match="prepared response lost"):
        await coordinator.run(
            graph,
            checkpoint_id=6,
            attempt_id=0,
            timeout=2,
        )

    assert not store.has_decision("job-1", 6)
    assert {task_id for action, task_id, _ in gateway.calls if action == "abort"} == set(
        graph.tasks
    )
    assert not tuple(output_root.rglob("part-*.csv"))
    assert not tuple(output_root.rglob("checkpoint-*.json"))


@pytest.mark.asyncio
async def test_decided后complete未知结果不abort且可重放finalize(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    output_root = tmp_path / "output"
    store = LocalCheckpointStore(tmp_path / "checkpoints")
    graph, workers = running_graph(exactly_once_graph(two_task_graph, output_root))
    sink_task = next(
        task.task_id for task in graph.tasks.values() if task.operator_type is OperatorType.SINK
    )
    gateway = RecordingCheckpointGateway(
        store,
        fail_complete_task=sink_task,
        transactional=True,
        output_root=output_root,
    )
    coordinator = CheckpointCoordinator(store, gateway, workers.__getitem__)

    with pytest.raises(CheckpointCoordinationError, match="已 DECIDED"):
        await coordinator.run(
            graph,
            checkpoint_id=7,
            attempt_id=0,
            timeout=1.0,
        )

    assert store.has_decision("job-1", 7)
    assert len(store.unfinalized_decisions("job-1")) == 1
    assert not any(action == "abort" for action, _, _ in gateway.calls)
    assert not tuple(output_root.rglob("checkpoint-*.json"))

    gateway.fail_complete_task = None
    manifest = await coordinator.resume_decision(
        graph,
        checkpoint_id=7,
        timeout=2,
    )

    assert manifest.checkpoint_id == 7
    assert store.unfinalized_decisions("job-1") == ()
    finalization = store.read_finalization("job-1", 7)
    assert Path(finalization.output_manifests[0]).is_file()
    assert not any(action == "abort" for action, _, _ in gateway.calls)
