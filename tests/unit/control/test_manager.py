"""JobManager 部署编排、回滚、取消和状态聚合测试。"""

from __future__ import annotations

import asyncio
import hashlib
import threading
import time
from datetime import UTC, datetime, timedelta

import pytest

from pystream.api import (
    CheckpointConfig,
    DeliveryGuarantee,
    ExecutionConfig,
    OperatorType,
    RestartConfig,
    StreamGraph,
)
from pystream.checkpoint import (
    CheckpointStore,
    LocalCheckpointStore,
    S3CheckpointStore,
    TaskSnapshotDescriptor,
)
from pystream.control import (
    ArtifactError,
    CheckpointCoordinationError,
    ControlPlaneError,
    DeploymentError,
    InsufficientSlots,
    JobManager,
    JobMetadataRevision,
    JobStatus,
    LocalArtifactRepository,
    StoredJobMetadata,
    TaskDeployment,
    TaskStatus,
    WorkerNode,
)
from pystream.storage import ObjectConflict, ObjectNotFound, ObjectValue


class RecordingGateway:
    """记录部署/停止顺序并支持稳定故障注入。"""

    def __init__(
        self,
        *,
        fail_deploy_at: int | None = None,
        fail_stop_task: str | None = None,
        fail_checkpoint: bool = False,
        fail_deploy_attempts: set[int] | None = None,
    ) -> None:
        self.fail_deploy_at = fail_deploy_at
        self.fail_stop_task = fail_stop_task
        self.fail_checkpoint = fail_checkpoint
        self.fail_deploy_attempts = set(fail_deploy_attempts or ())
        self.checkpoint_store: CheckpointStore | None = None
        self.deploy_calls: list[tuple[str, str]] = []
        self.deployments: list[TaskDeployment] = []
        self.deployment_attempts: list[tuple[str, int, tuple[TaskSnapshotDescriptor, ...]]] = []
        self.stop_calls: list[tuple[str, str]] = []
        self.checkpoint_calls: list[tuple[str, str, int]] = []

    async def deploy_task(self, worker: WorkerNode, deployment: TaskDeployment) -> None:
        call_number = len(self.deploy_calls) + 1
        self.deploy_calls.append((worker.worker_id, deployment.task.task_id))
        self.deployments.append(deployment)
        self.deployment_attempts.append(
            (
                deployment.task.task_id,
                deployment.task.attempt_id,
                deployment.restore_descriptors,
            )
        )
        if self.fail_deploy_at == call_number:
            raise ConnectionError("simulated deploy failure")
        if deployment.task.attempt_id in self.fail_deploy_attempts:
            raise ConnectionError(f"simulated attempt {deployment.task.attempt_id} deploy failure")

    async def stop_task(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        coordinator_epoch: int = 0,
    ) -> None:
        del attempt_id, coordinator_epoch
        self.stop_calls.append((worker.worker_id, task_id))
        if task_id == self.fail_stop_task:
            raise ConnectionError("simulated stop failure")

    async def arm_checkpoint(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> None:
        del worker, attempt_id, coordinator_epoch
        self.checkpoint_calls.append(("arm", task_id, checkpoint_id))

    async def trigger_checkpoint(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> TaskSnapshotDescriptor:
        del worker, attempt_id
        self.checkpoint_calls.append(("trigger", task_id, checkpoint_id))
        if self.fail_checkpoint:
            raise ConnectionError("simulated checkpoint failure")
        return self._snapshot(task_id, checkpoint_id, coordinator_epoch)

    async def wait_checkpoint(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> TaskSnapshotDescriptor:
        del worker, attempt_id
        self.checkpoint_calls.append(("wait", task_id, checkpoint_id))
        return self._snapshot(task_id, checkpoint_id, coordinator_epoch)

    async def complete_checkpoint(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> None:
        del worker, attempt_id, coordinator_epoch
        self.checkpoint_calls.append(("complete", task_id, checkpoint_id))

    async def abort_checkpoint(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> None:
        del worker, attempt_id, coordinator_epoch
        self.checkpoint_calls.append(("abort", task_id, checkpoint_id))

    def _snapshot(
        self,
        task_id: str,
        checkpoint_id: int,
        coordinator_epoch: int,
    ) -> TaskSnapshotDescriptor:
        if self.checkpoint_store is None:
            raise AssertionError("测试 Gateway 缺少 Checkpoint Store")
        return self.checkpoint_store.write_task_snapshot(
            job_id=task_id.split(":", 1)[0],
            checkpoint_id=checkpoint_id,
            attempt_id=0,
            coordinator_epoch=coordinator_epoch,
            task_id=task_id,
            operator_id=task_id.rsplit(":", 2)[1],
            state={"kind": "test", "snapshot": "{}"},
        )


class SourceBarrierGateway(RecordingGateway):
    """要求同一 Source 的全部 subtasks 同时进入部署调用。"""

    def __init__(self, expected_sources: int) -> None:
        super().__init__()
        self.expected_sources = expected_sources
        self.source_entries = 0
        self.source_barrier = asyncio.Event()

    async def deploy_task(self, worker: WorkerNode, deployment: TaskDeployment) -> None:
        if deployment.task.operator_type is OperatorType.SOURCE:
            self.source_entries += 1
            if self.source_entries == self.expected_sources:
                self.source_barrier.set()
            await asyncio.wait_for(self.source_barrier.wait(), timeout=1)
        await super().deploy_task(worker, deployment)


class RecordingMetadataRepository:
    def __init__(self) -> None:
        self.revisions: list[JobMetadataRevision] = []
        self.etag: str | None = None

    def publish(
        self,
        revision: JobMetadataRevision,
        *,
        expected_current_etag: str | None,
    ) -> StoredJobMetadata:
        assert expected_current_etag == self.etag
        self.revisions.append(revision)
        self.etag = f"etag-{revision.revision}"
        return StoredJobMetadata(revision, self.etag)

    def read_current(self, job_id: str) -> StoredJobMetadata:
        revision = self.revisions[-1]
        assert revision.job_id == job_id
        assert self.etag is not None
        return StoredJobMetadata(revision, self.etag)

    def list_jobs(self) -> tuple[str, ...]:
        return tuple(sorted({item.job_id for item in self.revisions}))


class ConcurrencyDetectingMetadataRepository(RecordingMetadataRepository):
    def __init__(self) -> None:
        super().__init__()
        self._state_lock = threading.Lock()
        self._publish_active = False

    def publish(
        self,
        revision: JobMetadataRevision,
        *,
        expected_current_etag: str | None,
    ) -> StoredJobMetadata:
        with self._state_lock:
            if self._publish_active:
                raise AssertionError("metadata publish 不允许并发")
            self._publish_active = True
        try:
            time.sleep(0.02)
            return super().publish(
                revision,
                expected_current_etag=expected_current_etag,
            )
        finally:
            with self._state_lock:
                self._publish_active = False


class MemoryObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, ObjectValue] = {}

    def get(self, key: str) -> ObjectValue:
        try:
            return self.objects[key]
        except KeyError as exc:
            raise ObjectNotFound(key) from exc

    def put_if_absent(self, key: str, content: bytes) -> str:
        if key in self.objects:
            raise ObjectConflict(key)
        etag = hashlib.sha256(content).hexdigest()
        self.objects[key] = ObjectValue(content, etag)
        return etag

    def put_if_match(self, key: str, content: bytes, etag: str) -> str:
        current = self.objects.get(key)
        if current is None or current.etag != etag:
            raise ObjectConflict(key)
        new_etag = hashlib.sha256(content).hexdigest()
        self.objects[key] = ObjectValue(content, new_etag)
        return new_etag

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        return tuple(sorted(key for key in self.objects if key.startswith(prefix)))


def manager_with_workers(
    tmp_path,
    gateway: RecordingGateway,
    *,
    worker_count: int = 3,
    slots: int = 4,
    heartbeat_at: datetime | None = None,
    coordinator_epoch: int = 0,
    metadata_repository: RecordingMetadataRepository | None = None,
    checkpoint_store: CheckpointStore | None = None,
) -> JobManager:
    """创建带本地制品仓库和固定 Worker 的 JobManager。"""
    resolved_checkpoint_store = checkpoint_store or LocalCheckpointStore(tmp_path / "checkpoints")
    gateway.checkpoint_store = resolved_checkpoint_store
    manager = JobManager(
        LocalArtifactRepository(tmp_path / "artifacts"),
        gateway,
        heartbeat_timeout=timedelta(seconds=10),
        checkpoint_store=resolved_checkpoint_store,
        metadata_repository=metadata_repository,
        coordinator_epoch=coordinator_epoch,
    )
    for index in range(worker_count):
        manager.register_worker(
            f"worker-{index}",
            f"http://worker-{index}:8081",
            f"worker-{index}",
            9000 + index,
            slots,
            heartbeat_at,
        )
    return manager


def checkpoint_graph(
    graph: StreamGraph,
    *,
    delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE,
    max_consecutive_failures: int = 3,
    max_recovery_attempts: int = 3,
    recovery_delay: str = "0s",
) -> StreamGraph:
    definition = graph.definition.model_copy(
        update={
            "execution": ExecutionConfig(
                delivery_guarantee=delivery_guarantee,
                checkpoint=CheckpointConfig(
                    interval="3600s",
                    timeout="2s",
                    max_consecutive_failures=max_consecutive_failures,
                ),
                restart=RestartConfig(
                    max_attempts=max_recovery_attempts,
                    delay=recovery_delay,
                ),
            )
        }
    )
    return StreamGraph(definition)


@pytest.mark.asyncio
async def test_submit_job_保存制品_跨worker下游优先部署并可查询(
    tmp_path,
    linear_graph: StreamGraph,
):
    gateway = RecordingGateway()
    manager = manager_with_workers(tmp_path, gateway)
    artifact = b"PK\x03\x04wordcount"
    digest = hashlib.sha256(artifact).hexdigest()

    job = await manager.submit_job(
        linear_graph,
        artifact,
        expected_sha256=digest,
        job_id="job-1",
    )

    assert job.status is JobStatus.RUNNING
    assert [task_id.split(":")[1] for _, task_id in gateway.deploy_calls] == [
        "output",
        "totals",
        "totals",
        "totals",
        "by_word",
        "by_word",
        "normalize",
        "normalize",
        "words",
        "words",
    ]
    assert len({worker_id for worker_id, _ in gateway.deploy_calls}) >= 2
    assert all(
        deployment.artifact.sha256 == digest
        and all(channel.target_endpoint is not None for channel in deployment.outgoing_channels)
        for deployment in gateway.deployments
    )
    assert manager.download_artifact("job-1", digest) == artifact
    assert manager.get_job("job-1") is job
    assert manager.get_execution_graph("job-1").total_tasks == 10
    assert manager.health()["running_jobs"] == 1
    view = manager.status_view("job-1")
    assert view["status"] == "RUNNING"
    assert all(item["worker_id"] is not None for item in view["tasks"])


@pytest.mark.asyncio
async def test_submit_job_同一source的subtasks并发加入消费组(
    tmp_path,
    linear_graph: StreamGraph,
) -> None:
    gateway = SourceBarrierGateway(expected_sources=2)
    manager = manager_with_workers(tmp_path, gateway)

    job = await manager.submit_job(linear_graph, b"bundle", job_id="job-source-barrier")

    assert job.status is JobStatus.RUNNING
    assert gateway.source_entries == 2
    assert {task_id for _, task_id in gateway.deploy_calls[-2:]} == {
        "job-source-barrier:words:0",
        "job-source-barrier:words:1",
    }


@pytest.mark.asyncio
async def test_jobmanager关键状态发布不可变metadata_revision(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    gateway = RecordingGateway()
    metadata = RecordingMetadataRepository()
    manager = manager_with_workers(
        tmp_path,
        gateway,
        worker_count=2,
        slots=2,
        metadata_repository=metadata,
        checkpoint_store=S3CheckpointStore(MemoryObjectStore()),
    )
    graph = checkpoint_graph(two_task_graph)
    await manager.submit_job(graph, b"bundle", job_id="job-metadata")

    assert [item.status for item in metadata.revisions[:2]] == [
        JobStatus.DEPLOYING,
        JobStatus.RUNNING,
    ]
    assert [item.revision for item in metadata.revisions[:2]] == [0, 1]
    assert metadata.revisions[-1].definition["api_version"] == "pystream/v1"

    await manager.trigger_checkpoint("job-metadata")
    checkpoint_revision = metadata.revisions[-1]
    assert checkpoint_revision.last_decided_checkpoint_id == 1
    assert checkpoint_revision.last_finalized_checkpoint_id == 1
    assert checkpoint_revision.next_checkpoint_id == 2

    await manager.cancel_job("job-metadata")
    assert metadata.revisions[-1].status is JobStatus.CANCELLED
    assert manager.status_view("job-metadata")["metadata_revision"] == 4
    await manager.close()


@pytest.mark.asyncio
async def test_jobmanager并发状态发布按revision串行化(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    gateway = RecordingGateway()
    metadata = ConcurrencyDetectingMetadataRepository()
    manager = manager_with_workers(
        tmp_path,
        gateway,
        worker_count=2,
        slots=2,
        metadata_repository=metadata,
    )
    await manager.submit_job(two_task_graph, b"bundle", job_id="job-metadata-lock")
    run = manager._runs["job-metadata-lock"]

    await asyncio.gather(
        manager._persist_run_metadata(run),
        manager._persist_run_metadata(run),
    )

    assert [item.revision for item in metadata.revisions] == [0, 1, 2, 3]
    assert run.metadata_revision == 3
    await manager.cancel_job("job-metadata-lock")
    await manager.close()


@pytest.mark.asyncio
async def test_submit_job_资源不足时拒绝且不部署(tmp_path, linear_graph: StreamGraph):
    gateway = RecordingGateway()
    manager = manager_with_workers(tmp_path, gateway, worker_count=1, slots=2)

    with pytest.raises(InsufficientSlots, match="需要 10"):
        await manager.submit_job(linear_graph, b"bundle", job_id="job-small")

    assert manager.get_job("job-small").status is JobStatus.REJECTED
    assert gateway.deploy_calls == []
    assert manager.resources()[0].used_slots == 0


@pytest.mark.asyncio
async def test_submit_job_制品摘要错误时拒绝且不占slot(tmp_path, linear_graph: StreamGraph):
    gateway = RecordingGateway()
    manager = manager_with_workers(tmp_path, gateway)

    with pytest.raises(ArtifactError, match="摘要不匹配"):
        await manager.submit_job(
            linear_graph,
            b"bundle",
            expected_sha256="0" * 64,
            job_id="job-bad-artifact",
        )

    assert manager.get_job("job-bad-artifact").status is JobStatus.REJECTED
    assert all(view.used_slots == 0 for view in manager.resources())


@pytest.mark.asyncio
async def test_submit_job_中途失败时逆序停止并释放全部slot(
    tmp_path,
    linear_graph: StreamGraph,
):
    gateway = RecordingGateway(fail_deploy_at=4)
    manager = manager_with_workers(tmp_path, gateway)

    with pytest.raises(DeploymentError, match="已回滚"):
        await manager.submit_job(linear_graph, b"bundle", job_id="job-failed")

    job = manager.get_job("job-failed")
    execution = manager.get_execution_graph("job-failed")
    assert job.status is JobStatus.FAILED
    assert "simulated deploy failure" in (job.error or "")
    assert [task_id for _, task_id in gateway.stop_calls] == [
        task_id for _, task_id in reversed(gateway.deploy_calls[:3])
    ]
    assert sum(task.status is TaskStatus.FAILED for task in execution.tasks.values()) == 1
    assert all(
        task.status in {TaskStatus.CANCELLED, TaskStatus.FAILED}
        for task in execution.tasks.values()
    )
    assert all(view.used_slots == 0 for view in manager.resources())


@pytest.mark.asyncio
async def test_cancel_job_逆部署顺序停止并释放资源(tmp_path, two_task_graph: StreamGraph):
    gateway = RecordingGateway()
    manager = manager_with_workers(tmp_path, gateway, worker_count=2, slots=2)
    await manager.submit_job(two_task_graph, b"bundle", job_id="job-cancel")

    job = await manager.cancel_job("job-cancel")

    assert job.status is JobStatus.CANCELLED
    assert [task_id for _, task_id in gateway.stop_calls] == [
        task_id for _, task_id in reversed(gateway.deploy_calls)
    ]
    assert all(view.used_slots == 0 for view in manager.resources())
    assert all(
        task.status is TaskStatus.CANCELLED and task.worker_id is None
        for task in manager.get_execution_graph("job-cancel").tasks.values()
    )


@pytest.mark.asyncio
async def test_cancel_job_停止失败时仍清理其余任务并进入failed(
    tmp_path,
    two_task_graph: StreamGraph,
):
    gateway = RecordingGateway()
    manager = manager_with_workers(tmp_path, gateway, worker_count=2, slots=2)
    await manager.submit_job(two_task_graph, b"bundle", job_id="job-cancel-fail")
    gateway.fail_stop_task = gateway.deploy_calls[-1][1]

    with pytest.raises(DeploymentError, match="取消作业"):
        await manager.cancel_job("job-cancel-fail")

    assert manager.get_job("job-cancel-fail").status is JobStatus.FAILED
    assert len(gateway.stop_calls) == 2
    assert all(view.used_slots == 0 for view in manager.resources())


@pytest.mark.asyncio
async def test_report_task_failed_停止其他任务并聚合作业失败(
    tmp_path,
    two_task_graph: StreamGraph,
):
    gateway = RecordingGateway()
    manager = manager_with_workers(tmp_path, gateway, worker_count=2, slots=2)
    await manager.submit_job(two_task_graph, b"bundle", job_id="job-report")
    failed_task_id = gateway.deploy_calls[0][1]

    job = await manager.report_task_status(
        "job-report",
        failed_task_id,
        TaskStatus.FAILED,
        "runtime disconnected",
    )

    assert job.status is JobStatus.FAILED
    assert "runtime disconnected" in (job.error or "")
    assert gateway.stop_calls == [gateway.deploy_calls[1]]
    assert all(view.used_slots == 0 for view in manager.resources())


@pytest.mark.asyncio
async def test_report_task_status_忽略旧attempt上报(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    gateway = RecordingGateway()
    manager = manager_with_workers(tmp_path, gateway, worker_count=2, slots=2)
    await manager.submit_job(two_task_graph, b"bundle", job_id="job-stale-report")
    task_id = gateway.deploy_calls[0][1]
    task = manager.get_execution_graph("job-stale-report").tasks[task_id]
    task.attempt_id = 1

    job = await manager.report_task_status(
        "job-stale-report",
        task_id,
        TaskStatus.FAILED,
        "old runtime failed",
        attempt_id=0,
    )

    assert job.status is JobStatus.RUNNING
    assert task.status is TaskStatus.RUNNING
    assert gateway.stop_calls == []


@pytest.mark.asyncio
async def test_coordinator_epoch_贯穿部署状态并fence旧上报(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    gateway = RecordingGateway()
    manager = manager_with_workers(
        tmp_path,
        gateway,
        worker_count=2,
        slots=2,
        coordinator_epoch=4,
    )
    await manager.submit_job(two_task_graph, b"bundle", job_id="job-epoch")
    task_id = gateway.deploy_calls[0][1]
    task = manager.get_execution_graph("job-epoch").tasks[task_id]

    assert {item.coordinator_epoch for item in gateway.deployments} == {4}
    assert manager.status_view("job-epoch")["coordinator_epoch"] == 4

    job = await manager.report_task_status(
        "job-epoch",
        task_id,
        TaskStatus.FAILED,
        "stale leader",
        attempt_id=0,
        coordinator_epoch=3,
    )

    assert job.status is JobStatus.RUNNING
    assert task.status is TaskStatus.RUNNING
    await manager.cancel_job("job-epoch")


@pytest.mark.asyncio
async def test_reconcile_worker_health_将失联worker上的作业置为failed(
    tmp_path,
    two_task_graph: StreamGraph,
):
    heartbeat = datetime.now(UTC)
    gateway = RecordingGateway()
    manager = manager_with_workers(
        tmp_path,
        gateway,
        worker_count=2,
        slots=2,
        heartbeat_at=heartbeat,
    )
    await manager.submit_job(two_task_graph, b"bundle", job_id="job-timeout")

    failed_jobs = await manager.reconcile_worker_health(heartbeat + timedelta(seconds=11))

    assert failed_jobs == ("job-timeout",)
    assert manager.get_job("job-timeout").status is JobStatus.FAILED
    assert "心跳超时" in (manager.get_job("job-timeout").error or "")
    assert all(view.used_slots == 0 for view in manager.resources(heartbeat))


@pytest.mark.asyncio
async def test_duplicate_job_id_和错误下载摘要被拒绝(tmp_path, two_task_graph: StreamGraph):
    gateway = RecordingGateway()
    manager = manager_with_workers(tmp_path, gateway, worker_count=2, slots=2)
    await manager.submit_job(two_task_graph, b"bundle", job_id="job-1")

    with pytest.raises(ControlPlaneError, match="已存在"):
        await manager.submit_job(two_task_graph, b"other", job_id="job-1")
    with pytest.raises(ControlPlaneError, match="不匹配"):
        manager.download_artifact("job-1", "0" * 64)


@pytest.mark.asyncio
async def test_jobmanager手动checkpoint更新状态并保留周期任务(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    gateway = RecordingGateway()
    manager = manager_with_workers(tmp_path, gateway, worker_count=2, slots=2)
    graph = checkpoint_graph(two_task_graph)
    try:
        await manager.submit_job(graph, b"bundle", job_id="job-checkpoint")

        manifest = await manager.trigger_checkpoint("job-checkpoint")

        assert manifest.checkpoint_id == 1
        checkpoint = manager.status_view("job-checkpoint")["checkpoint"]
        assert checkpoint == {
            "next_id": 2,
            "last_completed_id": 1,
            "consecutive_failures": 0,
        }
        assert manager._runs["job-checkpoint"].checkpoint_task is not None
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_checkpoint连续失败达到阈值后触发整作业恢复且编号不复用(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    gateway = RecordingGateway(fail_checkpoint=True)
    manager = manager_with_workers(tmp_path, gateway, worker_count=2, slots=2)
    graph = checkpoint_graph(two_task_graph, max_consecutive_failures=2)
    await manager.submit_job(graph, b"bundle", job_id="job-checkpoint-fail")

    with pytest.raises(CheckpointCoordinationError, match="simulated checkpoint failure"):
        await manager.trigger_checkpoint("job-checkpoint-fail")
    assert manager.get_job("job-checkpoint-fail").status is JobStatus.RUNNING

    with pytest.raises(CheckpointCoordinationError, match="simulated checkpoint failure"):
        await manager.trigger_checkpoint("job-checkpoint-fail")

    recovery = manager._runs["job-checkpoint-fail"].recovery_task
    assert recovery is not None
    await recovery

    assert manager.get_job("job-checkpoint-fail").status is JobStatus.RUNNING
    assert manager._runs["job-checkpoint-fail"].attempt_id == 1
    assert {
        checkpoint_id for action, _, checkpoint_id in gateway.checkpoint_calls if action == "arm"
    } == {1, 2}
    await manager.close()


@pytest.mark.asyncio
async def test_exactly_once_predecision失败立即触发整作业恢复(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    gateway = RecordingGateway(fail_checkpoint=True)
    manager = manager_with_workers(tmp_path, gateway, worker_count=2, slots=2)
    graph = checkpoint_graph(
        two_task_graph,
        delivery_guarantee=DeliveryGuarantee.EXACTLY_ONCE,
        max_consecutive_failures=3,
    )
    await manager.submit_job(graph, b"bundle", job_id="job-exact-fail")

    with pytest.raises(CheckpointCoordinationError, match="simulated checkpoint failure"):
        await manager.trigger_checkpoint("job-exact-fail")

    recovery = manager._runs["job-exact-fail"].recovery_task
    assert recovery is not None
    await recovery

    assert manager.get_job("job-exact-fail").status is JobStatus.RUNNING
    assert manager._runs["job-exact-fail"].attempt_id == 1
    assert manager._runs["job-exact-fail"].consecutive_checkpoint_failures == 0
    await manager.close()


@pytest.mark.asyncio
async def test_task失败从最近完整checkpoint恢复全图(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    gateway = RecordingGateway()
    manager = manager_with_workers(tmp_path, gateway, worker_count=2, slots=2)
    graph = checkpoint_graph(two_task_graph)
    await manager.submit_job(graph, b"bundle", job_id="job-recover")
    await manager.trigger_checkpoint("job-recover")
    failed_task_id = gateway.deploy_calls[0][1]

    job = await manager.report_task_status(
        "job-recover",
        failed_task_id,
        TaskStatus.FAILED,
        "runtime disconnected",
        attempt_id=0,
    )
    assert job.status is JobStatus.RECOVERING
    recovery = manager._runs["job-recover"].recovery_task
    assert recovery is not None
    await recovery

    assert job.status is JobStatus.RUNNING
    assert manager._runs["job-recover"].attempt_id == 1
    attempt_one = [item for item in gateway.deployment_attempts if item[1] == 1]
    assert len(attempt_one) == 2
    assert all(
        descriptors and {descriptor.checkpoint_id for descriptor in descriptors} == {1}
        for _, _, descriptors in attempt_one
    )
    assert all(
        task.attempt_id == 1 and task.restored_checkpoint_id == 1
        for task in manager.get_execution_graph("job-recover").tasks.values()
    )
    await manager.close()


@pytest.mark.asyncio
async def test_worker新incarnation重注册触发checkpoint作业恢复(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    gateway = RecordingGateway()
    manager = manager_with_workers(tmp_path, gateway, worker_count=2, slots=2)
    graph = checkpoint_graph(two_task_graph)
    await manager.submit_job(graph, b"bundle", job_id="job-worker-restarted")
    await manager.trigger_checkpoint("job-worker-restarted")
    worker_id = gateway.deploy_calls[0][0]
    worker = manager.registry.get(worker_id)

    registered, restarted, affected_jobs = await manager.register_worker_process(
        worker_id,
        "replacement-process",
        worker.control_address,
        worker.data_host,
        worker.data_port,
        worker.total_slots,
    )

    assert registered.incarnation_id == "replacement-process"
    assert restarted is True
    assert affected_jobs == ("job-worker-restarted",)
    recovery = manager._runs["job-worker-restarted"].recovery_task
    assert recovery is not None
    await recovery
    status = manager.status_view("job-worker-restarted")
    assert status["status"] == "RUNNING"
    assert status["attempt"] == 1
    assert {
        task["restored_checkpoint_id"] for task in status["tasks"] if isinstance(task, dict)
    } == {1}
    await manager.close()


@pytest.mark.asyncio
async def test_worker相同incarnation重复注册不触发恢复(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    gateway = RecordingGateway()
    manager = manager_with_workers(tmp_path, gateway, worker_count=2, slots=2)
    graph = checkpoint_graph(two_task_graph)
    await manager.submit_job(graph, b"bundle", job_id="job-worker-reregistered")
    worker = manager.registry.get("worker-0")

    _, restarted, affected_jobs = await manager.register_worker_process(
        worker.worker_id,
        worker.incarnation_id,
        worker.control_address,
        worker.data_host,
        worker.data_port,
        worker.total_slots,
    )

    assert restarted is False
    assert affected_jobs == ()
    assert manager.get_job("job-worker-reregistered").status is JobStatus.RUNNING
    assert manager._runs["job-worker-reregistered"].recovery_task is None
    await manager.close()


@pytest.mark.asyncio
async def test_中级作业初次部署失败进入recovery后成功(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    gateway = RecordingGateway(fail_deploy_at=1)
    manager = manager_with_workers(tmp_path, gateway, worker_count=2, slots=2)
    graph = checkpoint_graph(two_task_graph, max_recovery_attempts=1)

    job = await manager.submit_job(graph, b"bundle", job_id="job-initial-recovery")
    assert job.status is JobStatus.RECOVERING
    recovery = manager._runs["job-initial-recovery"].recovery_task
    assert recovery is not None
    await recovery

    assert job.status is JobStatus.RUNNING
    assert manager._runs["job-initial-recovery"].attempt_id == 1
    await manager.close()


@pytest.mark.asyncio
async def test_recovery重试耗尽后failed并释放资源(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    gateway = RecordingGateway(fail_deploy_attempts={1, 2})
    manager = manager_with_workers(tmp_path, gateway, worker_count=2, slots=2)
    graph = checkpoint_graph(two_task_graph, max_recovery_attempts=2)
    await manager.submit_job(graph, b"bundle", job_id="job-retry-exhausted")
    task_id = gateway.deploy_calls[0][1]

    await manager.report_task_status(
        "job-retry-exhausted",
        task_id,
        TaskStatus.FAILED,
        "worker lost",
        attempt_id=0,
    )
    recovery = manager._runs["job-retry-exhausted"].recovery_task
    assert recovery is not None
    await recovery

    assert manager.get_job("job-retry-exhausted").status is JobStatus.FAILED
    assert manager._runs["job-retry-exhausted"].attempt_id == 2
    assert all(view.used_slots == 0 for view in manager.resources())
    await manager.close()


@pytest.mark.asyncio
async def test_recovering期间cancel阻止后续attempt(
    tmp_path,
    two_task_graph: StreamGraph,
) -> None:
    gateway = RecordingGateway()
    manager = manager_with_workers(tmp_path, gateway, worker_count=2, slots=2)
    graph = checkpoint_graph(
        two_task_graph,
        max_recovery_attempts=3,
        recovery_delay="10s",
    )
    await manager.submit_job(graph, b"bundle", job_id="job-recovery-cancel")
    task_id = gateway.deploy_calls[0][1]
    await manager.report_task_status(
        "job-recovery-cancel",
        task_id,
        TaskStatus.FAILED,
        "worker lost",
        attempt_id=0,
    )

    job = await manager.cancel_job("job-recovery-cancel")

    assert job.status is JobStatus.CANCELLED
    assert manager._runs["job-recovery-cancel"].attempt_id == 0
    assert manager._runs["job-recovery-cancel"].recovery_task is None
    await manager.close()
