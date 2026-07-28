"""JobManager 部署编排、回滚、取消和状态聚合测试。"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

import pytest

from pystream.api import StreamGraph
from pystream.control import (
    ArtifactError,
    ControlPlaneError,
    DeploymentError,
    InsufficientSlots,
    JobManager,
    JobStatus,
    LocalArtifactRepository,
    TaskDeployment,
    TaskStatus,
    WorkerNode,
)


class RecordingGateway:
    """记录部署/停止顺序并支持稳定故障注入。"""

    def __init__(
        self,
        *,
        fail_deploy_at: int | None = None,
        fail_stop_task: str | None = None,
    ) -> None:
        self.fail_deploy_at = fail_deploy_at
        self.fail_stop_task = fail_stop_task
        self.deploy_calls: list[tuple[str, str]] = []
        self.deployments: list[TaskDeployment] = []
        self.stop_calls: list[tuple[str, str]] = []

    async def deploy_task(self, worker: WorkerNode, deployment: TaskDeployment) -> None:
        call_number = len(self.deploy_calls) + 1
        self.deploy_calls.append((worker.worker_id, deployment.task.task_id))
        self.deployments.append(deployment)
        if self.fail_deploy_at == call_number:
            raise ConnectionError("simulated deploy failure")

    async def stop_task(self, worker: WorkerNode, task_id: str) -> None:
        self.stop_calls.append((worker.worker_id, task_id))
        if task_id == self.fail_stop_task:
            raise ConnectionError("simulated stop failure")


def manager_with_workers(
    tmp_path,
    gateway: RecordingGateway,
    *,
    worker_count: int = 3,
    slots: int = 4,
    heartbeat_at: datetime | None = None,
) -> JobManager:
    """创建带本地制品仓库和固定 Worker 的 JobManager。"""
    manager = JobManager(
        LocalArtifactRepository(tmp_path / "artifacts"),
        gateway,
        heartbeat_timeout=timedelta(seconds=10),
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
