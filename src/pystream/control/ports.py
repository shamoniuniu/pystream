"""控制面到 Worker 与制品存储的可替换端口。

JobManager 只依赖这些协议。后续 HTTP Worker 客户端和 Task 3 的 ZIP 制品
实现可独立接入，不需要修改调度、状态机或部署回滚逻辑。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from pystream.checkpoint import TaskSnapshotDescriptor
from pystream.control.models import (
    ArtifactDescriptor,
    PhysicalChannel,
    TaskInstance,
    WorkerNode,
)


class ArtifactRepository(Protocol):
    """不可变制品存储端口。"""

    def put(
        self,
        job_id: str,
        content: bytes,
        expected_sha256: str | None = None,
    ) -> ArtifactDescriptor:
        """保存制品并返回下载标识。"""

    def read(self, descriptor: ArtifactDescriptor) -> bytes:
        """按 job_id 与摘要读取并再次验证制品。"""


@dataclass(frozen=True, slots=True)
class TaskDeployment:
    """发送给 Worker 的完整任务部署描述。"""

    task: TaskInstance
    artifact: ArtifactDescriptor
    incoming_channels: tuple[PhysicalChannel, ...]
    outgoing_channels: tuple[PhysicalChannel, ...]
    restore_descriptors: tuple[TaskSnapshotDescriptor, ...] = ()
    coordinator_epoch: int = 0

    def __post_init__(self) -> None:
        if (
            isinstance(self.coordinator_epoch, bool)
            or not isinstance(self.coordinator_epoch, int)
            or self.coordinator_epoch < 0
        ):
            raise ValueError("coordinator_epoch 必须是非负整数")


class WorkerGateway(Protocol):
    """JobManager 调用 Worker 的异步控制端口。"""

    async def deploy_task(self, worker: WorkerNode, deployment: TaskDeployment) -> None:
        """部署并启动一个物理任务。"""

    async def stop_task(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        coordinator_epoch: int = 0,
    ) -> None:
        """停止物理任务；Worker 端实现应保持幂等。"""

    async def arm_checkpoint(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> None:
        """准备 Task 的 Checkpoint 状态机。"""

    async def trigger_checkpoint(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> TaskSnapshotDescriptor:
        """触发 Source Task 停流和快照。"""

    async def wait_checkpoint(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> TaskSnapshotDescriptor:
        """等待普通 Task 收齐 DRAIN 并完成快照。"""

    async def complete_checkpoint(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> None:
        """确认全图 manifest 已完成。"""

    async def abort_checkpoint(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> None:
        """中止 Task 的活动 Checkpoint。"""


__all__ = ["ArtifactRepository", "TaskDeployment", "WorkerGateway"]
