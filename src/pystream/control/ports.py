"""控制面到 Worker 与制品存储的可替换端口。

JobManager 只依赖这些协议。后续 HTTP Worker 客户端和 Task 3 的 ZIP 制品
实现可独立接入，不需要修改调度、状态机或部署回滚逻辑。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

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


class WorkerGateway(Protocol):
    """JobManager 调用 Worker 的异步控制端口。"""

    async def deploy_task(self, worker: WorkerNode, deployment: TaskDeployment) -> None:
        """部署并启动一个物理任务。"""

    async def stop_task(self, worker: WorkerNode, task_id: str) -> None:
        """停止物理任务；Worker 端实现应保持幂等。"""


__all__ = ["ArtifactRepository", "TaskDeployment", "WorkerGateway"]
