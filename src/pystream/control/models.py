"""JobManager 控制面的领域模型与状态机。

本模块只保存控制面状态，不执行网络请求。Job、TaskInstance 和 WorkerNode
分别拥有自己的合法转换或资源不变量，避免协调服务产生隐式状态跳跃。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from pystream.api import OperatorType, Partitioning
from pystream.control.errors import InvalidStateTransition


class JobStatus(StrEnum):
    """第一阶段作业状态。"""

    SUBMITTED = "SUBMITTED"
    VALIDATING = "VALIDATING"
    REJECTED = "REJECTED"
    DEPLOYING = "DEPLOYING"
    RUNNING = "RUNNING"
    FAILING = "FAILING"
    FAILED = "FAILED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"


class TaskStatus(StrEnum):
    """物理任务状态。"""

    CREATED = "CREATED"
    SCHEDULED = "SCHEDULED"
    DEPLOYING = "DEPLOYING"
    RUNNING = "RUNNING"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


_JOB_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.SUBMITTED: frozenset({JobStatus.VALIDATING}),
    JobStatus.VALIDATING: frozenset({JobStatus.DEPLOYING, JobStatus.REJECTED}),
    JobStatus.REJECTED: frozenset(),
    JobStatus.DEPLOYING: frozenset({JobStatus.RUNNING, JobStatus.FAILING}),
    JobStatus.RUNNING: frozenset({JobStatus.CANCELLING, JobStatus.FAILING}),
    JobStatus.FAILING: frozenset({JobStatus.FAILED}),
    JobStatus.FAILED: frozenset(),
    JobStatus.CANCELLING: frozenset({JobStatus.CANCELLED, JobStatus.FAILING}),
    JobStatus.CANCELLED: frozenset(),
}

_TASK_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.CREATED: frozenset({TaskStatus.SCHEDULED, TaskStatus.FAILED}),
    TaskStatus.SCHEDULED: frozenset(
        {TaskStatus.DEPLOYING, TaskStatus.CANCELLED, TaskStatus.FAILED}
    ),
    TaskStatus.DEPLOYING: frozenset({TaskStatus.RUNNING, TaskStatus.CANCELLING, TaskStatus.FAILED}),
    TaskStatus.RUNNING: frozenset({TaskStatus.CANCELLING, TaskStatus.FAILED}),
    TaskStatus.CANCELLING: frozenset({TaskStatus.CANCELLED, TaskStatus.FAILED}),
    TaskStatus.CANCELLED: frozenset(),
    TaskStatus.FAILED: frozenset(),
}


@dataclass(slots=True)
class Job:
    """作业身份、状态和最后错误。"""

    job_id: str
    name: str
    status: JobStatus = JobStatus.SUBMITTED
    error: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def transition(self, target: JobStatus, error: str | None = None) -> None:
        """执行一条声明过的状态转换。"""
        if target not in _JOB_TRANSITIONS[self.status]:
            raise InvalidStateTransition(f"作业不能从 {self.status} 转换到 {target}")
        self.status = target
        self.error = error
        self.updated_at = datetime.now(UTC)


@dataclass(slots=True)
class WorkerSlot:
    """Worker 上一个可分配 slot。"""

    index: int
    task_id: str | None = None

    @property
    def available(self) -> bool:
        """返回 slot 是否空闲。"""
        return self.task_id is None


@dataclass(slots=True)
class WorkerNode:
    """注册到 JobManager 的 Worker 资源与地址。"""

    worker_id: str
    control_address: str
    data_host: str
    data_port: int
    slots: list[WorkerSlot]
    last_heartbeat: datetime = field(default_factory=lambda: datetime.now(UTC))

    @classmethod
    def create(
        cls,
        worker_id: str,
        control_address: str,
        data_host: str,
        data_port: int,
        total_slots: int,
        heartbeat_at: datetime | None = None,
    ) -> WorkerNode:
        """校验注册参数并创建 Worker。"""
        if not worker_id:
            raise ValueError("worker_id 不能为空")
        if not control_address or not data_host:
            raise ValueError("Worker 地址不能为空")
        if not 1 <= data_port <= 65535:
            raise ValueError("data_port 必须位于 1..65535")
        if total_slots < 1:
            raise ValueError("total_slots 必须大于 0")
        return cls(
            worker_id=worker_id,
            control_address=control_address,
            data_host=data_host,
            data_port=data_port,
            slots=[WorkerSlot(index=index) for index in range(total_slots)],
            last_heartbeat=heartbeat_at or datetime.now(UTC),
        )

    @property
    def total_slots(self) -> int:
        """返回 slot 总数。"""
        return len(self.slots)

    @property
    def used_slots(self) -> int:
        """返回已占用 slot 数。"""
        return sum(not slot.available for slot in self.slots)

    @property
    def available_slots(self) -> int:
        """返回可用 slot 数。"""
        return self.total_slots - self.used_slots

    def heartbeat(self, at: datetime | None = None) -> None:
        """更新最后心跳时间。"""
        self.last_heartbeat = at or datetime.now(UTC)

    def is_healthy(self, now: datetime, timeout: timedelta) -> bool:
        """根据心跳超时判断 Worker 是否健康。"""
        return now - self.last_heartbeat <= timeout

    def reserve(self, task_id: str) -> int:
        """占用编号最小的空闲 slot，返回 slot 编号。"""
        for slot in self.slots:
            if slot.available:
                slot.task_id = task_id
                return slot.index
        raise InvalidStateTransition(f"Worker {self.worker_id} 没有可用 slot")

    def release(self, task_id: str) -> None:
        """释放指定任务占用的 slot；重复释放保持幂等。"""
        for slot in self.slots:
            if slot.task_id == task_id:
                slot.task_id = None
                return


@dataclass(slots=True)
class TaskInstance:
    """逻辑算子按并发度展开后的物理任务。"""

    task_id: str
    job_id: str
    operator_id: str
    operator_type: OperatorType
    subtask_index: int
    parallelism: int
    status: TaskStatus = TaskStatus.CREATED
    worker_id: str | None = None
    slot_index: int | None = None
    error: str | None = None

    def transition(self, target: TaskStatus, error: str | None = None) -> None:
        """执行一条声明过的任务状态转换。"""
        if target not in _TASK_TRANSITIONS[self.status]:
            raise InvalidStateTransition(f"任务不能从 {self.status} 转换到 {target}")
        self.status = target
        self.error = error

    def assign(self, worker_id: str, slot_index: int) -> None:
        """记录调度结果并进入 SCHEDULED。"""
        if self.status is not TaskStatus.CREATED:
            raise InvalidStateTransition("只有 CREATED 任务可以分配 slot")
        self.worker_id = worker_id
        self.slot_index = slot_index
        self.transition(TaskStatus.SCHEDULED)

    def clear_assignment(self) -> None:
        """清除 Worker/slot 引用，保留任务终态供查询。"""
        self.worker_id = None
        self.slot_index = None


@dataclass(frozen=True, slots=True)
class TaskEndpoint:
    """物理任务的数据面地址。"""

    task_id: str
    host: str
    port: int


@dataclass(frozen=True, slots=True)
class PhysicalChannel:
    """两个物理任务之间的有向通道。"""

    channel_id: str
    source_task_id: str
    target_task_id: str
    partitioning: Partitioning
    target_endpoint: TaskEndpoint | None = None


@dataclass(frozen=True, slots=True)
class ArtifactDescriptor:
    """不可变作业制品的下载标识。"""

    job_id: str
    sha256: str
    size: int


@dataclass(frozen=True, slots=True)
class ResourceView:
    """供状态接口展示的 Worker 资源快照。"""

    worker_id: str
    healthy: bool
    total_slots: int
    used_slots: int
    available_slots: int
    control_address: str
    data_host: str
    data_port: int
    last_heartbeat: datetime


__all__ = [
    "ArtifactDescriptor",
    "Job",
    "JobStatus",
    "PhysicalChannel",
    "ResourceView",
    "TaskEndpoint",
    "TaskInstance",
    "TaskStatus",
    "WorkerNode",
    "WorkerSlot",
]
