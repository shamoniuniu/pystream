"""逻辑 StreamGraph 到物理执行图的展开。

FORWARD 只连接同编号 subtask；REBALANCE 与 HASH 创建全连接物理通道，由
运行时在这些候选通道中执行轮询或 key 路由。通道端点在调度完成后绑定。
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from pystream.api import Partitioning, StreamGraph
from pystream.control.models import PhysicalChannel, TaskEndpoint, TaskInstance, WorkerNode


@dataclass(slots=True)
class ExecutionGraph:
    """一个作业的物理任务、通道和稳定部署顺序。"""

    job_id: str
    topological_order: tuple[str, ...]
    tasks: dict[str, TaskInstance]
    channels: tuple[PhysicalChannel, ...]
    _tasks_by_operator: dict[str, tuple[str, ...]] = field(repr=False)

    @property
    def total_tasks(self) -> int:
        """返回物理任务总数。"""
        return len(self.tasks)

    def operator_tasks(self, operator_id: str) -> tuple[TaskInstance, ...]:
        """按 subtask 编号返回逻辑算子的物理任务。"""
        return tuple(self.tasks[task_id] for task_id in self._tasks_by_operator[operator_id])

    def deployment_order(self) -> tuple[TaskInstance, ...]:
        """按下游到上游、同算子 subtask 升序返回任务。"""
        return tuple(
            task
            for operator_id in reversed(self.topological_order)
            for task in self.operator_tasks(operator_id)
        )

    def incoming_channels(self, task_id: str) -> tuple[PhysicalChannel, ...]:
        """返回任务的入通道。"""
        return tuple(channel for channel in self.channels if channel.target_task_id == task_id)

    def outgoing_channels(self, task_id: str) -> tuple[PhysicalChannel, ...]:
        """返回任务的出通道。"""
        return tuple(channel for channel in self.channels if channel.source_task_id == task_id)

    def bind_endpoints(self, workers: dict[str, WorkerNode]) -> None:
        """将调度结果转换成下游数据面地址。"""
        bound: list[PhysicalChannel] = []
        for channel in self.channels:
            target = self.tasks[channel.target_task_id]
            if target.worker_id is None:
                raise ValueError(f"任务 {target.task_id} 尚未完成调度")
            worker = workers[target.worker_id]
            endpoint = TaskEndpoint(
                task_id=target.task_id,
                host=worker.data_host,
                port=worker.data_port,
            )
            bound.append(replace(channel, target_endpoint=endpoint))
        self.channels = tuple(bound)

    def reset_for_attempt(
        self,
        attempt_id: int,
        restored_checkpoint_id: int | None,
    ) -> None:
        """释放完成后把全部逻辑 Task 重置为同一新 attempt。"""
        for task in self.tasks.values():
            task.reset_for_attempt(attempt_id, restored_checkpoint_id)


def _task_id(job_id: str, operator_id: str, subtask_index: int) -> str:
    return f"{job_id}:{operator_id}:{subtask_index}"


def _channel_id(source_task_id: str, target_task_id: str) -> str:
    return f"{source_task_id}->{target_task_id}"


def build_execution_graph(job_id: str, graph: StreamGraph) -> ExecutionGraph:
    """按逻辑并发度展开任务，并生成确定性物理通道。"""
    tasks: dict[str, TaskInstance] = {}
    tasks_by_operator: dict[str, tuple[str, ...]] = {}
    for operator_id in graph.topological_order:
        operator = graph.operator(operator_id)
        operator_task_ids: list[str] = []
        for subtask_index in range(operator.parallelism):
            task_id = _task_id(job_id, operator_id, subtask_index)
            tasks[task_id] = TaskInstance(
                task_id=task_id,
                job_id=job_id,
                operator_id=operator_id,
                operator_type=operator.type,
                subtask_index=subtask_index,
                parallelism=operator.parallelism,
            )
            operator_task_ids.append(task_id)
        tasks_by_operator[operator_id] = tuple(operator_task_ids)

    channels: list[PhysicalChannel] = []
    for edge in graph.edges:
        source_ids = tasks_by_operator[edge.source_id]
        target_ids = tasks_by_operator[edge.target_id]
        if edge.partitioning is Partitioning.FORWARD:
            if len(source_ids) != len(target_ids):
                raise ValueError("FORWARD 边要求上下游并发度相同")
            pairs = zip(source_ids, target_ids, strict=True)
        else:
            pairs = (
                (source_task_id, target_task_id)
                for source_task_id in source_ids
                for target_task_id in target_ids
            )
        for source_task_id, target_task_id in pairs:
            channels.append(
                PhysicalChannel(
                    channel_id=_channel_id(source_task_id, target_task_id),
                    source_task_id=source_task_id,
                    target_task_id=target_task_id,
                    partitioning=edge.partitioning,
                )
            )

    return ExecutionGraph(
        job_id=job_id,
        topological_order=graph.topological_order,
        tasks=tasks,
        channels=tuple(channels),
        _tasks_by_operator=tasks_by_operator,
    )


__all__ = ["ExecutionGraph", "build_execution_graph"]
