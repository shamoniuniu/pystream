"""Worker 注册、心跳资源视图和确定性 slot 调度。

调度采用“先全量规划、再一次性预留”的两阶段方式。资源不足时不会修改
任何 Worker 或 TaskInstance；成功时按当前占用比例和 worker_id 稳定选择。
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from fractions import Fraction

from pystream.control.errors import InsufficientSlots, WorkerNotFound
from pystream.control.execution import ExecutionGraph
from pystream.control.models import ResourceView, TaskInstance, WorkerNode


class WorkerRegistry:
    """保存 Worker 注册信息、心跳和 slot 占用。"""

    def __init__(self, heartbeat_timeout: timedelta = timedelta(seconds=15)) -> None:
        if heartbeat_timeout.total_seconds() <= 0:
            raise ValueError("heartbeat_timeout 必须大于 0")
        self.heartbeat_timeout = heartbeat_timeout
        self._workers: dict[str, WorkerNode] = {}

    def register(
        self,
        worker_id: str,
        control_address: str,
        data_host: str,
        data_port: int,
        total_slots: int,
        heartbeat_at: datetime | None = None,
    ) -> WorkerNode:
        """注册新 Worker，或在不破坏占用的前提下刷新已有 Worker。"""
        candidate = WorkerNode.create(
            worker_id=worker_id,
            control_address=control_address,
            data_host=data_host,
            data_port=data_port,
            total_slots=total_slots,
            heartbeat_at=heartbeat_at,
        )
        existing = self._workers.get(worker_id)
        if existing is not None and existing.used_slots:
            if total_slots != existing.total_slots:
                raise ValueError("存在运行任务时不能修改 Worker slot 数")
            existing.control_address = candidate.control_address
            existing.data_host = candidate.data_host
            existing.data_port = candidate.data_port
            existing.last_heartbeat = candidate.last_heartbeat
            return existing

        self._workers[worker_id] = candidate
        return candidate

    def heartbeat(self, worker_id: str, at: datetime | None = None) -> WorkerNode:
        """更新已注册 Worker 心跳。"""
        try:
            worker = self._workers[worker_id]
        except KeyError as exc:
            raise WorkerNotFound(f"Worker {worker_id!r} 尚未注册") from exc
        worker.heartbeat(at)
        return worker

    def get(self, worker_id: str) -> WorkerNode:
        """按 ID 获取 Worker。"""
        try:
            return self._workers[worker_id]
        except KeyError as exc:
            raise WorkerNotFound(f"Worker {worker_id!r} 尚未注册") from exc

    def all(self) -> tuple[WorkerNode, ...]:
        """按 worker_id 返回所有 Worker。"""
        return tuple(self._workers[key] for key in sorted(self._workers))

    def healthy(self, now: datetime | None = None) -> tuple[WorkerNode, ...]:
        """按 worker_id 返回未超时 Worker。"""
        observed_at = now or datetime.now(UTC)
        return tuple(
            worker
            for worker in self.all()
            if worker.is_healthy(observed_at, self.heartbeat_timeout)
        )

    def resource_view(self, now: datetime | None = None) -> tuple[ResourceView, ...]:
        """生成不会暴露可变 slot 对象的资源快照。"""
        observed_at = now or datetime.now(UTC)
        return tuple(
            ResourceView(
                worker_id=worker.worker_id,
                healthy=worker.is_healthy(observed_at, self.heartbeat_timeout),
                total_slots=worker.total_slots,
                used_slots=worker.used_slots,
                available_slots=worker.available_slots,
                control_address=worker.control_address,
                data_host=worker.data_host,
                data_port=worker.data_port,
                last_heartbeat=worker.last_heartbeat,
            )
            for worker in self.all()
        )


class SlotScheduler:
    """按占用比例进行稳定、无部分结果的 slot 调度。"""

    def schedule(
        self,
        execution_graph: ExecutionGraph,
        registry: WorkerRegistry,
        now: datetime | None = None,
    ) -> None:
        """为执行图全部任务预留 slot，并绑定通道端点。"""
        workers = [worker for worker in registry.healthy(now) if worker.available_slots > 0]
        required = execution_graph.total_tasks
        available = sum(worker.available_slots for worker in workers)
        if available < required:
            raise InsufficientSlots(required=required, available=available)

        planned: dict[str, list[TaskInstance]] = {worker.worker_id: [] for worker in workers}
        remaining = {worker.worker_id: worker.available_slots for worker in workers}
        tasks = list(execution_graph.tasks.values())
        job_workers: set[str] = set()
        spread_target = min(2, len(tasks), len(workers))
        for task_index, task in enumerate(tasks):
            candidates = [worker for worker in workers if remaining[worker.worker_id] > 0]
            if task_index < spread_target:
                candidates = [
                    worker for worker in candidates if worker.worker_id not in job_workers
                ]
            selected = min(
                candidates,
                key=lambda worker: (
                    Fraction(
                        worker.used_slots + len(planned[worker.worker_id]),
                        worker.total_slots,
                    ),
                    worker.used_slots + len(planned[worker.worker_id]),
                    worker.worker_id,
                ),
            )
            planned[selected.worker_id].append(task)
            remaining[selected.worker_id] -= 1
            job_workers.add(selected.worker_id)

        assigned: list[TaskInstance] = []
        try:
            for worker in workers:
                for task in planned[worker.worker_id]:
                    slot_index = worker.reserve(task.task_id)
                    task.assign(worker.worker_id, slot_index)
                    assigned.append(task)
            execution_graph.bind_endpoints({worker.worker_id: worker for worker in workers})
        except Exception:
            for task in assigned:
                worker = registry.get(task.worker_id or "")
                worker.release(task.task_id)
                task.clear_assignment()
            raise

    def release(self, execution_graph: ExecutionGraph, registry: WorkerRegistry) -> None:
        """释放执行图的全部 slot，重复调用保持幂等。"""
        for task in execution_graph.tasks.values():
            if task.worker_id is not None:
                registry.get(task.worker_id).release(task.task_id)
                task.clear_assignment()


__all__ = ["SlotScheduler", "WorkerRegistry"]
