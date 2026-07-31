"""JobManager 的停流协调 Checkpoint 协议。"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from functools import partial

from pystream.api import OperatorType
from pystream.checkpoint import (
    CheckpointManifest,
    LocalCheckpointStore,
)
from pystream.control.errors import ControlPlaneError
from pystream.control.execution import ExecutionGraph
from pystream.control.models import TaskInstance, TaskStatus, WorkerNode
from pystream.control.ports import WorkerGateway


class CheckpointCoordinationError(ControlPlaneError):
    """全图 Checkpoint 未能在超时内完整完成。"""


class CheckpointCoordinator:
    """按下游到上游 arm，并以 manifest-last 完成一个停流 Checkpoint。"""

    def __init__(
        self,
        store: LocalCheckpointStore,
        worker_gateway: WorkerGateway,
        worker_lookup: Callable[[str], WorkerNode],
    ) -> None:
        self.store = store
        self.worker_gateway = worker_gateway
        self.worker_lookup = worker_lookup

    async def run(
        self,
        graph: ExecutionGraph,
        *,
        checkpoint_id: int,
        attempt_id: int,
        coordinator_epoch: int = 0,
        timeout: float,
    ) -> CheckpointManifest:
        """完成一次全图快照；任一失败都会 abort 已 arm 的 Task。"""
        if timeout <= 0:
            raise ValueError("Checkpoint timeout 必须大于 0")
        tasks = graph.deployment_order()
        if not tasks:
            raise CheckpointCoordinationError("Checkpoint 执行图不能为空")
        if any(task.status is not TaskStatus.RUNNING for task in tasks):
            raise CheckpointCoordinationError("Checkpoint 只允许在全部 Task RUNNING 时执行")
        armed: list[TaskInstance] = []

        async def execute() -> CheckpointManifest:
            for task in tasks:
                # 请求结果未知时 Worker 可能已经 arm; 必须把本次尝试纳入 abort。
                armed.append(task)
                await self.worker_gateway.arm_checkpoint(
                    self._worker(task),
                    task.task_id,
                    task.attempt_id,
                    checkpoint_id,
                    coordinator_epoch,
                )

            source_tasks = tuple(
                task for task in tasks if task.operator_type is OperatorType.SOURCE
            )
            if not source_tasks:
                raise CheckpointCoordinationError("Checkpoint 执行图缺少 Source Task")
            source_snapshots = await asyncio.gather(
                *(
                    self.worker_gateway.trigger_checkpoint(
                        self._worker(task),
                        task.task_id,
                        task.attempt_id,
                        checkpoint_id,
                        coordinator_epoch,
                    )
                    for task in source_tasks
                )
            )
            operator_tasks = tuple(
                task for task in tasks if task.operator_type is not OperatorType.SOURCE
            )
            operator_snapshots = await asyncio.gather(
                *(
                    self.worker_gateway.wait_checkpoint(
                        self._worker(task),
                        task.task_id,
                        task.attempt_id,
                        checkpoint_id,
                        coordinator_epoch,
                    )
                    for task in operator_tasks
                )
            )
            snapshots = tuple(source_snapshots) + tuple(operator_snapshots)
            manifest = await _uncancellable_to_thread(
                partial(
                    self.store.complete_checkpoint,
                    job_id=graph.job_id,
                    checkpoint_id=checkpoint_id,
                    attempt_id=attempt_id,
                    coordinator_epoch=coordinator_epoch,
                    expected_task_ids=set(graph.tasks),
                    snapshots=snapshots,
                )
            )
            for task in tasks:
                await self.worker_gateway.complete_checkpoint(
                    self._worker(task),
                    task.task_id,
                    task.attempt_id,
                    checkpoint_id,
                    coordinator_epoch,
                )
            return manifest

        try:
            return await asyncio.wait_for(execute(), timeout=timeout)
        except asyncio.CancelledError:
            await self._abort(
                graph.job_id,
                checkpoint_id,
                attempt_id,
                coordinator_epoch,
                armed,
                timeout,
            )
            raise
        except Exception as exc:
            abort_errors = await self._abort(
                graph.job_id,
                checkpoint_id,
                attempt_id,
                coordinator_epoch,
                armed,
                timeout,
            )
            details = f"{type(exc).__name__}: {exc}"
            if abort_errors:
                details += "; abort errors: " + "; ".join(abort_errors)
            raise CheckpointCoordinationError(
                f"Checkpoint {checkpoint_id} 协调失败: {details}"
            ) from exc

    async def _abort(
        self,
        job_id: str,
        checkpoint_id: int,
        attempt_id: int,
        coordinator_epoch: int,
        armed: list[TaskInstance],
        timeout: float,
    ) -> list[str]:
        errors: list[str] = []

        async def abort_task(task: TaskInstance) -> None:
            try:
                await self.worker_gateway.abort_checkpoint(
                    self._worker(task),
                    task.task_id,
                    task.attempt_id,
                    checkpoint_id,
                    coordinator_epoch,
                )
            except Exception as exc:
                errors.append(f"{task.task_id}: {type(exc).__name__}: {exc}")

        try:
            await asyncio.wait_for(
                asyncio.gather(*(abort_task(task) for task in reversed(armed))),
                timeout=min(timeout, 5.0),
            )
        except TimeoutError:
            errors.append("abort timeout")
        try:
            self.store.abort_checkpoint(job_id, checkpoint_id, attempt_id)
        except Exception as exc:
            errors.append(f"store: {type(exc).__name__}: {exc}")
        return errors

    def _worker(self, task: TaskInstance) -> WorkerNode:
        if task.worker_id is None:
            raise CheckpointCoordinationError(f"Task {task.task_id} 未分配 Worker")
        return self.worker_lookup(task.worker_id)


async def _uncancellable_to_thread(operation):
    """取消时等待原子文件操作结束，避免后台线程在 abort 后重建快照。"""
    task = asyncio.create_task(asyncio.to_thread(operation))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


__all__ = ["CheckpointCoordinationError", "CheckpointCoordinator"]
