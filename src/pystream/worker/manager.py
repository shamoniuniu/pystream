"""Worker 本地制品、UDF 和 TaskRuntime 生命周期管理。

一个作业制品在 Worker 上按 job_id/sha256 只下载并安全解压一次；每个物理任务
仍创建独立 UDFLoader 命名空间。任务异常通过 StatusReporter 上报 JobManager，
由控制面执行整作业失败和 slot 清理。
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from pystream.api import (
    DeliveryGuarantee,
    FileSinkConfig,
    KafkaSourceConfig,
    OperatorType,
    load_stream_graph,
)
from pystream.artifact import UDFKind, UDFLoader, extract_job_bundle, verify_job_bundle
from pystream.checkpoint import LocalCheckpointStore, TaskSnapshotDescriptor
from pystream.control import ArtifactDescriptor, TaskDeployment
from pystream.operators import (
    FileSinkOperator,
    KafkaConsumerFactory,
    KafkaJsonSource,
    KeyByOperator,
    MapOperator,
    OperatorContext,
    ReduceWindowOperator,
    SystemClock,
)
from pystream.runtime import DataPlaneServer, RuntimeSnapshot, TaskRuntime, TaskRuntimeState


class ArtifactFetcher(Protocol):
    """Worker 下载不可变 ZIP 所需的异步端口。"""

    async def fetch(self, descriptor: ArtifactDescriptor) -> bytes:
        """返回与描述符摘要和大小一致的 ZIP bytes。"""


class StatusReporter(Protocol):
    """Worker 向 JobManager 上报运行时失败的端口。"""

    async def report_task_failed(
        self,
        job_id: str,
        task_id: str,
        attempt_id: int,
        error: str,
        *,
        coordinator_epoch: int = 0,
    ) -> None:
        """报告任务失败；控制面负责停止其余任务。"""


class WorkerTaskError(RuntimeError):
    """Worker 无法准备、启动或停止物理任务。"""


class WorkerTaskManager:
    """管理一个 Worker 进程内的全部 TaskRuntime。"""

    def __init__(
        self,
        worker_id: str,
        work_root: str | Path,
        data_server: DataPlaneServer,
        artifact_fetcher: ArtifactFetcher,
        *,
        status_reporter: StatusReporter | None = None,
        consumer_factory: KafkaConsumerFactory | None = None,
        clock_factory: Callable[[], Any] = SystemClock,
        runtime_options: dict[str, Any] | None = None,
        checkpoint_root: str | Path | None = None,
    ) -> None:
        if not worker_id:
            raise ValueError("worker_id 不能为空")
        self.worker_id = worker_id
        self.work_root = Path(work_root).resolve()
        self.work_root.mkdir(parents=True, exist_ok=True)
        self.data_server = data_server
        self.artifact_fetcher = artifact_fetcher
        self.status_reporter = status_reporter
        self.consumer_factory = consumer_factory
        self.clock_factory = clock_factory
        self.runtime_options = dict(runtime_options or {})
        self.checkpoint_store = LocalCheckpointStore(
            checkpoint_root or self.work_root / "checkpoints"
        )
        self._runtimes: dict[str, TaskRuntime] = {}
        self._highest_attempts: dict[str, int] = {}
        self._highest_coordinator_epoch = -1
        self._deployment_lock = asyncio.Lock()
        self._artifact_locks: dict[tuple[str, str], asyncio.Lock] = {}

    @property
    def task_count(self) -> int:
        """返回保留状态的本地任务数。"""
        return len(self._runtimes)

    async def deploy(self, deployment: TaskDeployment) -> RuntimeSnapshot:
        """准备制品、构建算子并启动任务；同一 task_id 重复请求保持幂等。"""
        task = deployment.task
        if task.worker_id != self.worker_id:
            raise WorkerTaskError(
                f"任务 {task.task_id} 分配给 {task.worker_id!r}, 不能部署到 {self.worker_id!r}"
            )
        async with self._deployment_lock:
            if deployment.coordinator_epoch < self._highest_coordinator_epoch:
                raise WorkerTaskError(
                    f"拒绝旧 coordinator epoch {deployment.coordinator_epoch}, "
                    f"Worker 当前最高 epoch={self._highest_coordinator_epoch}"
                )
            highest_attempt = self._highest_attempts.get(task.task_id, -1)
            if task.attempt_id < highest_attempt:
                raise WorkerTaskError(
                    f"拒绝旧 attempt {task.attempt_id}, "
                    f"Task {task.task_id} 当前最高 attempt={highest_attempt}"
                )
            existing = self._runtimes.get(task.task_id)
            if existing is not None:
                existing_attempt = existing.deployment.task.attempt_id
                existing_epoch = existing.deployment.coordinator_epoch
                if deployment.coordinator_epoch < existing_epoch:
                    raise WorkerTaskError(
                        f"拒绝旧 coordinator epoch {deployment.coordinator_epoch}, "
                        f"运行中 epoch={existing_epoch}"
                    )
                if (
                    deployment.coordinator_epoch == existing_epoch
                    and task.attempt_id < existing_attempt
                ):
                    raise WorkerTaskError(
                        f"拒绝旧 attempt {task.attempt_id}, 运行中 attempt={existing_attempt}"
                    )
                if (
                    deployment.coordinator_epoch == existing_epoch
                    and task.attempt_id == existing_attempt
                    and existing.state
                    in {
                        TaskRuntimeState.STARTING,
                        TaskRuntimeState.RUNNING,
                    }
                ):
                    return existing.snapshot
                if (
                    deployment.coordinator_epoch == existing_epoch
                    and task.attempt_id == existing_attempt
                ):
                    raise WorkerTaskError(
                        f"任务 {task.task_id} attempt={task.attempt_id} "
                        f"已存在且状态为 {existing.state.value}"
                    )
                await existing.stop()
                del self._runtimes[task.task_id]
            self._highest_coordinator_epoch = deployment.coordinator_epoch
            self._highest_attempts[task.task_id] = task.attempt_id
            job_root = await self._prepare_artifact(deployment.artifact)
            loader: UDFLoader | None = None
            try:
                graph = load_stream_graph(job_root / "job.yaml")
                operator_spec = graph.operator(task.operator_id)
                execution = graph.definition.execution
                event_time_strategy = execution.event_time if execution is not None else None
                if operator_spec.type is not task.operator_type:
                    raise WorkerTaskError(
                        f"任务类型 {task.operator_type.value} 与 job.yaml "
                        f"{operator_spec.type.value} 不一致"
                    )
                context = OperatorContext(
                    operator_id=task.operator_id,
                    subtask_index=task.subtask_index,
                    clock=self.clock_factory(),
                )
                source = None
                operator = None
                if operator_spec.type is OperatorType.SOURCE:
                    config = operator_spec.config
                    if not isinstance(config, KafkaSourceConfig):
                        raise WorkerTaskError("Source 缺少 Kafka 配置")
                    arguments: dict[str, Any] = {
                        "job_id": task.job_id,
                        "config": config,
                        "source_parallelism": task.parallelism,
                        "event_time_strategy": event_time_strategy,
                    }
                    if config.validator is not None:
                        loader = UDFLoader(job_root, job_id=f"{task.job_id}-{task.task_id}")
                        arguments["payload_validator"] = loader.load(
                            config.validator,
                            UDFKind.VALIDATOR,
                        )
                    if self.consumer_factory is not None:
                        arguments["consumer_factory"] = self.consumer_factory
                    source = KafkaJsonSource(context, **arguments)
                elif operator_spec.type is OperatorType.SINK:
                    config = operator_spec.config
                    if not isinstance(config, FileSinkConfig):
                        raise WorkerTaskError("Sink 缺少文件配置")
                    operator = FileSinkOperator(context, job_id=task.job_id, config=config)
                else:
                    loader = UDFLoader(job_root, job_id=f"{task.job_id}-{task.task_id}")
                    if operator_spec.udf is None:  # pragma: no cover - API 模型保证
                        raise WorkerTaskError(f"{operator_spec.type.value} 缺少 UDF")
                    if operator_spec.type is OperatorType.MAP:
                        udf = loader.load(operator_spec.udf, UDFKind.MAP)
                        operator = MapOperator(context, udf)
                    elif operator_spec.type is OperatorType.KEY_BY:
                        udf = loader.load(operator_spec.udf, UDFKind.KEY_SELECTOR)
                        operator = KeyByOperator(context, udf)
                    elif operator_spec.type is OperatorType.REDUCE:
                        udf = loader.load(operator_spec.udf, UDFKind.REDUCE)
                        retract_udf = (
                            loader.load(operator_spec.retract_udf, UDFKind.RETRACT)
                            if operator_spec.retract_udf is not None
                            else None
                        )
                        if operator_spec.window is None:  # pragma: no cover - API 模型保证
                            raise WorkerTaskError("Reduce 缺少窗口配置")
                        operator = ReduceWindowOperator(
                            context,
                            udf,
                            window_size_seconds=operator_spec.window.size_seconds,
                            time_characteristic=operator_spec.window.time_characteristic,
                            emit_mode=operator_spec.emit_mode,
                            retract_function=retract_udf,
                        )
                    else:  # pragma: no cover - StrEnum 完整处理
                        raise WorkerTaskError(f"不支持算子 {operator_spec.type}")

                runtime_arguments = dict(self.runtime_options)
                runtime_arguments.setdefault("monotonic_clock", context.clock.monotonic)
                runtime_arguments.setdefault("checkpoint_enabled", execution is not None)
                runtime_arguments.setdefault(
                    "aligned_checkpoints",
                    execution is not None
                    and execution.delivery_guarantee is DeliveryGuarantee.EXACTLY_ONCE,
                )
                if execution is not None or deployment.restore_descriptors:
                    runtime_arguments.setdefault("checkpoint_store", self.checkpoint_store)
                if event_time_strategy is not None:
                    runtime_arguments.setdefault(
                        "watermark_idle_timeout",
                        event_time_strategy.idle_timeout_seconds,
                    )

                async def report_failure(
                    job_id: str,
                    task_id: str,
                    error: BaseException,
                    attempt_id: int = task.attempt_id,
                    coordinator_epoch: int = deployment.coordinator_epoch,
                ) -> None:
                    await self._report_failure(
                        job_id,
                        task_id,
                        attempt_id,
                        error,
                        coordinator_epoch=coordinator_epoch,
                    )

                runtime = TaskRuntime(
                    deployment,
                    self.data_server,
                    source=source,
                    operator=operator,
                    udf_loader=loader,
                    failure_callback=report_failure,
                    **runtime_arguments,
                )
                self._runtimes[task.task_id] = runtime
                await runtime.start()
                return runtime.snapshot
            except BaseException as exc:
                self._runtimes.pop(task.task_id, None)
                if loader is not None:
                    loader.close()
                raise WorkerTaskError(f"部署任务 {task.task_id} 失败: {exc}") from exc

    async def stop(
        self,
        task_id: str,
        attempt_id: int | None = None,
        coordinator_epoch: int = 0,
    ) -> RuntimeSnapshot | None:
        """幂等停止任务；未知 task_id 返回 None。"""
        runtime = self._runtimes.get(task_id)
        if runtime is None:
            return None
        if coordinator_epoch < self._highest_coordinator_epoch:
            raise WorkerTaskError(
                f"拒绝旧 coordinator epoch {coordinator_epoch}, "
                f"Worker 当前最高 epoch={self._highest_coordinator_epoch}"
            )
        self._highest_coordinator_epoch = coordinator_epoch
        current_attempt = runtime.deployment.task.attempt_id
        current_epoch = runtime.deployment.coordinator_epoch
        if coordinator_epoch < current_epoch:
            return runtime.snapshot
        if attempt_id is not None and attempt_id < current_attempt:
            return runtime.snapshot
        if attempt_id is not None and attempt_id > current_attempt:
            raise WorkerTaskError(
                f"停止请求 attempt={attempt_id} 高于当前 attempt={current_attempt}"
            )
        await runtime.stop()
        return runtime.snapshot

    async def arm_checkpoint(
        self,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> None:
        """为本地 Task 准备 Checkpoint。"""
        await self._runtime(task_id, attempt_id, coordinator_epoch).arm_checkpoint(checkpoint_id)

    async def trigger_checkpoint(
        self,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> TaskSnapshotDescriptor:
        """触发本地 Source Task 快照。"""
        return await self._runtime(
            task_id,
            attempt_id,
            coordinator_epoch,
        ).trigger_checkpoint(checkpoint_id)

    async def wait_checkpoint(
        self,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> TaskSnapshotDescriptor:
        """等待本地 Task 收齐 DRAIN 并完成快照。"""
        return await self._runtime(
            task_id,
            attempt_id,
            coordinator_epoch,
        ).wait_checkpoint(checkpoint_id)

    async def complete_checkpoint(
        self,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> None:
        """通知本地 Task 全图 manifest 已完成。"""
        await self._runtime(
            task_id,
            attempt_id,
            coordinator_epoch,
        ).complete_checkpoint(checkpoint_id)

    async def abort_checkpoint(
        self,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        coordinator_epoch: int = 0,
    ) -> None:
        """中止本地 Task 的活动 Checkpoint。"""
        await self._runtime(task_id, attempt_id, coordinator_epoch).abort_checkpoint(checkpoint_id)

    def get(self, task_id: str) -> RuntimeSnapshot:
        """查询单个本地任务。"""
        try:
            return self._runtimes[task_id].snapshot
        except KeyError as exc:
            raise WorkerTaskError(f"未知任务 {task_id!r}") from exc

    def _runtime(
        self,
        task_id: str,
        attempt_id: int,
        coordinator_epoch: int,
    ) -> TaskRuntime:
        try:
            runtime = self._runtimes[task_id]
        except KeyError as exc:
            raise WorkerTaskError(f"未知任务 {task_id!r}") from exc
        if coordinator_epoch < self._highest_coordinator_epoch:
            raise WorkerTaskError(
                f"拒绝旧 coordinator epoch {coordinator_epoch}, "
                f"Worker 当前最高 epoch={self._highest_coordinator_epoch}"
            )
        current_attempt = runtime.deployment.task.attempt_id
        current_epoch = runtime.deployment.coordinator_epoch
        if coordinator_epoch != current_epoch:
            raise WorkerTaskError(
                f"Task {task_id} coordinator epoch 不匹配: "
                f"request={coordinator_epoch}, current={current_epoch}"
            )
        if attempt_id != current_attempt:
            raise WorkerTaskError(
                f"Task {task_id} attempt 不匹配: request={attempt_id}, current={current_attempt}"
            )
        return runtime

    def snapshots(self) -> tuple[RuntimeSnapshot, ...]:
        """按 task_id 返回全部本地任务状态。"""
        return tuple(self._runtimes[task_id].snapshot for task_id in sorted(self._runtimes))

    async def close(self) -> None:
        """停止全部任务，继续清理后续任务而不因单个错误中断。"""
        errors: list[str] = []
        for task_id in reversed(sorted(self._runtimes)):
            try:
                await self._runtimes[task_id].stop()
            except Exception as exc:
                errors.append(f"{task_id}: {exc}")
        if errors:
            raise WorkerTaskError("关闭 Worker 任务失败: " + "; ".join(errors))

    async def _prepare_artifact(self, descriptor: ArtifactDescriptor) -> Path:
        key = (descriptor.job_id, descriptor.sha256)
        lock = self._artifact_locks.setdefault(key, asyncio.Lock())
        async with lock:
            artifact_dir = self.work_root / "artifacts" / descriptor.job_id
            artifact_path = artifact_dir / f"{descriptor.sha256}.zip"
            job_root = self.work_root / "jobs" / descriptor.job_id / descriptor.sha256
            if artifact_path.is_file() and job_root.is_dir():
                verify_job_bundle(artifact_path, expected_sha256=descriptor.sha256)
                return job_root

            content = await self.artifact_fetcher.fetch(descriptor)
            actual = hashlib.sha256(content).hexdigest()
            if len(content) != descriptor.size or actual != descriptor.sha256:
                raise WorkerTaskError(
                    "下载制品不匹配: "
                    f"expected_size={descriptor.size}, actual_size={len(content)}, "
                    f"expected_sha256={descriptor.sha256}, actual_sha256={actual}"
                )
            artifact_dir.mkdir(parents=True, exist_ok=True)
            if not artifact_path.exists():
                descriptor_fd, temporary_name = tempfile.mkstemp(
                    dir=artifact_dir,
                    prefix=f".{descriptor.sha256}.",
                    suffix=".tmp",
                )
                try:
                    with os.fdopen(descriptor_fd, "wb") as stream:
                        stream.write(content)
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(temporary_name, artifact_path)
                finally:
                    Path(temporary_name).unlink(missing_ok=True)
            verify_job_bundle(artifact_path, expected_sha256=descriptor.sha256)
            if not job_root.exists():
                job_root.parent.mkdir(parents=True, exist_ok=True)
                extract_job_bundle(
                    artifact_path,
                    job_root,
                    expected_sha256=descriptor.sha256,
                )
            return job_root

    async def _report_failure(
        self,
        job_id: str,
        task_id: str,
        attempt_id: int,
        error: BaseException,
        *,
        coordinator_epoch: int = 0,
    ) -> None:
        if self.status_reporter is not None:
            await self.status_reporter.report_task_failed(
                job_id,
                task_id,
                attempt_id,
                f"{type(error).__name__}: {error}",
                coordinator_epoch=coordinator_epoch,
            )


__all__ = [
    "ArtifactFetcher",
    "StatusReporter",
    "WorkerTaskError",
    "WorkerTaskManager",
]
