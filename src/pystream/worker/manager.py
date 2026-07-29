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
    FileSinkConfig,
    KafkaSourceConfig,
    OperatorType,
    load_stream_graph,
)
from pystream.artifact import UDFKind, UDFLoader, extract_job_bundle, verify_job_bundle
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
        error: str,
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
        self._runtimes: dict[str, TaskRuntime] = {}
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
            existing = self._runtimes.get(task.task_id)
            if existing is not None:
                if existing.state in {
                    TaskRuntimeState.STARTING,
                    TaskRuntimeState.RUNNING,
                }:
                    return existing.snapshot
                raise WorkerTaskError(f"任务 {task.task_id} 已存在且状态为 {existing.state.value}")
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
                if event_time_strategy is not None:
                    runtime_arguments.setdefault(
                        "watermark_idle_timeout",
                        event_time_strategy.idle_timeout_seconds,
                    )
                runtime = TaskRuntime(
                    deployment,
                    self.data_server,
                    source=source,
                    operator=operator,
                    udf_loader=loader,
                    failure_callback=self._report_failure,
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

    async def stop(self, task_id: str) -> RuntimeSnapshot | None:
        """幂等停止任务；未知 task_id 返回 None。"""
        runtime = self._runtimes.get(task_id)
        if runtime is None:
            return None
        await runtime.stop()
        return runtime.snapshot

    def get(self, task_id: str) -> RuntimeSnapshot:
        """查询单个本地任务。"""
        try:
            return self._runtimes[task_id].snapshot
        except KeyError as exc:
            raise WorkerTaskError(f"未知任务 {task_id!r}") from exc

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
        error: BaseException,
    ) -> None:
        if self.status_reporter is not None:
            await self.status_reporter.report_task_failed(
                job_id,
                task_id,
                f"{type(error).__name__}: {error}",
            )


__all__ = [
    "ArtifactFetcher",
    "StatusReporter",
    "WorkerTaskError",
    "WorkerTaskManager",
]
