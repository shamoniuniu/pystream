"""JobManager 控制面协调服务。

服务把已校验 StreamGraph 转成物理执行图，执行全量资源预检和下游优先部署。
任何部署或运行故障都会停止其余任务、释放全部 slot，并把作业收敛到 FAILED。
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from pystream.api import OperatorType, StreamGraph
from pystream.checkpoint import CheckpointManifest, LocalCheckpointStore
from pystream.control.checkpoint import (
    CheckpointCoordinationError,
    CheckpointCoordinator,
)
from pystream.control.errors import (
    ControlPlaneError,
    DeploymentError,
    InvalidStateTransition,
    WorkerNotFound,
)
from pystream.control.execution import ExecutionGraph, build_execution_graph
from pystream.control.models import (
    ArtifactDescriptor,
    Job,
    JobStatus,
    ResourceView,
    TaskInstance,
    TaskStatus,
    WorkerNode,
)
from pystream.control.ports import ArtifactRepository, TaskDeployment, WorkerGateway
from pystream.control.scheduler import SlotScheduler, WorkerRegistry
from pystream.observability import log_event


@dataclass(slots=True)
class JobRun:
    """一个作业在 JobManager 内的聚合根。"""

    job: Job
    execution_graph: ExecutionGraph
    artifact: ArtifactDescriptor | None = None
    deployed_task_ids: list[str] | None = None
    attempt_id: int = 0
    next_checkpoint_id: int = 1
    last_completed_checkpoint_id: int | None = None
    consecutive_checkpoint_failures: int = 0
    checkpoint_interval: float | None = None
    checkpoint_timeout: float = 30.0
    max_consecutive_checkpoint_failures: int = 3
    checkpoint_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    checkpoint_task: asyncio.Task[None] | None = field(default=None, repr=False)
    max_recovery_attempts: int = 0
    recovery_delay: float = 0.0
    recovery_attempts: int = 0
    last_failure: str | None = None
    last_recovery_completed_at: datetime | None = None
    restore_manifest: CheckpointManifest | None = None
    recovery_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    recovery_task: asyncio.Task[None] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.deployed_task_ids is None:
            self.deployed_task_ids = []


class JobManager:
    """协调 Worker、制品、调度和作业生命周期。"""

    def __init__(
        self,
        artifact_repository: ArtifactRepository,
        worker_gateway: WorkerGateway,
        *,
        heartbeat_timeout: timedelta = timedelta(seconds=15),
        registry: WorkerRegistry | None = None,
        scheduler: SlotScheduler | None = None,
        checkpoint_store: LocalCheckpointStore | None = None,
    ) -> None:
        self.artifact_repository = artifact_repository
        self.worker_gateway = worker_gateway
        self.registry = registry or WorkerRegistry(heartbeat_timeout)
        self.scheduler = scheduler or SlotScheduler()
        self.checkpoint_store = checkpoint_store
        self.checkpoint_coordinator = (
            CheckpointCoordinator(
                checkpoint_store,
                worker_gateway,
                self.registry.get,
            )
            if checkpoint_store is not None
            else None
        )
        self._runs: dict[str, JobRun] = {}

    def register_worker(
        self,
        worker_id: str,
        control_address: str,
        data_host: str,
        data_port: int,
        total_slots: int,
        heartbeat_at: datetime | None = None,
        *,
        incarnation_id: str = "legacy",
    ) -> WorkerNode:
        """注册 Worker 并返回当前资源对象。"""
        return self.registry.register(
            worker_id=worker_id,
            control_address=control_address,
            data_host=data_host,
            data_port=data_port,
            total_slots=total_slots,
            heartbeat_at=heartbeat_at,
            incarnation_id=incarnation_id,
        )

    async def register_worker_process(
        self,
        worker_id: str,
        incarnation_id: str,
        control_address: str,
        data_host: str,
        data_port: int,
        total_slots: int,
    ) -> tuple[WorkerNode, bool, tuple[str, ...]]:
        """注册 Worker 进程，并在 incarnation 变化时处理旧进程承载的作业。"""
        try:
            previous = self.registry.get(worker_id)
        except WorkerNotFound:
            previous = None
        previous_incarnation = previous.incarnation_id if previous is not None else None
        restarted = previous is not None and previous_incarnation != incarnation_id
        affected_runs = (
            tuple(
                run
                for run in self._runs.values()
                if run.job.status
                in {
                    JobStatus.DEPLOYING,
                    JobStatus.RUNNING,
                    JobStatus.RECOVERING,
                }
                and any(task.worker_id == worker_id for task in run.execution_graph.tasks.values())
            )
            if restarted
            else ()
        )
        worker = self.register_worker(
            worker_id,
            control_address,
            data_host,
            data_port,
            total_slots,
            incarnation_id=incarnation_id,
        )
        reason = (
            f"Worker {worker_id} incarnation 已变化: "
            f"{previous_incarnation or 'unknown'} -> {incarnation_id}"
        )
        for run in affected_runs:
            if run.checkpoint_interval is not None:
                await self._request_recovery(run, reason)
                continue
            failed_task = next(
                task for task in run.execution_graph.tasks.values() if task.worker_id == worker_id
            )
            await self._cancel_checkpoint_loop(run)
            async with run.checkpoint_lock:
                await self._fail_run(run, reason, failed_task=failed_task)
        return worker, restarted, tuple(run.job.job_id for run in affected_runs)

    def heartbeat(self, worker_id: str, at: datetime | None = None) -> WorkerNode:
        """接收 Worker 心跳。"""
        return self.registry.heartbeat(worker_id, at)

    def resources(self, now: datetime | None = None) -> tuple[ResourceView, ...]:
        """返回 Worker 资源快照。"""
        return self.registry.resource_view(now)

    def health(self, now: datetime | None = None) -> dict[str, int | str]:
        """返回 JobManager 健康和资源摘要。"""
        resource_view = self.resources(now)
        return {
            "status": "ok",
            "workers": len(resource_view),
            "healthy_workers": sum(view.healthy for view in resource_view),
            "jobs": len(self._runs),
            "running_jobs": sum(run.job.status is JobStatus.RUNNING for run in self._runs.values()),
        }

    def get_job(self, job_id: str) -> Job:
        """查询作业；未知 ID 保留 KeyError 语义供 HTTP 层转换。"""
        return self._runs[job_id].job

    def get_execution_graph(self, job_id: str) -> ExecutionGraph:
        """查询物理执行图。"""
        return self._runs[job_id].execution_graph

    def status_view(self, job_id: str) -> dict[str, object]:
        """生成可直接序列化的作业与任务位置快照。"""
        run = self._runs[job_id]
        return {
            "job_id": run.job.job_id,
            "name": run.job.name,
            "status": run.job.status.value,
            "error": run.job.error,
            "attempt": run.attempt_id,
            "recovery": {
                "attempts": run.recovery_attempts,
                "max_attempts": run.max_recovery_attempts,
                "last_failure": run.last_failure,
                "last_completed_at": (
                    run.last_recovery_completed_at.isoformat()
                    if run.last_recovery_completed_at is not None
                    else None
                ),
            },
            "checkpoint": (
                None
                if run.checkpoint_interval is None
                else {
                    "next_id": run.next_checkpoint_id,
                    "last_completed_id": run.last_completed_checkpoint_id,
                    "consecutive_failures": run.consecutive_checkpoint_failures,
                }
            ),
            "tasks": [
                {
                    "task_id": task.task_id,
                    "operator_id": task.operator_id,
                    "subtask": task.subtask_index,
                    "attempt_id": task.attempt_id,
                    "restored_checkpoint_id": task.restored_checkpoint_id,
                    "status": task.status.value,
                    "worker_id": task.worker_id,
                    "slot": task.slot_index,
                    "error": task.error,
                }
                for task in run.execution_graph.tasks.values()
            ],
        }

    async def submit_job(
        self,
        graph: StreamGraph,
        artifact_content: bytes,
        *,
        expected_sha256: str | None = None,
        job_id: str | None = None,
    ) -> Job:
        """保存制品、调度并下游优先部署完整作业。"""
        resolved_job_id = job_id or uuid.uuid4().hex
        if resolved_job_id in self._runs:
            raise ControlPlaneError(f"作业 {resolved_job_id!r} 已存在")

        job = Job(job_id=resolved_job_id, name=graph.definition.job.name)
        execution_graph = build_execution_graph(resolved_job_id, graph)
        execution = graph.definition.execution
        run = JobRun(
            job=job,
            execution_graph=execution_graph,
            checkpoint_interval=(
                execution.checkpoint.interval_seconds if execution is not None else None
            ),
            checkpoint_timeout=(
                execution.checkpoint.timeout_seconds if execution is not None else 30.0
            ),
            max_consecutive_checkpoint_failures=(
                execution.checkpoint.max_consecutive_failures if execution is not None else 3
            ),
            max_recovery_attempts=(execution.restart.max_attempts if execution is not None else 0),
            recovery_delay=(execution.restart.delay_seconds if execution is not None else 0.0),
        )
        self._runs[resolved_job_id] = run
        job.transition(JobStatus.VALIDATING)

        try:
            if execution is not None and self.checkpoint_coordinator is None:
                raise ControlPlaneError("中级作业需要配置共享 Checkpoint Store")
            run.artifact = self.artifact_repository.put(
                resolved_job_id,
                artifact_content,
                expected_sha256,
            )
            self.scheduler.schedule(execution_graph, self.registry)
        except Exception as exc:
            job.transition(JobStatus.REJECTED, str(exc))
            raise

        job.transition(JobStatus.DEPLOYING)
        try:
            await self._deploy_run_tasks(run)
        except Exception as exc:
            failed_task = next(
                (
                    candidate
                    for candidate in execution_graph.deployment_order()
                    if candidate.status is TaskStatus.FAILED
                ),
                None,
            )
            if run.checkpoint_interval is not None:
                await self._request_recovery(run, f"部署失败: {exc}")
                return job
            await self._fail_run(run, f"部署失败: {exc}", failed_task=failed_task)
            raise DeploymentError(f"作业 {resolved_job_id} 部署失败并已回滚: {exc}") from exc

        job.transition(JobStatus.RUNNING)
        if run.checkpoint_interval is not None:
            run.checkpoint_task = asyncio.create_task(
                self._checkpoint_loop(run),
                name=f"pystream-checkpoint-{resolved_job_id}",
            )
        return job

    async def trigger_checkpoint(self, job_id: str) -> CheckpointManifest:
        """立即执行一次串行 Checkpoint，供周期任务和确定性测试复用。"""
        run = self._runs[job_id]
        coordinator = self.checkpoint_coordinator
        if run.checkpoint_interval is None or coordinator is None:
            raise CheckpointCoordinationError(f"作业 {job_id} 未启用 Checkpoint")
        async with run.checkpoint_lock:
            if run.job.status is not JobStatus.RUNNING:
                raise CheckpointCoordinationError(
                    f"作业 {job_id} 状态 {run.job.status.value} 不能执行 Checkpoint"
                )
            checkpoint_id = run.next_checkpoint_id
            run.next_checkpoint_id += 1
            self._log_checkpoint(
                logging.INFO,
                "checkpoint_started",
                "JobManager 开始协调 Checkpoint",
                run,
                checkpoint_id,
            )
            try:
                manifest = await coordinator.run(
                    run.execution_graph,
                    checkpoint_id=checkpoint_id,
                    attempt_id=run.attempt_id,
                    timeout=run.checkpoint_timeout,
                )
            except Exception as exc:
                run.consecutive_checkpoint_failures += 1
                self._log_checkpoint(
                    logging.ERROR,
                    "checkpoint_failed",
                    "JobManager Checkpoint 协调失败",
                    run,
                    checkpoint_id,
                    error=f"{type(exc).__name__}: {exc}",
                    exc_info=exc,
                )
                if run.consecutive_checkpoint_failures >= run.max_consecutive_checkpoint_failures:
                    await self._request_recovery(
                        run,
                        "Checkpoint 连续失败达到上限: "
                        f"{run.consecutive_checkpoint_failures}; "
                        f"last_error={type(exc).__name__}: {exc}",
                    )
                raise
            run.last_completed_checkpoint_id = checkpoint_id
            run.consecutive_checkpoint_failures = 0
            self._log_checkpoint(
                logging.INFO,
                "checkpoint_completed",
                "JobManager Checkpoint 已完成",
                run,
                checkpoint_id,
                snapshots=len(manifest.snapshots),
            )
            return manifest

    async def close(self) -> None:
        """停止全部周期 Checkpoint 和 Recovery 协程。"""
        for run in self._runs.values():
            await self._cancel_checkpoint_loop(run)
            await self._cancel_recovery_loop(run)

    async def _checkpoint_loop(self, run: JobRun) -> None:
        interval = run.checkpoint_interval
        if interval is None:  # pragma: no cover - 只为启用作业创建
            return
        try:
            while run.job.status is JobStatus.RUNNING:
                await asyncio.sleep(interval)
                if run.job.status is not JobStatus.RUNNING:
                    return
                try:
                    await self.trigger_checkpoint(run.job.job_id)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if run.job.status is not JobStatus.RUNNING:
                        return
        finally:
            if run.checkpoint_task is asyncio.current_task():
                run.checkpoint_task = None

    async def _request_recovery(self, run: JobRun, reason: str) -> None:
        async with run.recovery_lock:
            if run.job.status in {
                JobStatus.CANCELLING,
                JobStatus.CANCELLED,
                JobStatus.FAILING,
                JobStatus.FAILED,
                JobStatus.REJECTED,
            }:
                return
            run.last_failure = reason
            existing = run.recovery_task
            if existing is not None and not existing.done():
                return
            await self._cancel_checkpoint_loop(run)
            if run.job.status in {JobStatus.RUNNING, JobStatus.DEPLOYING}:
                run.job.transition(JobStatus.RECOVERING, reason)
            elif run.job.status is not JobStatus.RECOVERING:
                raise InvalidStateTransition(f"作业状态 {run.job.status.value} 不能开始恢复")
            run.recovery_attempts = 0
            run.recovery_task = asyncio.create_task(
                self._recover_run(run),
                name=f"pystream-recovery-{run.job.job_id}",
            )
            self._log_recovery(
                logging.WARNING,
                "recovery_started",
                "JobManager 已启动整作业恢复",
                run,
                reason=reason,
            )

    async def _recover_run(self, run: JobRun) -> None:
        try:
            cleanup_errors = await self._stop_tasks(run, self._task_stop_order(run))
            self._cancel_scheduled_tasks(run)
            self.scheduler.release(run.execution_graph, self.registry)
            run.deployed_task_ids.clear()
            if cleanup_errors:
                run.last_failure = f"{run.last_failure}; 旧 attempt 清理错误: " + "; ".join(
                    cleanup_errors
                )

            manifest = (
                self.checkpoint_store.latest_manifest(
                    run.job.job_id,
                    expected_task_ids=set(run.execution_graph.tasks),
                )
                if self.checkpoint_store is not None
                else None
            )
            run.restore_manifest = manifest
            restored_checkpoint_id = manifest.checkpoint_id if manifest is not None else None
            if restored_checkpoint_id is not None:
                run.last_completed_checkpoint_id = restored_checkpoint_id
                run.next_checkpoint_id = max(
                    run.next_checkpoint_id,
                    restored_checkpoint_id + 1,
                )

            while run.recovery_attempts < run.max_recovery_attempts:
                if run.job.status is not JobStatus.RECOVERING:
                    return
                await asyncio.sleep(run.recovery_delay)
                if run.job.status is not JobStatus.RECOVERING:
                    return
                run.recovery_attempts += 1
                run.attempt_id += 1
                run.execution_graph.reset_for_attempt(
                    run.attempt_id,
                    restored_checkpoint_id,
                )
                self._log_recovery(
                    logging.INFO,
                    "recovery_attempt_started",
                    "JobManager 开始恢复 attempt",
                    run,
                    restored_checkpoint_id=restored_checkpoint_id,
                )
                try:
                    self.scheduler.schedule(run.execution_graph, self.registry)
                    run.job.transition(JobStatus.DEPLOYING, run.last_failure)
                    await self._deploy_run_tasks(run)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    reason = f"恢复 attempt {run.attempt_id} 失败: {exc}"
                    run.last_failure = reason
                    cleanup_errors = await self._stop_tasks(
                        run,
                        self._task_stop_order(run),
                    )
                    self._cancel_scheduled_tasks(run)
                    self.scheduler.release(run.execution_graph, self.registry)
                    run.deployed_task_ids.clear()
                    if cleanup_errors:
                        run.last_failure += "; 清理错误: " + "; ".join(cleanup_errors)
                    if run.job.status is JobStatus.DEPLOYING:
                        run.job.transition(JobStatus.RECOVERING, run.last_failure)
                    self._log_recovery(
                        logging.ERROR,
                        "recovery_attempt_failed",
                        "JobManager 恢复 attempt 失败",
                        run,
                        error=f"{type(exc).__name__}: {exc}",
                        exc_info=exc,
                    )
                    continue

                run.job.transition(JobStatus.RUNNING)
                run.last_recovery_completed_at = datetime.now(UTC)
                run.consecutive_checkpoint_failures = 0
                if run.checkpoint_interval is not None:
                    run.checkpoint_task = asyncio.create_task(
                        self._checkpoint_loop(run),
                        name=f"pystream-checkpoint-{run.job.job_id}",
                    )
                self._log_recovery(
                    logging.INFO,
                    "recovery_completed",
                    "JobManager 整作业恢复完成",
                    run,
                    restored_checkpoint_id=restored_checkpoint_id,
                )
                return

            await self._fail_run(
                run,
                run.last_failure or f"恢复重试已耗尽: max_attempts={run.max_recovery_attempts}",
            )
        finally:
            if run.recovery_task is asyncio.current_task():
                run.recovery_task = None

    async def _deploy_run_tasks(self, run: JobRun) -> None:
        """保持下游优先，同一 Source 的 subtasks 并发加入消费组。"""
        ordered = run.execution_graph.deployment_order()
        index = 0
        while index < len(ordered):
            task = ordered[index]
            if task.operator_type is not OperatorType.SOURCE:
                await self._deploy_task(run, task)
                index += 1
                continue
            source_tasks: list[TaskInstance] = []
            operator_id = task.operator_id
            while (
                index < len(ordered)
                and ordered[index].operator_type is OperatorType.SOURCE
                and ordered[index].operator_id == operator_id
            ):
                source_tasks.append(ordered[index])
                index += 1
            results = await asyncio.gather(
                *(self._deploy_task(run, source_task) for source_task in source_tasks),
                return_exceptions=True,
            )
            failures = [result for result in results if isinstance(result, BaseException)]
            if failures:
                first = failures[0]
                if isinstance(first, asyncio.CancelledError):
                    raise first
                raise first

    async def _deploy_task(self, run: JobRun, task: TaskInstance) -> None:
        task.transition(TaskStatus.DEPLOYING)
        worker = self.registry.get(task.worker_id or "")
        try:
            await self.worker_gateway.deploy_task(
                worker,
                self._deployment(run, task),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            task.transition(TaskStatus.FAILED, str(exc))
            raise
        task.transition(TaskStatus.RUNNING)
        run.deployed_task_ids.append(task.task_id)

    def _deployment(self, run: JobRun, task: TaskInstance) -> TaskDeployment:
        if run.artifact is None:  # pragma: no cover - submit 顺序保证
            raise InvalidStateTransition("作业制品尚未保存")
        restore_descriptors = ()
        manifest = run.restore_manifest
        if task.restored_checkpoint_id is not None:
            if manifest is None or manifest.checkpoint_id != task.restored_checkpoint_id:
                raise InvalidStateTransition("Task 恢复编号与 manifest 不一致")
            if task.operator_type is OperatorType.SOURCE:
                restore_descriptors = tuple(
                    descriptor
                    for descriptor in manifest.snapshots
                    if descriptor.operator_id == task.operator_id
                )
            else:
                restore_descriptors = tuple(
                    descriptor
                    for descriptor in manifest.snapshots
                    if descriptor.task_id == task.task_id
                )
            if not restore_descriptors:
                raise InvalidStateTransition(f"Task {task.task_id} 缺少恢复 descriptor")
        return TaskDeployment(
            task=task,
            artifact=run.artifact,
            incoming_channels=run.execution_graph.incoming_channels(task.task_id),
            outgoing_channels=run.execution_graph.outgoing_channels(task.task_id),
            restore_descriptors=restore_descriptors,
        )

    async def cancel_job(self, job_id: str) -> Job:
        """停止 RUNNING 作业并释放全部 slot。"""
        run = self._runs[job_id]
        await self._cancel_checkpoint_loop(run)
        await self._cancel_recovery_loop(run)
        async with run.checkpoint_lock:
            run.job.transition(JobStatus.CANCELLING)
            errors = await self._stop_tasks(run, self._task_stop_order(run))
            self._cancel_scheduled_tasks(run)
            self._cancel_created_tasks(run)
            self.scheduler.release(run.execution_graph, self.registry)
            run.deployed_task_ids.clear()
            if errors:
                run.job.transition(JobStatus.FAILING, "; ".join(errors))
                run.job.transition(JobStatus.FAILED, "; ".join(errors))
                raise DeploymentError(f"取消作业 {job_id} 时发生错误: {'; '.join(errors)}")
            run.job.transition(JobStatus.CANCELLED)
            return run.job

    async def report_task_status(
        self,
        job_id: str,
        task_id: str,
        status: TaskStatus,
        error: str | None = None,
        *,
        attempt_id: int = 0,
    ) -> Job:
        """汇总 Worker 任务状态；任一 FAILED 触发全作业失败清理。"""
        run = self._runs[job_id]
        task = run.execution_graph.tasks[task_id]
        if attempt_id != task.attempt_id:
            log_event(
                logging.getLogger(__name__),
                logging.WARNING,
                "stale_attempt_report",
                "忽略旧 attempt 的 Task 状态上报",
                component="job_manager",
                job_id=job_id,
                operator_id=task.operator_id,
                subtask=task.subtask_index,
                task_id=task_id,
                report_attempt_id=attempt_id,
                current_attempt_id=task.attempt_id,
            )
            return run.job
        if task.status is status:
            return run.job
        if status is TaskStatus.FAILED:
            reason = error or f"任务 {task_id} 失败"
            if task.status not in {TaskStatus.FAILED, TaskStatus.CANCELLED}:
                task.transition(TaskStatus.FAILED, reason)
            if run.checkpoint_interval is not None:
                await self._request_recovery(run, reason)
            else:
                await self._cancel_checkpoint_loop(run)
                async with run.checkpoint_lock:
                    await self._fail_run(run, reason, failed_task=task)
            return run.job

        task.transition(status, error)
        if run.job.status is JobStatus.DEPLOYING and all(
            item.status is TaskStatus.RUNNING for item in run.execution_graph.tasks.values()
        ):
            run.job.transition(JobStatus.RUNNING)
        return run.job

    async def reconcile_worker_health(self, now: datetime | None = None) -> tuple[str, ...]:
        """将使用心跳超时 Worker 的运行作业标记失败并清理。"""
        observed_at = now or datetime.now(UTC)
        healthy_ids = {worker.worker_id for worker in self.registry.healthy(observed_at)}
        failed_jobs: list[str] = []
        for run in self._runs.values():
            if run.job.status not in {JobStatus.DEPLOYING, JobStatus.RUNNING}:
                continue
            failed_task = next(
                (
                    task
                    for task in run.execution_graph.tasks.values()
                    if task.worker_id is not None and task.worker_id not in healthy_ids
                ),
                None,
            )
            if failed_task is not None:
                reason = f"Worker {failed_task.worker_id} 心跳超时"
                if failed_task.status not in {
                    TaskStatus.FAILED,
                    TaskStatus.CANCELLED,
                }:
                    failed_task.transition(TaskStatus.FAILED, reason)
                if run.checkpoint_interval is not None:
                    await self._request_recovery(run, reason)
                else:
                    await self._cancel_checkpoint_loop(run)
                    async with run.checkpoint_lock:
                        await self._fail_run(
                            run,
                            reason,
                            failed_task=failed_task,
                        )
                failed_jobs.append(run.job.job_id)
        return tuple(failed_jobs)

    def download_artifact(self, job_id: str, sha256: str) -> bytes:
        """按 job_id 和摘要提供 Worker 下载内容，并在读取时复验摘要。"""
        run = self._runs[job_id]
        descriptor = run.artifact
        if descriptor is None or descriptor.sha256 != sha256:
            raise ControlPlaneError("作业制品摘要不存在或不匹配")
        return self.artifact_repository.read(descriptor)

    async def _fail_run(
        self,
        run: JobRun,
        reason: str,
        *,
        failed_task: TaskInstance | None = None,
    ) -> None:
        await self._cancel_checkpoint_loop(run)
        await self._cancel_recovery_loop(run)
        if run.job.status in {
            JobStatus.DEPLOYING,
            JobStatus.RUNNING,
            JobStatus.RECOVERING,
            JobStatus.CANCELLING,
        }:
            run.job.transition(JobStatus.FAILING, reason)
        if failed_task is not None and failed_task.status not in {
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        }:
            failed_task.transition(TaskStatus.FAILED, reason)

        stop_ids = [
            task_id
            for task_id in self._task_stop_order(run)
            if failed_task is None or task_id != failed_task.task_id
        ]
        errors = await self._stop_tasks(run, stop_ids)
        self._cancel_scheduled_tasks(run, exclude=failed_task)
        self._fail_created_tasks(run, reason)
        self.scheduler.release(run.execution_graph, self.registry)
        run.deployed_task_ids.clear()
        final_reason = reason if not errors else f"{reason}; 回滚错误: {'; '.join(errors)}"
        run.job.transition(JobStatus.FAILED, final_reason)

    async def _cancel_checkpoint_loop(self, run: JobRun) -> None:
        task = run.checkpoint_task
        if task is None:
            return
        if task is asyncio.current_task():
            run.checkpoint_task = None
            return
        run.checkpoint_task = None
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def _cancel_recovery_loop(self, run: JobRun) -> None:
        task = run.recovery_task
        if task is None:
            return
        if task is asyncio.current_task():
            run.recovery_task = None
            return
        run.recovery_task = None
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    @staticmethod
    def _log_checkpoint(
        level: int,
        event: str,
        message: str,
        run: JobRun,
        checkpoint_id: int,
        *,
        exc_info: BaseException | bool | None = None,
        **fields,
    ) -> None:
        log_event(
            logging.getLogger(__name__),
            level,
            event,
            message,
            component="job_manager",
            job_id=run.job.job_id,
            checkpoint_id=checkpoint_id,
            attempt_id=run.attempt_id,
            exc_info=exc_info,
            **fields,
        )

    @staticmethod
    def _log_recovery(
        level: int,
        event: str,
        message: str,
        run: JobRun,
        *,
        exc_info: BaseException | bool | None = None,
        **fields,
    ) -> None:
        log_event(
            logging.getLogger(__name__),
            level,
            event,
            message,
            component="job_manager",
            job_id=run.job.job_id,
            attempt_id=run.attempt_id,
            recovery_attempt=run.recovery_attempts,
            exc_info=exc_info,
            **fields,
        )

    @staticmethod
    def _task_stop_order(run: JobRun) -> tuple[str, ...]:
        return tuple(task.task_id for task in reversed(run.execution_graph.deployment_order()))

    async def _stop_tasks(self, run: JobRun, task_ids) -> list[str]:
        errors: list[str] = []
        for task_id in task_ids:
            task = run.execution_graph.tasks[task_id]
            if task.status not in {
                TaskStatus.RUNNING,
                TaskStatus.DEPLOYING,
                TaskStatus.FAILED,
            }:
                continue
            worker_id = task.worker_id
            was_failed = task.status is TaskStatus.FAILED
            if not was_failed:
                task.transition(TaskStatus.CANCELLING)
            try:
                worker = self.registry.get(worker_id or "")
                await self.worker_gateway.stop_task(
                    worker,
                    task.task_id,
                    task.attempt_id,
                )
                if not was_failed:
                    task.transition(TaskStatus.CANCELLED)
            except Exception as exc:
                message = f"{task.task_id}: {exc}"
                if not was_failed:
                    task.transition(TaskStatus.FAILED, message)
                else:
                    task.error = message
                errors.append(message)
        return errors

    @staticmethod
    def _cancel_scheduled_tasks(
        run: JobRun,
        exclude: TaskInstance | None = None,
    ) -> None:
        for task in run.execution_graph.tasks.values():
            if task is not exclude and task.status is TaskStatus.SCHEDULED:
                task.transition(TaskStatus.CANCELLED)

    @staticmethod
    def _cancel_created_tasks(run: JobRun) -> None:
        for task in run.execution_graph.tasks.values():
            if task.status is TaskStatus.CREATED:
                task.transition(TaskStatus.CANCELLED)

    @staticmethod
    def _fail_created_tasks(run: JobRun, reason: str) -> None:
        for task in run.execution_graph.tasks.values():
            if task.status is TaskStatus.CREATED:
                task.transition(TaskStatus.FAILED, reason)


__all__ = ["JobManager", "JobRun"]
