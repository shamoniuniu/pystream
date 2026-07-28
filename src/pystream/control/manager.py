"""JobManager 控制面协调服务。

服务把已校验 StreamGraph 转成物理执行图，执行全量资源预检和下游优先部署。
任何部署或运行故障都会停止其余任务、释放全部 slot，并把作业收敛到 FAILED。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pystream.api import StreamGraph
from pystream.control.errors import (
    ControlPlaneError,
    DeploymentError,
    InvalidStateTransition,
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


@dataclass(slots=True)
class JobRun:
    """一个作业在 JobManager 内的聚合根。"""

    job: Job
    execution_graph: ExecutionGraph
    artifact: ArtifactDescriptor | None = None
    deployed_task_ids: list[str] | None = None

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
    ) -> None:
        self.artifact_repository = artifact_repository
        self.worker_gateway = worker_gateway
        self.registry = registry or WorkerRegistry(heartbeat_timeout)
        self.scheduler = scheduler or SlotScheduler()
        self._runs: dict[str, JobRun] = {}

    def register_worker(
        self,
        worker_id: str,
        control_address: str,
        data_host: str,
        data_port: int,
        total_slots: int,
        heartbeat_at: datetime | None = None,
    ) -> WorkerNode:
        """注册 Worker 并返回当前资源对象。"""
        return self.registry.register(
            worker_id=worker_id,
            control_address=control_address,
            data_host=data_host,
            data_port=data_port,
            total_slots=total_slots,
            heartbeat_at=heartbeat_at,
        )

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
            "tasks": [
                {
                    "task_id": task.task_id,
                    "operator_id": task.operator_id,
                    "subtask": task.subtask_index,
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
        run = JobRun(job=job, execution_graph=execution_graph)
        self._runs[resolved_job_id] = run
        job.transition(JobStatus.VALIDATING)

        try:
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
            for task in execution_graph.deployment_order():
                task.transition(TaskStatus.DEPLOYING)
                worker = self.registry.get(task.worker_id or "")
                await self.worker_gateway.deploy_task(
                    worker,
                    self._deployment(run, task),
                )
                task.transition(TaskStatus.RUNNING)
                run.deployed_task_ids.append(task.task_id)
        except Exception as exc:
            await self._fail_run(run, f"部署失败: {exc}", failed_task=task)
            raise DeploymentError(f"作业 {resolved_job_id} 部署失败并已回滚: {exc}") from exc

        job.transition(JobStatus.RUNNING)
        return job

    def _deployment(self, run: JobRun, task: TaskInstance) -> TaskDeployment:
        if run.artifact is None:  # pragma: no cover - submit 顺序保证
            raise InvalidStateTransition("作业制品尚未保存")
        return TaskDeployment(
            task=task,
            artifact=run.artifact,
            incoming_channels=run.execution_graph.incoming_channels(task.task_id),
            outgoing_channels=run.execution_graph.outgoing_channels(task.task_id),
        )

    async def cancel_job(self, job_id: str) -> Job:
        """停止 RUNNING 作业并释放全部 slot。"""
        run = self._runs[job_id]
        run.job.transition(JobStatus.CANCELLING)
        errors = await self._stop_tasks(run, reversed(run.deployed_task_ids))
        self._cancel_scheduled_tasks(run)
        self.scheduler.release(run.execution_graph, self.registry)
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
    ) -> Job:
        """汇总 Worker 任务状态；任一 FAILED 触发全作业失败清理。"""
        run = self._runs[job_id]
        task = run.execution_graph.tasks[task_id]
        if task.status is status:
            return run.job
        if status is TaskStatus.FAILED:
            await self._fail_run(run, error or f"任务 {task_id} 失败", failed_task=task)
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
                await self._fail_run(
                    run,
                    f"Worker {failed_task.worker_id} 心跳超时",
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
        if run.job.status in {JobStatus.DEPLOYING, JobStatus.RUNNING, JobStatus.CANCELLING}:
            run.job.transition(JobStatus.FAILING, reason)
        if failed_task is not None and failed_task.status not in {
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        }:
            failed_task.transition(TaskStatus.FAILED, reason)

        stop_ids = [
            task_id
            for task_id in reversed(run.deployed_task_ids)
            if failed_task is None or task_id != failed_task.task_id
        ]
        errors = await self._stop_tasks(run, stop_ids)
        self._cancel_scheduled_tasks(run, exclude=failed_task)
        self.scheduler.release(run.execution_graph, self.registry)
        final_reason = reason if not errors else f"{reason}; 回滚错误: {'; '.join(errors)}"
        run.job.transition(JobStatus.FAILED, final_reason)

    async def _stop_tasks(self, run: JobRun, task_ids) -> list[str]:
        errors: list[str] = []
        for task_id in task_ids:
            task = run.execution_graph.tasks[task_id]
            if task.status not in {TaskStatus.RUNNING, TaskStatus.DEPLOYING}:
                continue
            worker_id = task.worker_id
            task.transition(TaskStatus.CANCELLING)
            try:
                worker = self.registry.get(worker_id or "")
                await self.worker_gateway.stop_task(worker, task.task_id)
                task.transition(TaskStatus.CANCELLED)
            except Exception as exc:
                message = f"{task.task_id}: {exc}"
                task.transition(TaskStatus.FAILED, message)
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


__all__ = ["JobManager", "JobRun"]
