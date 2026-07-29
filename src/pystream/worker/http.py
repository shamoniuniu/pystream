"""Worker HTTP 服务及控制面 HTTP 客户端适配器。

服务暴露健康、部署、停止和状态查询；启动时先打开数据端口，再向 JobManager
注册并周期心跳。客户端适配器分别实现制品下载、任务失败上报和
``WorkerGateway``，使控制面与 Worker 不共享进程内对象。
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from typing import Any, Protocol
from urllib.parse import quote
from uuid import uuid4

from aiohttp import ClientResponseError, ClientSession, ClientTimeout, web

from pystream.checkpoint import TaskSnapshotDescriptor
from pystream.control import ArtifactDescriptor, TaskDeployment, TaskStatus, WorkerNode
from pystream.observability import log_event
from pystream.runtime import DataPlaneServer, RuntimeErrorBase, RuntimeSnapshot
from pystream.worker.manager import (
    ArtifactFetcher,
    StatusReporter,
    WorkerTaskError,
    WorkerTaskManager,
)
from pystream.worker.models import (
    WorkerRequestError,
    deployment_from_dict,
    deployment_to_dict,
)


class RegistrationClient(StatusReporter, Protocol):
    """Worker 注册、心跳和状态上报端口。"""

    async def register(
        self,
        *,
        worker_id: str,
        control_address: str,
        data_host: str,
        data_port: int,
        total_slots: int,
        incarnation_id: str,
    ) -> None:
        """向 JobManager 注册 Worker。"""

    async def heartbeat(self, worker_id: str) -> None:
        """刷新 Worker 存活时间。"""


@dataclass(frozen=True, slots=True)
class WorkerServiceConfig:
    """Worker 对外地址、slot 和心跳配置。"""

    worker_id: str
    control_address: str
    data_host: str
    incarnation_id: str = field(default_factory=lambda: uuid4().hex)
    total_slots: int = 4
    heartbeat_interval: float = 5.0

    def __post_init__(self) -> None:
        if (
            not self.worker_id
            or not self.control_address
            or not self.data_host
            or not self.incarnation_id
        ):
            raise ValueError("Worker ID、incarnation 和地址不能为空")
        if self.total_slots <= 0:
            raise ValueError("total_slots 必须大于 0")
        if self.heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval 必须大于 0")


class WorkerHttpService:
    """把 WorkerTaskManager 暴露为 aiohttp 应用。"""

    def __init__(
        self,
        config: WorkerServiceConfig,
        manager: WorkerTaskManager,
        data_server: DataPlaneServer,
        registration_client: RegistrationClient,
    ) -> None:
        self.config = config
        self.manager = manager
        self.data_server = data_server
        self.registration_client = registration_client
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._heartbeat_error: str | None = None

    def create_app(self) -> web.Application:
        """创建带生命周期钩子和控制路由的应用。"""
        app = web.Application()
        app.router.add_get("/health", self._health)
        app.router.add_post("/tasks/deploy", self._deploy)
        app.router.add_get("/tasks", self._tasks)
        app.router.add_post(
            "/tasks/{task_id}/checkpoints/{checkpoint_id}/arm",
            self._checkpoint_arm,
        )
        app.router.add_post(
            "/tasks/{task_id}/checkpoints/{checkpoint_id}/trigger",
            self._checkpoint_trigger,
        )
        app.router.add_get(
            "/tasks/{task_id}/checkpoints/{checkpoint_id}",
            self._checkpoint_wait,
        )
        app.router.add_post(
            "/tasks/{task_id}/checkpoints/{checkpoint_id}/complete",
            self._checkpoint_complete,
        )
        app.router.add_post(
            "/tasks/{task_id}/checkpoints/{checkpoint_id}/abort",
            self._checkpoint_abort,
        )
        app.router.add_get("/tasks/{task_id}", self._task)
        app.router.add_delete("/tasks/{task_id}", self._stop)
        app.on_startup.append(self._startup)
        app.on_cleanup.append(self._cleanup)
        return app

    async def _startup(self, _: web.Application) -> None:
        await self.data_server.start()
        await self.registration_client.register(
            worker_id=self.config.worker_id,
            control_address=self.config.control_address,
            data_host=self.config.data_host,
            data_port=self.data_server.bound_port,
            total_slots=self.config.total_slots,
            incarnation_id=self.config.incarnation_id,
        )
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(),
            name=f"pystream-heartbeat-{self.config.worker_id}",
        )
        self._log(
            logging.INFO,
            "worker_started",
            "Worker 服务已启动",
            incarnation_id=self.config.incarnation_id,
            data_port=self.data_server.bound_port,
            total_slots=self.config.total_slots,
        )

    async def _cleanup(self, _: web.Application) -> None:
        task = self._heartbeat_task
        self._heartbeat_task = None
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        with suppress(WorkerTaskError):
            await self.manager.close()
        await self.data_server.close()
        self._log(logging.INFO, "worker_stopped", "Worker 服务已停止")

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.heartbeat_interval)
            try:
                await self.registration_client.heartbeat(self.config.worker_id)
                self._heartbeat_error = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # JobManager 通过超时判定 Worker 失联; 本地保留错误用于健康诊断,
                # 下个周期继续尝试, 避免瞬时控制面故障终止数据任务。
                self._heartbeat_error = f"{type(exc).__name__}: {exc}"
                self._log(
                    logging.ERROR,
                    "heartbeat_failed",
                    "Worker 心跳失败",
                    error=self._heartbeat_error,
                    exc_info=exc,
                )

    async def _health(self, _: web.Request) -> web.Response:
        snapshots = self.manager.snapshots()
        return web.json_response(
            {
                "status": "ok" if self._heartbeat_error is None else "degraded",
                "worker_id": self.config.worker_id,
                "incarnation_id": self.config.incarnation_id,
                "tasks": self.manager.task_count,
                "data_host": self.config.data_host,
                "data_port": self.data_server.bound_port,
                "total_slots": self.config.total_slots,
                "heartbeat_error": self._heartbeat_error,
                "data_plane": self.data_server.metrics,
                "runtime": {
                    "records_in": sum(item.records_in for item in snapshots),
                    "records_out": sum(item.records_out for item in snapshots),
                    "input_queue_depth": sum(item.input_queue_depth for item in snapshots),
                    "output_queue_depth": sum(item.output_queue_depth for item in snapshots),
                    "errors": sum(item.errors for item in snapshots),
                },
            }
        )

    async def _deploy(self, request: web.Request) -> web.Response:
        try:
            deployment = deployment_from_dict(await request.json())
            snapshot = await self.manager.deploy(deployment)
        except (WorkerRequestError, WorkerTaskError, ValueError) as exc:
            self._log(
                logging.ERROR,
                "task_deploy_failed",
                "任务部署请求失败",
                error=f"{type(exc).__name__}: {exc}",
                exc_info=exc,
            )
            raise web.HTTPBadRequest(text=str(exc)) from exc
        task = deployment.task
        self._log(
            logging.INFO,
            "task_deployed",
            "任务已部署到 Worker",
            job_id=task.job_id,
            operator_id=task.operator_id,
            subtask=task.subtask_index,
            task_id=task.task_id,
        )
        return web.json_response(_snapshot_to_dict(snapshot), status=201)

    async def _tasks(self, _: web.Request) -> web.Response:
        return web.json_response(
            {"tasks": [_snapshot_to_dict(snapshot) for snapshot in self.manager.snapshots()]}
        )

    async def _task(self, request: web.Request) -> web.Response:
        try:
            snapshot = self.manager.get(request.match_info["task_id"])
        except WorkerTaskError as exc:
            raise web.HTTPNotFound(text=str(exc)) from exc
        return web.json_response(_snapshot_to_dict(snapshot))

    async def _stop(self, request: web.Request) -> web.Response:
        attempt_id = _attempt_id(request)
        snapshot = await self.manager.stop(
            request.match_info["task_id"],
            attempt_id,
        )
        if snapshot is None:
            return web.Response(status=204)
        if snapshot.attempt_id != attempt_id:
            self._log(
                logging.WARNING,
                "stale_attempt_stop_ignored",
                "Worker 已忽略旧 attempt 的停止请求",
                job_id=snapshot.job_id,
                operator_id=snapshot.operator_id,
                subtask=snapshot.subtask_index,
                task_id=snapshot.task_id,
                request_attempt_id=attempt_id,
                current_attempt_id=snapshot.attempt_id,
            )
            return web.json_response(_snapshot_to_dict(snapshot))
        self._log(
            logging.INFO,
            "task_stopped",
            "Worker 已停止任务",
            job_id=snapshot.job_id,
            operator_id=snapshot.operator_id,
            subtask=snapshot.subtask_index,
            task_id=snapshot.task_id,
        )
        return web.json_response(_snapshot_to_dict(snapshot))

    async def _checkpoint_arm(self, request: web.Request) -> web.Response:
        task_id, attempt_id, checkpoint_id = _checkpoint_coordinates(request)
        await self._checkpoint_action(
            self.manager.arm_checkpoint(task_id, attempt_id, checkpoint_id),
        )
        return web.json_response(
            {"task_id": task_id, "checkpoint_id": checkpoint_id, "status": "armed"}
        )

    async def _checkpoint_trigger(self, request: web.Request) -> web.Response:
        task_id, attempt_id, checkpoint_id = _checkpoint_coordinates(request)
        descriptor = await self._checkpoint_action(
            self.manager.trigger_checkpoint(task_id, attempt_id, checkpoint_id),
        )
        return web.json_response(descriptor.to_dict())

    async def _checkpoint_wait(self, request: web.Request) -> web.Response:
        task_id, attempt_id, checkpoint_id = _checkpoint_coordinates(request)
        descriptor = await self._checkpoint_action(
            self.manager.wait_checkpoint(task_id, attempt_id, checkpoint_id),
        )
        return web.json_response(descriptor.to_dict())

    async def _checkpoint_complete(self, request: web.Request) -> web.Response:
        task_id, attempt_id, checkpoint_id = _checkpoint_coordinates(request)
        await self._checkpoint_action(
            self.manager.complete_checkpoint(task_id, attempt_id, checkpoint_id),
        )
        return web.Response(status=204)

    async def _checkpoint_abort(self, request: web.Request) -> web.Response:
        task_id, attempt_id, checkpoint_id = _checkpoint_coordinates(request)
        await self._checkpoint_action(
            self.manager.abort_checkpoint(task_id, attempt_id, checkpoint_id),
        )
        return web.Response(status=204)

    @staticmethod
    async def _checkpoint_action(operation):
        try:
            return await operation
        except (WorkerTaskError, RuntimeErrorBase, ValueError) as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc

    def _log(
        self,
        level: int,
        event: str,
        message: str,
        *,
        job_id: str | None = None,
        operator_id: str | None = None,
        subtask: int | None = None,
        exc_info: BaseException | bool | None = None,
        **fields,
    ) -> None:
        log_event(
            logging.getLogger(__name__),
            level,
            event,
            message,
            component="worker",
            job_id=job_id,
            operator_id=operator_id,
            subtask=subtask,
            worker_id=self.config.worker_id,
            exc_info=exc_info,
            **fields,
        )


class _HttpClientBase:
    def __init__(
        self,
        base_url: str,
        *,
        session: ClientSession | None = None,
        timeout: float = 10.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._session = session
        self._owns_session = session is None
        self._timeout = ClientTimeout(total=timeout)

    async def _get_session(self) -> ClientSession:
        if self._session is None:
            self._session = ClientSession(timeout=self._timeout)
        return self._session

    async def close(self) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None


class HttpArtifactFetcher(_HttpClientBase, ArtifactFetcher):
    """从 JobManager 下载不可变 ZIP。"""

    async def fetch(self, descriptor: ArtifactDescriptor) -> bytes:
        session = await self._get_session()
        path = (
            f"{self.base_url}/jobs/{quote(descriptor.job_id, safe='')}"
            f"/artifacts/{descriptor.sha256}"
        )
        async with session.get(path) as response:
            response.raise_for_status()
            return await response.read()


class HttpJobManagerClient(_HttpClientBase, RegistrationClient):
    """Worker 到 JobManager 的注册、心跳和失败上报客户端。"""

    async def register(
        self,
        *,
        worker_id: str,
        control_address: str,
        data_host: str,
        data_port: int,
        total_slots: int,
        incarnation_id: str,
    ) -> None:
        await self._post(
            "/workers/register",
            {
                "worker_id": worker_id,
                "control_address": control_address,
                "data_host": data_host,
                "data_port": data_port,
                "total_slots": total_slots,
                "incarnation_id": incarnation_id,
            },
        )

    async def heartbeat(self, worker_id: str) -> None:
        await self._post(f"/workers/{quote(worker_id, safe='')}/heartbeat", {})

    async def report_task_failed(
        self,
        job_id: str,
        task_id: str,
        attempt_id: int,
        error: str,
    ) -> None:
        await self._post(
            f"/jobs/{quote(job_id, safe='')}/tasks/{quote(task_id, safe='')}/status",
            {
                "status": TaskStatus.FAILED.value,
                "attempt_id": attempt_id,
                "error": error,
            },
        )

    async def _post(self, path: str, document: dict[str, Any]) -> None:
        session = await self._get_session()
        async with session.post(f"{self.base_url}{path}", json=document) as response:
            response.raise_for_status()


class HttpWorkerGateway(_HttpClientBase):
    """JobManager 的 WorkerGateway HTTP 实现。"""

    def __init__(
        self,
        *,
        session: ClientSession | None = None,
        timeout: float = 10.0,
    ) -> None:
        super().__init__("", session=session, timeout=timeout)

    async def deploy_task(self, worker: WorkerNode, deployment: TaskDeployment) -> None:
        session = await self._get_session()
        async with session.post(
            f"{worker.control_address.rstrip('/')}/tasks/deploy",
            json=deployment_to_dict(deployment),
        ) as response:
            try:
                response.raise_for_status()
            except ClientResponseError as exc:
                body = await response.text()
                raise WorkerTaskError(f"Worker 部署失败 {response.status}: {body}") from exc

    async def stop_task(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
    ) -> None:
        session = await self._get_session()
        async with session.delete(
            f"{worker.control_address.rstrip('/')}/tasks/{quote(task_id, safe='')}"
            f"?attempt_id={attempt_id}"
        ) as response:
            try:
                response.raise_for_status()
            except ClientResponseError as exc:
                body = await response.text()
                raise WorkerTaskError(f"Worker 停止失败 {response.status}: {body}") from exc

    async def arm_checkpoint(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
    ) -> None:
        await self._checkpoint_request(
            worker,
            task_id,
            attempt_id,
            checkpoint_id,
            "arm",
        )

    async def trigger_checkpoint(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
    ) -> TaskSnapshotDescriptor:
        document = await self._checkpoint_request(
            worker,
            task_id,
            attempt_id,
            checkpoint_id,
            "trigger",
        )
        return TaskSnapshotDescriptor.from_dict(document)

    async def wait_checkpoint(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
    ) -> TaskSnapshotDescriptor:
        document = await self._checkpoint_request(
            worker,
            task_id,
            attempt_id,
            checkpoint_id,
            None,
        )
        return TaskSnapshotDescriptor.from_dict(document)

    async def complete_checkpoint(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
    ) -> None:
        await self._checkpoint_request(
            worker,
            task_id,
            attempt_id,
            checkpoint_id,
            "complete",
        )

    async def abort_checkpoint(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
    ) -> None:
        await self._checkpoint_request(
            worker,
            task_id,
            attempt_id,
            checkpoint_id,
            "abort",
        )

    async def _checkpoint_request(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
        action: str | None,
    ) -> object:
        path = (
            f"{worker.control_address.rstrip('/')}/tasks/{quote(task_id, safe='')}"
            f"/checkpoints/{checkpoint_id}"
        )
        if action is not None:
            path += f"/{action}"
        path += f"?attempt_id={attempt_id}"
        session = await self._get_session()
        method = session.get if action is None else session.post
        # Checkpoint 的总超时由 JobManager coordinator 控制; 不能被 Gateway
        # 的普通 10 秒请求超时提前截断。
        async with method(path, timeout=ClientTimeout(total=None)) as response:
            try:
                response.raise_for_status()
            except ClientResponseError as exc:
                body = await response.text()
                raise WorkerTaskError(
                    f"Worker Checkpoint 请求失败 {response.status}: {body}"
                ) from exc
            if response.status == 204:
                return {}
            return await response.json()


def _snapshot_to_dict(snapshot: RuntimeSnapshot) -> dict[str, Any]:
    document = asdict(snapshot)
    document["state"] = snapshot.state.value
    return document


def _checkpoint_coordinates(request: web.Request) -> tuple[str, int, int]:
    task_id = request.match_info["task_id"]
    try:
        checkpoint_id = int(request.match_info["checkpoint_id"])
    except ValueError as exc:
        raise web.HTTPBadRequest(text="checkpoint_id 必须是非负整数") from exc
    if checkpoint_id < 0:
        raise web.HTTPBadRequest(text="checkpoint_id 必须是非负整数")
    return task_id, _attempt_id(request), checkpoint_id


def _attempt_id(request: web.Request) -> int:
    try:
        attempt_id = int(request.query["attempt_id"])
    except (KeyError, ValueError) as exc:
        raise web.HTTPBadRequest(text="attempt_id 必须是非负整数") from exc
    if attempt_id < 0:
        raise web.HTTPBadRequest(text="attempt_id 必须是非负整数")
    return attempt_id


__all__ = [
    "HttpArtifactFetcher",
    "HttpJobManagerClient",
    "HttpWorkerGateway",
    "RegistrationClient",
    "WorkerHttpService",
    "WorkerServiceConfig",
]
