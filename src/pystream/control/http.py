"""JobManager 的 aiohttp 控制面适配层。

本模块把作业提交、状态、取消、Worker 注册/心跳、任务失败上报和制品下载映射
为 HTTP 路由。领域状态仍由 :class:`~pystream.control.manager.JobManager`
维护；HTTP 层只负责校验不可信请求、转换错误并管理心跳巡检生命周期。
"""

from __future__ import annotations

import asyncio
import logging
import re
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Any

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST

from pystream.api import JobConfigError, load_stream_graph
from pystream.artifact import ArtifactError as BundleArtifactError
from pystream.artifact import extract_job_bundle, verify_job_bundle
from pystream.control.errors import (
    ArtifactError,
    ControlPlaneError,
    DeploymentError,
    InsufficientSlots,
    InvalidStateTransition,
    NotLeaderError,
    WorkerNotFound,
)
from pystream.control.leader import LeaderCoordinator
from pystream.control.manager import JobManager
from pystream.control.models import TaskStatus
from pystream.observability import PyStreamMetrics, log_event
from pystream.security import (
    AuthenticationError,
    BearerTokenAuthenticator,
    PeerIdentityError,
    peer_identities,
    require_peer_identity,
)

DEFAULT_MAX_ARTIFACT_SIZE = 64 * 1024 * 1024
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class JobManagerHttpService:
    """把 JobManager 领域服务暴露为版本化 HTTP API。"""

    def __init__(
        self,
        manager: JobManager,
        *,
        max_artifact_size: int = DEFAULT_MAX_ARTIFACT_SIZE,
        reconcile_interval: float = 5.0,
        leadership: LeaderCoordinator | None = None,
        external_auth: BearerTokenAuthenticator | None = None,
        metrics_auth: BearerTokenAuthenticator | None = None,
        require_internal_tls: bool = False,
        metrics: PyStreamMetrics | None = None,
    ) -> None:
        if max_artifact_size <= 0:
            raise ValueError("max_artifact_size 必须大于 0")
        if reconcile_interval <= 0:
            raise ValueError("reconcile_interval 必须大于 0")
        self.manager = manager
        self.max_artifact_size = max_artifact_size
        self.reconcile_interval = reconcile_interval
        self.leadership = leadership
        self.external_auth = external_auth
        self.metrics_auth = metrics_auth
        self.require_internal_tls = require_internal_tls
        self.metrics = metrics or PyStreamMetrics()
        self._reconcile_task: asyncio.Task[None] | None = None

    def create_app(self) -> web.Application:
        """创建带请求大小限制、错误中间件和巡检钩子的应用。"""
        app = web.Application(
            client_max_size=self.max_artifact_size,
            middlewares=[self._error_middleware, self._security_middleware],
        )
        app.router.add_get("/health", self._health)
        app.router.add_get("/health/active", self._active_health)
        app.router.add_get("/health/leader", self._leader_health)
        app.router.add_get("/metrics", self._metrics)
        app.router.add_get("/v1/workers", self._workers)
        app.router.add_post("/v1/jobs", self._submit)
        app.router.add_get("/v1/jobs/{job_id}", self._status)
        app.router.add_post("/v1/jobs/{job_id}/checkpoint", self._checkpoint)
        app.router.add_post("/v1/jobs/{job_id}/cancel", self._cancel)
        app.router.add_post("/workers/register", self._register_worker)
        app.router.add_post("/workers/{worker_id}/heartbeat", self._heartbeat)
        app.router.add_post(
            "/jobs/{job_id}/tasks/{task_id}/status",
            self._task_status,
        )
        app.router.add_get(
            "/jobs/{job_id}/artifacts/{sha256}",
            self._download_artifact,
        )
        app.on_startup.append(self._startup)
        app.on_cleanup.append(self._cleanup)
        return app

    @web.middleware
    async def _security_middleware(
        self,
        request: web.Request,
        handler,
    ) -> web.StreamResponse:
        path = request.path
        if path.startswith("/health"):
            return await handler(request)
        if path == "/metrics":
            identities = peer_identities(request.transport)
            if identities & {"prometheus", "haproxy"}:
                return await handler(request)
            if self.metrics_auth is not None:
                try:
                    self.metrics_auth.authenticate(request.headers.get("Authorization"))
                except AuthenticationError:
                    self.metrics.record_auth_rejection("metrics", "invalid_token")
                    return _authentication_response("Metrics 认证失败")
                return await handler(request)
            if not self.require_internal_tls:
                return await handler(request)
            self.metrics.record_auth_rejection("metrics", "missing_identity")
            return _authentication_response("Metrics 认证失败")
        if path.startswith("/v1/"):
            if self.require_internal_tls and "haproxy" not in peer_identities(request.transport):
                self.metrics.record_auth_rejection(
                    "external_api",
                    "invalid_proxy_identity",
                )
                return _authorization_response("管理 API 代理证书身份不被允许")
            if self.external_auth is None:
                return await handler(request)
            try:
                self.external_auth.authenticate(request.headers.get("Authorization"))
            except AuthenticationError:
                self.metrics.record_auth_rejection("external_api", "invalid_token")
                return _authentication_response("管理 API 认证失败")
            return await handler(request)
        if not self.require_internal_tls:
            return await handler(request)
        try:
            require_peer_identity(
                request.transport,
                prefixes=("worker-",),
            )
        except PeerIdentityError:
            self.metrics.record_tls_failure("jobmanager_http")
            self.metrics.record_auth_rejection("internal_api", "invalid_identity")
            return _authorization_response("内部客户端证书身份不被允许")
        return await handler(request)

    @web.middleware
    async def _error_middleware(
        self,
        request: web.Request,
        handler,
    ) -> web.StreamResponse:
        try:
            return await handler(request)
        except web.HTTPException:
            raise
        except KeyError as exc:
            return _error_response(404, f"资源不存在: {exc}")
        except (JobConfigError, BundleArtifactError, ArtifactError, ValueError) as exc:
            return _error_response(400, str(exc))
        except NotLeaderError as exc:
            return _error_response(
                503,
                str(exc),
                headers={"Retry-After": "1"},
            )
        except (InsufficientSlots, InvalidStateTransition) as exc:
            return _error_response(409, str(exc))
        except WorkerNotFound as exc:
            return _error_response(404, str(exc))
        except DeploymentError as exc:
            return _error_response(502, str(exc))
        except ControlPlaneError as exc:
            return _error_response(409, str(exc))
        except Exception as exc:
            self._log(
                logging.ERROR,
                "request_failed",
                "JobManager 请求发生未处理错误",
                error=f"{type(exc).__name__}: {exc}",
                exc_info=exc,
                method=request.method,
                path=request.path,
            )
            raise

    async def _startup(self, _: web.Application) -> None:
        if self.leadership is not None:
            await self.leadership.start()
        self._reconcile_task = asyncio.create_task(
            self._reconcile_loop(),
            name="pystream-worker-reconcile",
        )

    async def _cleanup(self, _: web.Application) -> None:
        if self.leadership is not None:
            await self.leadership.close()
        task = self._reconcile_task
        self._reconcile_task = None
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        await self.manager.close()

    async def _reconcile_loop(self) -> None:
        while True:
            await asyncio.sleep(self.reconcile_interval)
            with suppress(Exception):
                await self.manager.reconcile_worker_health()

    async def _health(self, _: web.Request) -> web.Response:
        return web.json_response(self.manager.health())

    async def _metrics(self, _: web.Request) -> web.Response:
        self.metrics.update_jobmanager(self.manager, self.leadership)
        return web.Response(
            body=self.metrics.render(),
            headers={"Content-Type": CONTENT_TYPE_LATEST},
        )

    async def _active_health(self, _: web.Request) -> web.Response:
        return _role_health_response(
            self.manager.health(),
            available=self.manager.is_active,
        )

    async def _leader_health(self, _: web.Request) -> web.Response:
        return _role_health_response(
            self.manager.health(),
            available=self.manager.leader_ready,
        )

    async def _workers(self, _: web.Request) -> web.Response:
        workers = [
            {
                "worker_id": view.worker_id,
                "incarnation_id": view.incarnation_id,
                "healthy": view.healthy,
                "total_slots": view.total_slots,
                "used_slots": view.used_slots,
                "available_slots": view.available_slots,
                "control_address": view.control_address,
                "data_host": view.data_host,
                "data_port": view.data_port,
                "last_heartbeat": view.last_heartbeat.isoformat(),
            }
            for view in self.manager.resources()
        ]
        return web.json_response({"workers": workers})

    async def _submit(self, request: web.Request) -> web.Response:
        if request.content_type != "application/zip":
            raise web.HTTPUnsupportedMediaType(text="Content-Type 必须是 application/zip")
        digest = request.headers.get("X-PyStream-SHA256", "")
        if _SHA256.fullmatch(digest) is None:
            raise web.HTTPBadRequest(text="X-PyStream-SHA256 必须是 64 位小写十六进制")
        content = await request.read()
        if not content:
            raise web.HTTPBadRequest(text="作业制品不能为空")
        if len(content) > self.max_artifact_size:
            raise web.HTTPRequestEntityTooLarge(
                max_size=self.max_artifact_size,
                actual_size=len(content),
            )

        with tempfile.TemporaryDirectory(prefix="pystream-submit-") as temporary:
            root = Path(temporary)
            artifact_path = root / "artifact.zip"
            artifact_path.write_bytes(content)
            verify_job_bundle(artifact_path, expected_sha256=digest)
            job_root = root / "job"
            extract_job_bundle(
                artifact_path,
                job_root,
                expected_sha256=digest,
            )
            graph = load_stream_graph(job_root / "job.yaml")

        job = await self.manager.submit_job(
            graph,
            content,
            expected_sha256=digest,
        )
        self._log(
            logging.INFO,
            "job_running",
            "作业已完成部署并进入运行态",
            job_id=job.job_id,
            job_name=job.name,
        )
        return web.json_response(
            {"job_id": job.job_id, "status": job.status.value},
            status=201,
        )

    async def _status(self, request: web.Request) -> web.Response:
        return web.json_response(self.manager.status_view(request.match_info["job_id"]))

    async def _checkpoint(self, request: web.Request) -> web.Response:
        job_id = request.match_info["job_id"]
        manifest = await self.manager.trigger_checkpoint(job_id)
        self._log(
            logging.INFO,
            "checkpoint_triggered",
            "作业已按请求完成 Checkpoint",
            job_id=job_id,
            checkpoint_id=manifest.checkpoint_id,
            attempt_id=manifest.attempt_id,
        )
        return web.json_response(manifest.to_dict())

    async def _cancel(self, request: web.Request) -> web.Response:
        job_id = request.match_info["job_id"]
        execution = self.manager.get_execution_graph(job_id)
        await self.manager.cancel_job(job_id)
        view = self.manager.status_view(job_id)
        view["released_slots"] = execution.total_tasks
        self._log(
            logging.INFO,
            "job_cancelled",
            "作业已取消并释放资源",
            job_id=job_id,
            released_slots=execution.total_tasks,
        )
        return web.json_response(view)

    async def _register_worker(self, request: web.Request) -> web.Response:
        document = await _json_object(request)
        worker_id = _required_string(document, "worker_id")
        self._require_worker_identity(request, worker_id)
        worker, restarted, affected_jobs = await self.manager.register_worker_process(
            worker_id=worker_id,
            incarnation_id=_required_string(document, "incarnation_id"),
            control_address=_required_string(document, "control_address"),
            data_host=_required_string(document, "data_host"),
            data_port=_required_integer(document, "data_port", minimum=1, maximum=65535),
            total_slots=_required_integer(document, "total_slots", minimum=1),
        )
        self._log(
            logging.WARNING if restarted else logging.INFO,
            "worker_re_registered" if restarted else "worker_registered",
            "Worker 新进程已重注册" if restarted else "Worker 已注册",
            worker_id=worker.worker_id,
            incarnation_id=worker.incarnation_id,
            affected_jobs=list(affected_jobs),
            total_slots=worker.total_slots,
        )
        return web.json_response(
            {
                "worker_id": worker.worker_id,
                "incarnation_id": worker.incarnation_id,
                "restarted": restarted,
                "affected_jobs": affected_jobs,
                "total_slots": worker.total_slots,
                "coordinator_epoch": self.manager.coordinator_epoch,
                "status": "REGISTERED",
            },
            status=201,
        )

    async def _heartbeat(self, request: web.Request) -> web.Response:
        worker_id = request.match_info["worker_id"]
        self._require_worker_identity(request, worker_id)
        worker = self.manager.heartbeat(worker_id)
        return web.json_response(
            {
                "worker_id": worker.worker_id,
                "status": "HEALTHY",
                "last_heartbeat": worker.last_heartbeat.isoformat(),
                "coordinator_epoch": self.manager.coordinator_epoch,
            }
        )

    async def _task_status(self, request: web.Request) -> web.Response:
        document = await _json_object(request)
        try:
            status = TaskStatus(_required_string(document, "status"))
        except ValueError as exc:
            raise ValueError(f"不支持的任务状态: {document.get('status')!r}") from exc
        error = document.get("error")
        if error is not None and not isinstance(error, str):
            raise ValueError("error 必须是字符串或 null")
        job = await self.manager.report_task_status(
            request.match_info["job_id"],
            request.match_info["task_id"],
            status,
            error,
            attempt_id=_required_integer(document, "attempt_id", minimum=0),
            coordinator_epoch=_required_integer(
                document,
                "coordinator_epoch",
                minimum=0,
            ),
        )
        self._log(
            logging.ERROR if status is TaskStatus.FAILED else logging.INFO,
            "task_status_reported",
            "Worker 已上报任务状态",
            job_id=job.job_id,
            status=status.value,
            task_id=request.match_info["task_id"],
            attempt_id=document["attempt_id"],
            coordinator_epoch=document["coordinator_epoch"],
            error=error,
        )
        return web.json_response({"job_id": job.job_id, "status": job.status.value})

    async def _download_artifact(self, request: web.Request) -> web.Response:
        content = self.manager.download_artifact(
            request.match_info["job_id"],
            request.match_info["sha256"],
        )
        return web.Response(body=content, content_type="application/zip")

    def _require_worker_identity(self, request: web.Request, worker_id: str) -> None:
        if not self.require_internal_tls:
            return
        if worker_id not in peer_identities(request.transport):
            self.metrics.record_auth_rejection("internal_api", "worker_identity_mismatch")
            raise web.HTTPForbidden(text="Worker certificate identity 与 worker_id 不匹配")

    @staticmethod
    def _log(
        level: int,
        event: str,
        message: str,
        *,
        job_id: str | None = None,
        worker_id: str | None = None,
        exc_info: BaseException | bool | None = None,
        **fields,
    ) -> None:
        log_event(
            logging.getLogger(__name__),
            level,
            event,
            message,
            component="jobmanager",
            job_id=job_id,
            worker_id=worker_id,
            exc_info=exc_info,
            **fields,
        )


async def _json_object(request: web.Request) -> dict[str, Any]:
    try:
        document = await request.json()
    except Exception as exc:
        raise ValueError(f"请求体必须是 JSON object: {exc}") from exc
    if not isinstance(document, dict) or not all(isinstance(key, str) for key in document):
        raise ValueError("请求体必须是 JSON object")
    return document


def _required_string(document: dict[str, Any], field: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} 必须是非空字符串")
    return value


def _required_integer(
    document: dict[str, Any],
    field: str,
    *,
    minimum: int,
    maximum: int | None = None,
) -> int:
    value = document.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{field} 必须是大于等于 {minimum} 的整数")
    if maximum is not None and value > maximum:
        raise ValueError(f"{field} 必须小于等于 {maximum}")
    return value


def _error_response(
    status: int,
    message: str,
    *,
    headers: dict[str, str] | None = None,
) -> web.Response:
    return web.json_response({"error": message}, status=status, headers=headers)


def _authentication_response(message: str) -> web.Response:
    return _error_response(
        401,
        message,
        headers={"WWW-Authenticate": "Bearer"},
    )


def _authorization_response(message: str) -> web.Response:
    return _error_response(403, message)


def _role_health_response(
    health: dict[str, object],
    *,
    available: bool,
) -> web.Response:
    headers = None if available else {"Retry-After": "1"}
    return web.json_response(
        health,
        status=200 if available else 503,
        headers=headers,
    )


__all__ = [
    "DEFAULT_MAX_ARTIFACT_SIZE",
    "JobManagerHttpService",
]
