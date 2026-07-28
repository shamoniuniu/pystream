"""PyStream 容器进程启动入口。

``jobmanager`` 子命令组装本地制品仓库、HTTP Worker 网关和控制面服务；
``worker`` 子命令组装共享 TCP 数据端口、制品下载、TaskRuntime 管理和 Worker
HTTP 服务。模块只负责依赖注入与进程生命周期，不复制领域逻辑。
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from aiohttp import web

from pystream.control import JobManager, JobManagerHttpService, LocalArtifactRepository
from pystream.observability import configure_logging
from pystream.runtime import DataPlaneServer
from pystream.worker import (
    HttpArtifactFetcher,
    HttpJobManagerClient,
    HttpWorkerGateway,
    WorkerHttpService,
    WorkerServiceConfig,
    WorkerTaskManager,
)


def build_parser() -> argparse.ArgumentParser:
    """创建 JobManager 与 Worker 进程参数解析器。"""
    parser = argparse.ArgumentParser(
        prog="python -m pystream.service",
        description="启动 PyStream JobManager 或 Worker 服务",
    )
    subparsers = parser.add_subparsers(dest="service", required=True)

    jobmanager = subparsers.add_parser("jobmanager", help="启动 JobManager HTTP 服务")
    jobmanager.add_argument("--host", default="0.0.0.0")
    jobmanager.add_argument("--port", type=int, default=8080)
    jobmanager.add_argument("--artifact-root", type=Path, default=Path("/data/artifacts"))
    jobmanager.add_argument("--heartbeat-timeout", type=float, default=15.0)
    jobmanager.add_argument("--reconcile-interval", type=float, default=5.0)
    jobmanager.set_defaults(app_factory=_create_jobmanager_app)

    worker = subparsers.add_parser("worker", help="启动 Worker 控制面与数据面")
    worker.add_argument("--worker-id", required=True)
    worker.add_argument("--host", default="0.0.0.0")
    worker.add_argument("--port", type=int, default=8081)
    worker.add_argument("--control-address", required=True)
    worker.add_argument("--data-listen-host", default="0.0.0.0")
    worker.add_argument("--data-host", required=True)
    worker.add_argument("--data-port", type=int, default=9000)
    worker.add_argument("--slots", type=int, default=4)
    worker.add_argument("--heartbeat-interval", type=float, default=5.0)
    worker.add_argument("--jobmanager-url", default="http://jobmanager:8080")
    worker.add_argument("--work-root", type=Path, default=Path("/data/work"))
    worker.set_defaults(app_factory=_create_worker_app)
    return parser


def _create_jobmanager_app(args: argparse.Namespace) -> web.Application:
    from datetime import timedelta

    if args.heartbeat_timeout <= 0:
        raise ValueError("--heartbeat-timeout 必须大于 0")
    gateway = HttpWorkerGateway()
    manager = JobManager(
        LocalArtifactRepository(args.artifact_root),
        gateway,
        heartbeat_timeout=timedelta(seconds=args.heartbeat_timeout),
    )
    service = JobManagerHttpService(
        manager,
        reconcile_interval=args.reconcile_interval,
    )
    app = service.create_app()

    async def close_gateway(_: web.Application) -> None:
        await gateway.close()

    app.on_cleanup.append(close_gateway)
    return app


def _create_worker_app(args: argparse.Namespace) -> web.Application:
    artifact_fetcher = HttpArtifactFetcher(args.jobmanager_url)
    jobmanager_client = HttpJobManagerClient(args.jobmanager_url)
    data_server = DataPlaneServer(args.data_listen_host, args.data_port)
    manager = WorkerTaskManager(
        args.worker_id,
        args.work_root,
        data_server,
        artifact_fetcher,
        status_reporter=jobmanager_client,
    )
    service = WorkerHttpService(
        WorkerServiceConfig(
            worker_id=args.worker_id,
            control_address=args.control_address,
            data_host=args.data_host,
            total_slots=args.slots,
            heartbeat_interval=args.heartbeat_interval,
        ),
        manager,
        data_server,
        jobmanager_client,
    )
    app = service.create_app()

    async def close_clients(_: web.Application) -> None:
        await artifact_fetcher.close()
        await jobmanager_client.close()

    app.on_cleanup.append(close_clients)
    return app


def main(argv: Sequence[str] | None = None) -> int:
    """解析参数并以前台模式运行所选 aiohttp 服务。"""
    args = build_parser().parse_args(argv)
    if not 1 <= args.port <= 65535:
        raise SystemExit("--port 必须位于 1..65535")
    configure_logging()
    app = args.app_factory(args)
    web.run_app(
        app,
        host=args.host,
        port=args.port,
        shutdown_timeout=15.0,
        print=None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["build_parser", "main"]
