"""PyStream 容器进程启动入口。

``jobmanager`` 子命令组装本地制品仓库、HTTP Worker 网关和控制面服务；
``worker`` 子命令组装共享 TCP 数据端口、制品下载、TaskRuntime 管理和 Worker
HTTP 服务。模块只负责依赖注入与进程生命周期，不复制领域逻辑。
"""

from __future__ import annotations

import argparse
import asyncio
import os
import ssl
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path

from aiohttp import web

from pystream.checkpoint import LocalCheckpointStore, S3CheckpointStore
from pystream.control import (
    CoordinatorRole,
    JobManager,
    JobManagerHttpService,
    LeaderCoordinator,
    LocalArtifactRepository,
    S3ArtifactRepository,
    S3JobMetadataRepository,
    S3LeaderLeaseRepository,
)
from pystream.observability import PyStreamMetrics, configure_logging
from pystream.runtime import DataPlaneServer
from pystream.security import (
    BearerTokenAuthenticator,
    TlsFiles,
    create_client_ssl_context,
    create_server_ssl_context,
    read_secret_file,
)
from pystream.storage import S3ObjectStore
from pystream.worker import (
    HttpArtifactFetcher,
    HttpJobManagerClient,
    HttpWorkerGateway,
    WorkerHttpService,
    WorkerServiceConfig,
    WorkerTaskManager,
)

SERVER_SSL_CONTEXT = web.AppKey("pystream.server_ssl_context", ssl.SSLContext)


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
    jobmanager.add_argument("--checkpoint-root", type=Path, default=Path("/data/checkpoints"))
    _add_object_store_arguments(jobmanager)
    jobmanager.add_argument("--heartbeat-timeout", type=float, default=15.0)
    jobmanager.add_argument("--reconcile-interval", type=float, default=5.0)
    jobmanager.add_argument("--jobmanager-id")
    jobmanager.add_argument("--leader-lease-ttl", type=float, default=10.0)
    jobmanager.add_argument("--leader-renew-interval", type=float, default=3.0)
    jobmanager.add_argument("--leader-poll-interval", type=float, default=1.0)
    _add_tls_arguments(jobmanager)
    jobmanager.add_argument(
        "--external-token-file",
        type=Path,
        default=_environment_path("PYSTREAM_EXTERNAL_TOKEN_FILE"),
    )
    _add_metrics_arguments(jobmanager)
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
    worker.add_argument("--checkpoint-root", type=Path, default=Path("/data/checkpoints"))
    _add_object_store_arguments(worker)
    _add_tls_arguments(worker)
    _add_metrics_arguments(worker)
    worker.add_argument(
        "--kafka-ca-file",
        type=Path,
        default=_environment_path("PYSTREAM_KAFKA_CA_FILE"),
    )
    worker.add_argument(
        "--kafka-cert-file",
        type=Path,
        default=_environment_path("PYSTREAM_KAFKA_CERT_FILE"),
    )
    worker.add_argument(
        "--kafka-key-file",
        type=Path,
        default=_environment_path("PYSTREAM_KAFKA_KEY_FILE"),
    )
    worker.set_defaults(app_factory=_create_worker_app)
    return parser


def _create_jobmanager_app(args: argparse.Namespace) -> web.Application:
    if args.heartbeat_timeout <= 0:
        raise ValueError("--heartbeat-timeout 必须大于 0")
    metrics = PyStreamMetrics()
    tls_files = _tls_files(args)
    client_ssl = create_client_ssl_context(tls_files) if tls_files is not None else None
    server_ssl = create_server_ssl_context(tls_files) if tls_files is not None else None
    if tls_files is not None:
        metrics.observe_certificate(args.jobmanager_id or "jobmanager", tls_files.cert_file)
    gateway = HttpWorkerGateway(ssl_context=client_ssl)
    args._pystream_metrics = metrics
    object_store = _object_store(args)
    if args.jobmanager_id is not None and object_store is None:
        raise ValueError("--jobmanager-id 要求配置对象存储")
    artifact_repository = (
        S3ArtifactRepository(object_store)
        if object_store is not None
        else LocalArtifactRepository(args.artifact_root)
    )
    checkpoint_store = (
        S3CheckpointStore(object_store)
        if object_store is not None
        else LocalCheckpointStore(args.checkpoint_root)
    )
    manager = JobManager(
        artifact_repository,
        gateway,
        heartbeat_timeout=timedelta(seconds=args.heartbeat_timeout),
        checkpoint_store=checkpoint_store,
        metadata_repository=(
            S3JobMetadataRepository(object_store) if object_store is not None else None
        ),
        role=(
            CoordinatorRole.STANDBY if args.jobmanager_id is not None else CoordinatorRole.ACTIVE
        ),
        metrics=metrics,
    )
    leadership = None
    if args.jobmanager_id is not None:
        if object_store is None:  # pragma: no cover - 已由上方显式拒绝
            raise AssertionError("HA JobManager 缺少对象存储")
        leadership = LeaderCoordinator(
            args.jobmanager_id,
            S3LeaderLeaseRepository(object_store),
            on_acquired=manager.activate,
            on_lost=manager.step_down,
            on_standby=manager.become_standby,
            ttl=timedelta(seconds=args.leader_lease_ttl),
            renew_interval=timedelta(seconds=args.leader_renew_interval),
            poll_interval=timedelta(seconds=args.leader_poll_interval),
            metrics=metrics,
        )
    service = JobManagerHttpService(
        manager,
        reconcile_interval=args.reconcile_interval,
        leadership=leadership,
        external_auth=_authenticator(
            args.external_token_file,
            "external management token",
        ),
        metrics_auth=_authenticator(args.metrics_token_file, "metrics token"),
        require_internal_tls=tls_files is not None,
        metrics=metrics,
    )
    app = service.create_app()
    if server_ssl is not None:
        app[SERVER_SSL_CONTEXT] = server_ssl

    async def close_gateway(_: web.Application) -> None:
        await gateway.close()

    app.on_cleanup.append(close_gateway)
    return app


def _create_worker_app(args: argparse.Namespace) -> web.Application:
    metrics = PyStreamMetrics()
    tls_files = _tls_files(args)
    client_ssl = create_client_ssl_context(tls_files) if tls_files is not None else None
    server_ssl = create_server_ssl_context(tls_files) if tls_files is not None else None
    if tls_files is not None:
        metrics.observe_certificate(args.worker_id, tls_files.cert_file)
    artifact_fetcher = HttpArtifactFetcher(
        args.jobmanager_url,
        ssl_context=client_ssl,
    )
    jobmanager_client = HttpJobManagerClient(
        args.jobmanager_url,
        ssl_context=client_ssl,
    )
    data_server = DataPlaneServer(
        args.data_listen_host,
        args.data_port,
        ssl_context=server_ssl,
        metrics=metrics,
    )
    args._pystream_metrics = metrics
    object_store = _object_store(args)
    runtime_options: dict[str, object] = {}
    if client_ssl is not None:

        async def open_data_connection(host: str, port: int):
            return await asyncio.open_connection(
                host,
                port,
                ssl=client_ssl,
                server_hostname=host,
            )

        runtime_options["open_connection"] = open_data_connection
    kafka_tls = _optional_tls_files(
        args.kafka_ca_file,
        args.kafka_cert_file,
        args.kafka_key_file,
        "Kafka",
    )
    kafka_consumer_options = (
        {
            "security_protocol": "SSL",
            "ssl_context": create_client_ssl_context(kafka_tls),
        }
        if kafka_tls is not None
        else {}
    )
    manager = WorkerTaskManager(
        args.worker_id,
        args.work_root,
        data_server,
        artifact_fetcher,
        status_reporter=jobmanager_client,
        checkpoint_root=(args.checkpoint_root if object_store is None else None),
        checkpoint_store=(S3CheckpointStore(object_store) if object_store is not None else None),
        runtime_options=runtime_options,
        kafka_consumer_options=kafka_consumer_options,
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
        metrics_auth=_authenticator(args.metrics_token_file, "metrics token"),
        require_internal_tls=tls_files is not None,
        metrics=metrics,
    )
    app = service.create_app()
    if server_ssl is not None:
        app[SERVER_SSL_CONTEXT] = server_ssl

    async def close_clients(_: web.Application) -> None:
        await artifact_fetcher.close()
        await jobmanager_client.close()

    app.on_cleanup.append(close_clients)
    return app


def _add_object_store_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--object-store-endpoint",
        default=os.environ.get("PYSTREAM_OBJECT_STORE_ENDPOINT"),
    )
    parser.add_argument(
        "--object-store-bucket",
        default=os.environ.get("PYSTREAM_OBJECT_STORE_BUCKET", "pystream"),
    )
    parser.add_argument(
        "--object-store-region",
        default=os.environ.get("PYSTREAM_OBJECT_STORE_REGION", "us-east-1"),
    )
    parser.add_argument(
        "--object-store-access-key-file",
        type=Path,
        default=_environment_path("PYSTREAM_OBJECT_STORE_ACCESS_KEY_FILE"),
    )
    parser.add_argument(
        "--object-store-secret-key-file",
        type=Path,
        default=_environment_path("PYSTREAM_OBJECT_STORE_SECRET_KEY_FILE"),
    )
    parser.add_argument(
        "--object-store-ca-file",
        type=Path,
        default=_environment_path("PYSTREAM_OBJECT_STORE_CA_FILE"),
    )


def _add_tls_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--tls-ca-file",
        type=Path,
        default=_environment_path("PYSTREAM_TLS_CA_FILE"),
    )
    parser.add_argument(
        "--tls-cert-file",
        type=Path,
        default=_environment_path("PYSTREAM_TLS_CERT_FILE"),
    )
    parser.add_argument(
        "--tls-key-file",
        type=Path,
        default=_environment_path("PYSTREAM_TLS_KEY_FILE"),
    )


def _add_metrics_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--metrics-token-file",
        type=Path,
        default=_environment_path("PYSTREAM_METRICS_TOKEN_FILE"),
    )


def _object_store(
    args: argparse.Namespace,
) -> S3ObjectStore | None:
    endpoint = args.object_store_endpoint
    if endpoint is None:
        return None
    if args.object_store_access_key_file is None or args.object_store_secret_key_file is None:
        raise ValueError(
            "对象存储模式要求 --object-store-access-key-file 和 --object-store-secret-key-file"
        )
    access_key = read_secret_file(
        args.object_store_access_key_file,
        "object store access key",
    )
    secret_key = read_secret_file(
        args.object_store_secret_key_file,
        "object store secret key",
    )
    verify: bool | str = (
        str(args.object_store_ca_file) if args.object_store_ca_file is not None else True
    )
    arguments: dict[str, object] = {
        "endpoint_url": endpoint,
        "region_name": args.object_store_region,
        "access_key_id": access_key,
        "secret_access_key": secret_key,
        "verify": verify,
    }
    metrics = getattr(args, "_pystream_metrics", None)
    if metrics is not None:
        arguments["metrics"] = metrics
    store = S3ObjectStore(args.object_store_bucket, **arguments)
    store.probe_conditional_writes()
    return store


def _tls_files(args: argparse.Namespace) -> TlsFiles | None:
    return _optional_tls_files(
        args.tls_ca_file,
        args.tls_cert_file,
        args.tls_key_file,
        "service TLS",
    )


def _optional_tls_files(
    ca_file: Path | None,
    cert_file: Path | None,
    key_file: Path | None,
    name: str,
) -> TlsFiles | None:
    values = (ca_file, cert_file, key_file)
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise ValueError(f"{name} 要求同时配置 CA、certificate 和 private key 文件")
    return TlsFiles(ca_file, cert_file, key_file)


def _authenticator(
    path: Path | None,
    name: str,
) -> BearerTokenAuthenticator | None:
    if path is None:
        return None
    return BearerTokenAuthenticator(read_secret_file(path, name))


def _environment_path(name: str) -> Path | None:
    value = os.environ.get(name)
    return Path(value) if value else None


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
        ssl_context=app.get(SERVER_SSL_CONTEXT),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["build_parser", "main"]
