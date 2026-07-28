"""PyStream 命令行入口。

本模块提供作业校验、制品打包、提交、状态查询和取消流程。命令只依赖公开的
API、制品与 HTTP 客户端边界，不导入 JobManager 领域实现或 Worker 运行时。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import TextIO

from pystream import __version__
from pystream.api import JobConfigError, StreamGraph, load_stream_graph
from pystream.artifact import (
    ArtifactError,
    JobBundle,
    build_job_bundle,
    sha256_file,
    verify_job_bundle,
)
from pystream.client import ClientError, JobManagerClient

DEFAULT_JOBMANAGER_URL = "http://localhost:8080"
TERMINAL_STATUSES = frozenset({"CANCELLED", "FAILED", "REJECTED"})
FAILED_STATUSES = frozenset({"FAILED", "REJECTED"})
EXIT_OK = 0
EXIT_ERROR = 1
EXIT_JOB_FAILED = 3
EXIT_TIMEOUT = 4


def build_parser(*, client_factory=None) -> argparse.ArgumentParser:
    """创建 PyStream 顶层参数解析器。"""
    resolved_client_factory = client_factory or JobManagerClient
    parser = argparse.ArgumentParser(
        prog="pystream",
        description="PyStream 简易分布式流计算系统",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.set_defaults(client_factory=resolved_client_factory)
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")

    validate = subparsers.add_parser("validate", help="校验 YAML 作业并展示 DAG 摘要")
    validate.add_argument("job_yaml", type=Path, help="job.yaml 路径")
    validate.set_defaults(handler=_handle_validate)

    package = subparsers.add_parser("package", help="构建带清单与摘要的 ZIP 作业包")
    package.add_argument("source_dir", type=Path, help="包含 job.yaml 和 UDF 的作业目录")
    package.add_argument(
        "--output-dir",
        type=Path,
        default=Path(".pystream") / "artifacts",
        help="制品输出目录 (默认: .pystream/artifacts)",
    )
    package.set_defaults(handler=_handle_package)

    submit = subparsers.add_parser("submit", help="向 JobManager 上传 ZIP 作业包")
    submit.add_argument("bundle", type=Path, help="package 命令生成的 ZIP")
    _add_client_options(submit)
    submit.set_defaults(handler=_handle_submit)

    status = subparsers.add_parser("status", help="查询作业与物理任务状态")
    status.add_argument("job_id", help="作业 ID")
    status.add_argument("--json", action="store_true", help="输出原始 JSON")
    _add_client_options(status)
    status.set_defaults(handler=_handle_status)

    cancel = subparsers.add_parser("cancel", help="取消作业并等待终态")
    cancel.add_argument("job_id", help="作业 ID")
    cancel.add_argument(
        "--wait-timeout",
        type=float,
        default=30.0,
        help="等待终态的秒数 (默认: 30)",
    )
    cancel.add_argument(
        "--poll-interval",
        type=float,
        default=0.5,
        help="状态轮询间隔秒数 (默认: 0.5)",
    )
    _add_client_options(cancel)
    cancel.set_defaults(handler=_handle_cancel)
    return parser


def _add_client_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--jobmanager-url",
        default=os.getenv("PYSTREAM_JOBMANAGER_URL", DEFAULT_JOBMANAGER_URL),
        help="JobManager 地址",
    )
    parser.add_argument(
        "--http-timeout",
        type=float,
        default=10.0,
        help="单次 HTTP 请求超时秒数 (默认: 10)",
    )


def _handle_validate(args: argparse.Namespace, out: TextIO) -> int:
    graph = load_stream_graph(args.job_yaml)
    _print_graph(graph, out)
    return EXIT_OK


def _handle_package(args: argparse.Namespace, out: TextIO) -> int:
    graph = load_stream_graph(args.source_dir / "job.yaml")
    bundle = build_job_bundle(args.source_dir, args.output_dir)
    _print_graph(graph, out)
    _print_bundle(bundle, out)
    return EXIT_OK


def _handle_submit(args: argparse.Namespace, out: TextIO) -> int:
    bundle_path = args.bundle
    digest = sha256_file(bundle_path)
    manifest = verify_job_bundle(bundle_path, expected_sha256=digest)
    client = _make_client(args)
    response = client.submit(bundle_path.read_bytes(), digest, bundle_path.name)
    job_id = _required_text(response, "job_id")
    status = _optional_text(response, "status") or "SUBMITTED"
    print(f"job_id: {job_id}", file=out)
    print(f"status: {status}", file=out)
    print(f"artifact_sha256: {digest}", file=out)
    print(f"artifact_files: {len(manifest.files)}", file=out)
    return EXIT_JOB_FAILED if status in FAILED_STATUSES else EXIT_OK


def _handle_status(args: argparse.Namespace, out: TextIO) -> int:
    response = _make_client(args).status(args.job_id)
    if args.json:
        print(json.dumps(response, ensure_ascii=False, sort_keys=True), file=out)
    else:
        _print_status(response, out)
    status = _optional_text(response, "status")
    return EXIT_JOB_FAILED if status in FAILED_STATUSES else EXIT_OK


def _handle_cancel(args: argparse.Namespace, out: TextIO) -> int:
    if args.wait_timeout <= 0:
        raise CliError("--wait-timeout 必须大于 0")
    if args.poll_interval <= 0:
        raise CliError("--poll-interval 必须大于 0")
    client = _make_client(args)
    response = client.cancel(args.job_id)
    deadline = time.monotonic() + args.wait_timeout
    while _optional_text(response, "status") not in TERMINAL_STATUSES:
        if time.monotonic() >= deadline:
            raise WaitTimeout(f"等待作业 {args.job_id} 进入终态超时")
        time.sleep(args.poll_interval)
        response = client.status(args.job_id)
    _print_status(response, out)
    _print_released_resources(response, out)
    status = _optional_text(response, "status")
    return EXIT_JOB_FAILED if status in FAILED_STATUSES else EXIT_OK


def _make_client(args: argparse.Namespace) -> JobManagerClient:
    return args.client_factory(
        args.jobmanager_url,
        timeout=args.http_timeout,
    )


def _print_graph(graph: StreamGraph, out: TextIO) -> None:
    print(f"job: {graph.definition.job.name}", file=out)
    print(f"api_version: {graph.definition.api_version}", file=out)
    print(f"total_tasks: {graph.total_parallelism}", file=out)
    print("operators:", file=out)
    for operator_id in graph.topological_order:
        operator = graph.operator(operator_id)
        keyed = str(graph.data_stream(operator_id).keyed).lower()
        print(
            f"  - {operator.id}: type={operator.type.value}, "
            f"parallelism={operator.parallelism}, keyed={keyed}",
            file=out,
        )
    print("edges:", file=out)
    for edge in graph.edges:
        print(
            f"  - {edge.source_id} -> {edge.target_id}: {edge.partitioning.value}",
            file=out,
        )


def _print_bundle(bundle: JobBundle, out: TextIO) -> None:
    print(f"bundle: {bundle.path}", file=out)
    print(f"sha256: {bundle.sha256}", file=out)
    print(f"size: {bundle.size}", file=out)
    print("files:", file=out)
    for entry in bundle.manifest.files:
        print(f"  - {entry.path}: size={entry.size}, sha256={entry.sha256}", file=out)


def _print_status(response: dict[str, object], out: TextIO) -> None:
    print(f"job_id: {_optional_text(response, 'job_id') or '-'}", file=out)
    print(f"name: {_optional_text(response, 'name') or '-'}", file=out)
    print(f"status: {_optional_text(response, 'status') or 'UNKNOWN'}", file=out)
    error = _optional_text(response, "error")
    if error:
        print(f"error: {error}", file=out)
    print("tasks:", file=out)
    tasks = response.get("tasks", [])
    if not isinstance(tasks, list):
        raise ClientError("JobManager 响应字段 tasks 必须是 array")
    for task in tasks:
        if not isinstance(task, dict):
            raise ClientError("JobManager 响应中的 task 必须是 object")
        print(
            "  - "
            f"{task.get('operator_id', '-')}[{task.get('subtask', '-')}] "
            f"status={task.get('status', 'UNKNOWN')} "
            f"worker={task.get('worker_id') or '-'} "
            f"slot={task.get('slot') if task.get('slot') is not None else '-'}"
            + (f" error={task['error']}" if task.get("error") else ""),
            file=out,
        )


def _print_released_resources(response: dict[str, object], out: TextIO) -> None:
    released = response.get("released_slots")
    if released is not None:
        print(f"released_slots: {released}", file=out)


def _required_text(response: dict[str, object], field: str) -> str:
    value = _optional_text(response, field)
    if value is None:
        raise ClientError(f"JobManager 响应缺少字符串字段 {field}")
    return value


def _optional_text(response: dict[str, object], field: str) -> str | None:
    value = response.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ClientError(f"JobManager 响应字段 {field} 必须是字符串")
    return value


class CliError(RuntimeError):
    """命令参数之间的语义错误。"""


class WaitTimeout(CliError):
    """取消作业后等待终态超时。"""


def main(argv: Sequence[str] | None = None, *, client_factory=None) -> int:
    """解析并执行命令，将可预期错误转换为稳定退出码。"""
    parser = build_parser(client_factory=client_factory)
    args = parser.parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return EXIT_OK
    try:
        return handler(args, sys.stdout)
    except WaitTimeout as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return EXIT_TIMEOUT
    except (ArtifactError, CliError, ClientError, JobConfigError, OSError) as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return EXIT_ERROR


__all__ = [
    "EXIT_ERROR",
    "EXIT_JOB_FAILED",
    "EXIT_OK",
    "EXIT_TIMEOUT",
    "build_parser",
    "main",
]
