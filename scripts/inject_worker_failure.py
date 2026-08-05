"""SIGKILL 一个状态 Worker，并验证容器拉起、重注册和整作业恢复。"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from pystream.client import JobManagerClient

DEFAULT_COMPOSE_FILE = Path(__file__).resolve().parents[1] / "deploy" / "compose.yaml"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="注入 Worker SIGKILL 并验证自动恢复")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--jobmanager-url", default="http://localhost:8080")
    parser.add_argument("--compose-file", type=Path, default=DEFAULT_COMPOSE_FILE)
    parser.add_argument("--operator-id", default="word_totals")
    parser.add_argument(
        "--evidence-path",
        type=Path,
        default=Path("reports/intermediate-failure-evidence.json"),
    )
    parser.add_argument("--timeout", type=float, default=120.0)
    return parser


def select_target_worker(status: dict[str, object], operator_id: str) -> str:
    """选择承载指定状态算子的 Worker。"""
    tasks = status.get("tasks")
    if not isinstance(tasks, list):
        raise RuntimeError("作业状态缺少 tasks")
    for task in tasks:
        if not isinstance(task, dict) or task.get("operator_id") != operator_id:
            continue
        worker_id = task.get("worker_id")
        if isinstance(worker_id, str) and worker_id:
            return worker_id
    raise RuntimeError(f"找不到 operator={operator_id!r} 的 Worker")


def _worker_incarnation(workers: dict[str, object], worker_id: str) -> str:
    entries = workers.get("workers")
    if not isinstance(entries, list):
        raise RuntimeError("Worker 状态响应缺少 workers")
    for worker in entries:
        if not isinstance(worker, dict) or worker.get("worker_id") != worker_id:
            continue
        incarnation = worker.get("incarnation_id")
        if isinstance(incarnation, str) and incarnation:
            return incarnation
    raise RuntimeError(f"Worker {worker_id!r} 缺少 incarnation")


def _run(command: Sequence[str]) -> str:
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
    )
    return result.stdout.strip()


def _container_id(compose_file: Path, service: str) -> str:
    del compose_file
    return f"pystream-{service}-1"


def _inspect(container_id: str) -> dict[str, object]:
    document = json.loads(_run(["docker", "inspect", container_id]))
    if not isinstance(document, list) or len(document) != 1:
        raise RuntimeError(f"docker inspect 返回非法结果: {document!r}")
    item = document[0]
    if not isinstance(item, dict):
        raise RuntimeError("docker inspect item 必须是 object")
    return item


def _docker_state(document: dict[str, object]) -> tuple[int, bool, str]:
    restart_count = document.get("RestartCount")
    state = document.get("State")
    if not isinstance(restart_count, int) or not isinstance(state, dict):
        raise RuntimeError("docker inspect 缺少 RestartCount/State")
    running = state.get("Running")
    started_at = state.get("StartedAt")
    if not isinstance(running, bool) or not isinstance(started_at, str):
        raise RuntimeError("docker inspect State 字段非法")
    return restart_count, running, started_at


def _task_restore_ids(status: dict[str, object]) -> set[int | None]:
    tasks = status.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        return set()
    return {
        task.get("restored_checkpoint_id")
        for task in tasks
        if isinstance(task, dict)
        and (
            task.get("restored_checkpoint_id") is None
            or isinstance(task.get("restored_checkpoint_id"), int)
        )
    }


def _recovery_attempts(status: dict[str, object]) -> int:
    recovery = status.get("recovery")
    attempts = recovery.get("attempts") if isinstance(recovery, dict) else None
    if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0:
        raise RuntimeError("作业状态缺少合法 recovery.attempts")
    return attempts


def run_experiment(args: argparse.Namespace) -> dict[str, object]:
    """执行单 Worker 故障实验并返回结构化证据。"""
    if args.timeout <= 0:
        raise ValueError("--timeout 必须大于 0")
    client = JobManagerClient(args.jobmanager_url, timeout=3)
    before_status = client.status(args.job_id)
    if before_status.get("status") != "RUNNING":
        raise RuntimeError(f"故障注入前作业不处于 RUNNING: {before_status}")
    before_attempt = before_status.get("attempt")
    checkpoint = before_status.get("checkpoint")
    if not isinstance(before_attempt, int) or not isinstance(checkpoint, dict):
        raise RuntimeError("故障注入前状态缺少 attempt/checkpoint")
    before_recovery_attempts = _recovery_attempts(before_status)
    before_checkpoint = checkpoint.get("last_completed_id")
    if not isinstance(before_checkpoint, int):
        raise RuntimeError("故障注入前没有完整 Checkpoint")

    worker_id = select_target_worker(before_status, args.operator_id)
    old_incarnation = _worker_incarnation(client.workers(), worker_id)
    container_id = _container_id(args.compose_file.resolve(), worker_id)
    before_restart_count, _, before_started_at = _docker_state(_inspect(container_id))

    injected_at = datetime.now(UTC)
    _run(
        [
            "docker",
            "exec",
            container_id,
            "/bin/sh",
            "-c",
            "kill -9 $(cat /proc/1/task/1/children)",
        ]
    )

    trace: list[dict[str, object]] = []
    last_job_state: object = None
    final_status: dict[str, object] = {}
    final_restart_count = before_restart_count
    final_started_at = before_started_at
    new_incarnation = old_incarnation
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        try:
            final_restart_count, running, final_started_at = _docker_state(_inspect(container_id))
            workers = client.workers()
            new_incarnation = _worker_incarnation(workers, worker_id)
            final_status = client.status(args.job_id)
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
            time.sleep(0.1)
            continue

        job_state = final_status.get("status")
        if job_state != last_job_state:
            trace.append(
                {
                    "observed_at": datetime.now(UTC).isoformat(),
                    "status": job_state,
                    "attempt": final_status.get("attempt"),
                }
            )
            last_job_state = job_state
        final_attempt = final_status.get("attempt")
        final_recovery_attempts = _recovery_attempts(final_status)
        restored_ids = _task_restore_ids(final_status)
        tasks = final_status.get("tasks")
        if not isinstance(tasks, list):
            time.sleep(0.1)
            continue
        task_attempts = {task.get("attempt_id") for task in tasks if isinstance(task, dict)}
        if (
            running
            and final_restart_count > before_restart_count
            and final_started_at != before_started_at
            and new_incarnation != old_incarnation
            and final_status.get("status") == "RUNNING"
            and isinstance(final_attempt, int)
            and final_attempt > before_attempt
            and task_attempts == {final_attempt}
            and len(restored_ids) == 1
            and None not in restored_ids
            and next(iter(restored_ids)) >= before_checkpoint
            and final_recovery_attempts > before_recovery_attempts
        ):
            break
        time.sleep(0.1)
    else:
        raise TimeoutError(
            "Worker SIGKILL 恢复超时: "
            f"restart={before_restart_count}->{final_restart_count}, "
            f"incarnation={old_incarnation}->{new_incarnation}, "
            f"status={final_status}, trace={trace}"
        )

    evidence = {
        "job_id": args.job_id,
        "target_operator": args.operator_id,
        "worker_id": worker_id,
        "container_id": container_id,
        "injected_at": injected_at.isoformat(),
        "restart_count": {
            "before": before_restart_count,
            "after": final_restart_count,
        },
        "started_at": {
            "before": before_started_at,
            "after": final_started_at,
        },
        "incarnation_id": {
            "before": old_incarnation,
            "after": new_incarnation,
        },
        "attempt": {
            "before": before_attempt,
            "after": final_status["attempt"],
        },
        "recovery_attempts": {
            "before": before_recovery_attempts,
            "after": _recovery_attempts(final_status),
        },
        "checkpoint_before_failure": before_checkpoint,
        "restored_checkpoint_ids": sorted(_task_restore_ids(final_status)),
        "state_trace": trace,
        "result": "passed",
    }
    path = args.evidence_path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
    return evidence


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    evidence = run_experiment(args)
    print(json.dumps(evidence, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
