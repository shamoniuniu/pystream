"""等待中级作业完成 baseline 或恢复后的 Checkpoint。"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Sequence
from pathlib import Path

from _intermediate_demo import (
    DEFAULT_JOBMANAGER_URL,
    DEFAULT_OUTPUT_ROOT,
    load_job_id,
)

from pystream.client import JobManagerClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="等待 PyStream 完整 Checkpoint")
    parser.add_argument("--job-id")
    parser.add_argument("--jobmanager-url", default=DEFAULT_JOBMANAGER_URL)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--min-checkpoint", type=int, default=1)
    parser.add_argument("--min-attempt", type=int, default=0)
    parser.add_argument(
        "--require-post-recovery",
        action="store_true",
        help="要求 last completed checkpoint 严格晚于 Task 恢复点",
    )
    parser.add_argument(
        "--trigger",
        action="store_true",
        help="等待前先请求 JobManager 立即执行一次 Checkpoint",
    )
    parser.add_argument("--timeout", type=float, default=60.0)
    return parser


def checkpoint_ready(
    status: dict[str, object],
    *,
    min_checkpoint: int,
    min_attempt: int,
    require_post_recovery: bool,
) -> bool:
    """判断状态响应是否达到完整 Checkpoint 门槛。"""
    if status.get("status") != "RUNNING":
        return False
    attempt = status.get("attempt")
    checkpoint = status.get("checkpoint")
    if not isinstance(attempt, int) or attempt < min_attempt:
        return False
    if not isinstance(checkpoint, dict):
        return False
    completed = checkpoint.get("last_completed_id")
    if not isinstance(completed, int) or completed < min_checkpoint:
        return False
    if not require_post_recovery:
        return True
    tasks = status.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        return False
    restored = {task.get("restored_checkpoint_id") for task in tasks if isinstance(task, dict)}
    return len(restored) == 1 and None not in restored and completed > next(iter(restored))


def wait_for_checkpoint(
    client: JobManagerClient,
    *,
    job_id: str,
    min_checkpoint: int,
    min_attempt: int,
    require_post_recovery: bool,
    timeout: float,
    poll_interval: float = 0.25,
) -> dict[str, object]:
    """轮询状态，直到 Checkpoint 门槛满足或作业失败。"""
    deadline = time.monotonic() + timeout
    last: dict[str, object] = {}
    while time.monotonic() < deadline:
        last = client.status(job_id)
        state = last.get("status")
        if state in {"FAILED", "REJECTED", "CANCELLED"}:
            raise RuntimeError(f"等待 Checkpoint 时作业进入 {state}: {last.get('error')}")
        if checkpoint_ready(
            last,
            min_checkpoint=min_checkpoint,
            min_attempt=min_attempt,
            require_post_recovery=require_post_recovery,
        ):
            return last
        time.sleep(poll_interval)
    raise TimeoutError(f"等待完整 Checkpoint 超时; last_status={last}")


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.min_checkpoint < 0 or args.min_attempt < 0 or args.timeout <= 0:
        raise ValueError("Checkpoint/attempt 必须非负, timeout 必须大于 0")
    job_id = load_job_id(args.output_root, args.job_id)
    client = JobManagerClient(args.jobmanager_url, timeout=args.timeout)
    if args.trigger:
        client.trigger_checkpoint(job_id)
    status = wait_for_checkpoint(
        client,
        job_id=job_id,
        min_checkpoint=args.min_checkpoint,
        min_attempt=args.min_attempt,
        require_post_recovery=args.require_post_recovery,
        timeout=args.timeout,
    )
    print(
        json.dumps(
            {
                "job_id": job_id,
                "attempt": status["attempt"],
                "checkpoint": status["checkpoint"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
