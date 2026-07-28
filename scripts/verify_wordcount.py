"""验证 WordCount CSV 结果、任务分布和跨 Worker 执行证据。"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from _demo import (
    DEFAULT_JOBMANAGER_URL,
    DEFAULT_OUTPUT_ROOT,
    EXPECTED_WORDCOUNT,
    load_job_id,
    output_files,
    read_counts,
)

from pystream.client import JobManagerClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="验证 WordCount 演示结果")
    parser.add_argument("--job-id")
    parser.add_argument("--jobmanager-url", default=DEFAULT_JOBMANAGER_URL)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser


def cross_worker_hash_channels(tasks: object) -> list[dict[str, str]]:
    """根据 WordCount 的 by_word -> totals HASH 边生成跨 Worker 物理通道证据。"""
    if not isinstance(tasks, list) or not tasks:
        raise RuntimeError("状态响应缺少物理任务")

    by_word = _operator_placements(tasks, "by_word")
    totals = _operator_placements(tasks, "totals")
    if not by_word or not totals:
        raise RuntimeError("状态响应缺少 by_word 或 totals 物理任务")

    channels = [
        {
            "source_task_id": source_task,
            "source_worker_id": source_worker,
            "target_task_id": target_task,
            "target_worker_id": target_worker,
            "partitioning": "hash",
        }
        for source_task, source_worker in by_word
        for target_task, target_worker in totals
        if source_worker != target_worker
    ]
    if not channels:
        raise RuntimeError("缺少 by_word 到 totals 的跨 Worker HASH Shuffle 证据")
    return channels


def _operator_placements(tasks: list[object], operator_id: str) -> list[tuple[str, str]]:
    placements: list[tuple[str, str]] = []
    for task in tasks:
        if not isinstance(task, dict) or task.get("operator_id") != operator_id:
            continue
        task_id = task.get("task_id")
        worker_id = task.get("worker_id")
        if not isinstance(task_id, str) or not task_id:
            raise RuntimeError(f"{operator_id} 任务缺少 task_id")
        if not isinstance(worker_id, str) or not worker_id:
            raise RuntimeError(f"{task_id} 缺少 worker_id")
        placements.append((task_id, worker_id))
    return placements


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    job_id = load_job_id(args.output_root, args.job_id)
    status = JobManagerClient(args.jobmanager_url).status(job_id)
    if status.get("status") != "RUNNING":
        raise RuntimeError(f"验证时作业不处于 RUNNING: {status}")
    tasks = status.get("tasks")
    hash_channels = cross_worker_hash_channels(tasks)
    assert isinstance(tasks, list)
    worker_ids = {
        task.get("worker_id")
        for task in tasks
        if isinstance(task, dict) and isinstance(task.get("worker_id"), str)
    }
    if len(worker_ids) < 2:
        raise RuntimeError(f"物理任务只分布在 {len(worker_ids)} 个 Worker")

    files = output_files(args.output_root, job_id)
    if not files:
        raise RuntimeError("没有找到 WordCount CSV 分片")
    counts, rows = read_counts(files)
    if counts != EXPECTED_WORDCOUNT:
        raise RuntimeError(f"WordCount 结果不匹配: expected={EXPECTED_WORDCOUNT}, actual={counts}")
    if not all(window_end and word == word.lower() for window_end, word, _ in rows):
        raise RuntimeError(f"窗口时间或小写 key 不合法: {rows}")

    summary = {
        "job_id": job_id,
        "status": status["status"],
        "workers": sorted(worker_ids),
        "task_count": len(tasks),
        "cross_worker_hash_channels": hash_channels,
        "counts": dict(sorted(counts.items())),
        "rows": rows,
        "files": [str(path) for path in files],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
