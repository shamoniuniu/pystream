"""验证中级恢复结果、Sink 重放边界和 Kafka committed offset。"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter
from collections.abc import Sequence
from pathlib import Path

from _demo import kafka_client_options
from _intermediate_demo import (
    BASELINE_ROWS,
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_JOBMANAGER_URL,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_TOPIC,
    EXPECTED_ROWS,
    RECOVERY_PHASE_ROWS,
    load_job_id,
    output_files,
    read_rows,
)
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition

from pystream.client import JobManagerClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="验证中级故障恢复与 At-least-once")
    parser.add_argument("--job-id")
    parser.add_argument("--jobmanager-url", default=DEFAULT_JOBMANAGER_URL)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--min-attempt", type=int, default=1)
    parser.add_argument(
        "--allow-no-duplicate",
        action="store_true",
        help="只验证无丢失; 默认要求故障边界至少产生一条重复输出",
    )
    return parser


def validate_output(
    observed: Counter[tuple[str, int, int]],
    *,
    require_duplicate: bool,
) -> Counter[tuple[str, int, int]]:
    """验证 baseline 精确一次、恢复阶段至少一次，并返回允许的重复行。"""
    missing = EXPECTED_ROWS - observed
    if missing:
        raise RuntimeError(f"中级输出存在缺失: {dict(missing)}")
    unexpected_keys = set(observed) - set(EXPECTED_ROWS)
    if unexpected_keys:
        raise RuntimeError(f"中级输出包含非预期行: {sorted(unexpected_keys)}")
    baseline_duplicates = Counter(
        {
            row: observed[row] - expected_count
            for row, expected_count in BASELINE_ROWS.items()
            if observed[row] > expected_count
        }
    )
    if baseline_duplicates:
        raise RuntimeError(f"Checkpoint 前已完成窗口不应重放: {dict(baseline_duplicates)}")
    duplicates = Counter(
        {
            row: observed[row] - expected_count
            for row, expected_count in RECOVERY_PHASE_ROWS.items()
            if observed[row] > expected_count
        }
    )
    if require_duplicate and not duplicates:
        raise RuntimeError("故障边界未观察到追加 Sink 重复, 无法实证 At-least-once 重放")
    return duplicates


def validate_recovered_status(
    status: dict[str, object],
    *,
    min_attempt: int,
) -> int:
    """验证作业和全部 Task 已处于同一恢复 attempt。"""
    if status.get("status") != "RUNNING":
        raise RuntimeError(f"验证时作业不处于 RUNNING: {status}")
    attempt = status.get("attempt")
    tasks = status.get("tasks")
    if not isinstance(attempt, int) or attempt < min_attempt:
        raise RuntimeError(f"作业 attempt 未达到 {min_attempt}: {attempt!r}")
    if not isinstance(tasks, list) or not tasks:
        raise RuntimeError("恢复状态缺少 tasks")
    task_attempts = {task.get("attempt_id") for task in tasks if isinstance(task, dict)}
    restored = {task.get("restored_checkpoint_id") for task in tasks if isinstance(task, dict)}
    if task_attempts != {attempt}:
        raise RuntimeError(f"Task attempt 不一致: {task_attempts}")
    if len(restored) != 1 or None in restored:
        raise RuntimeError(f"Task restored checkpoint 不一致: {restored}")
    return next(iter(restored))


async def kafka_offsets(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    *,
    metadata_timeout: float = 30.0,
    poll_interval: float = 0.25,
) -> list[dict[str, int]]:
    """读取演示消费组每个 partition 的 committed/end offset。"""
    if metadata_timeout <= 0 or poll_interval < 0:
        raise ValueError("metadata_timeout 必须大于 0, poll_interval 必须非负")
    client_options = kafka_client_options()
    metadata_consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=None,
        enable_auto_commit=False,
        **client_options,
    )
    offset_consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        **client_options,
    )
    await metadata_consumer.start()
    try:
        await offset_consumer.start()
        try:
            deadline = asyncio.get_running_loop().time() + metadata_timeout
            partitions = None
            while asyncio.get_running_loop().time() < deadline:
                remaining = deadline - asyncio.get_running_loop().time()
                try:
                    await asyncio.wait_for(
                        metadata_consumer.topics(),
                        timeout=remaining,
                    )
                except TimeoutError:
                    break
                partitions = metadata_consumer.partitions_for_topic(topic)
                if partitions:
                    break
                await asyncio.sleep(poll_interval)
            if not partitions:
                raise RuntimeError(f"等待 Kafka topic {topic!r} metadata 超时或没有 partition")
            topic_partitions = [
                TopicPartition(topic, partition) for partition in sorted(partitions)
            ]
            end_offsets = await metadata_consumer.end_offsets(topic_partitions)
            result: list[dict[str, int]] = []
            for topic_partition in topic_partitions:
                committed = await offset_consumer.committed(topic_partition)
                end = end_offsets[topic_partition]
                if committed is None:
                    raise RuntimeError(
                        f"partition {topic_partition.partition} 没有 committed offset"
                    )
                result.append(
                    {
                        "partition": topic_partition.partition,
                        "committed_offset": committed,
                        "end_offset": end,
                        "lag": end - committed,
                    }
                )
            return result
        finally:
            await offset_consumer.stop()
    finally:
        await metadata_consumer.stop()


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.min_attempt < 0:
        raise ValueError("--min-attempt 必须非负")
    job_id = load_job_id(args.output_root, args.job_id)
    status = JobManagerClient(args.jobmanager_url).status(job_id)
    restored_checkpoint_id = validate_recovered_status(
        status,
        min_attempt=args.min_attempt,
    )
    files = output_files(args.output_root, job_id)
    if not files:
        raise RuntimeError("没有找到中级 File Sink 输出")
    observed = read_rows(files)
    duplicates = validate_output(
        observed,
        require_duplicate=not args.allow_no_duplicate,
    )
    offsets = asyncio.run(
        kafka_offsets(
            args.bootstrap_servers,
            args.topic,
            f"pystream-{job_id}-words",
        )
    )
    nonzero_lag = [item for item in offsets if item["lag"] != 0]
    if nonzero_lag:
        raise RuntimeError(f"Kafka 输入尚未全部提交: {nonzero_lag}")

    summary = {
        "job_id": job_id,
        "status": status["status"],
        "attempt": status["attempt"],
        "restored_checkpoint_id": restored_checkpoint_id,
        "observed_rows": [
            {
                "window_end": row[0],
                "count": row[1],
                "word_count": row[2],
                "occurrences": occurrences,
            }
            for row, occurrences in sorted(observed.items())
        ],
        "allowed_duplicates": [
            {
                "window_end": row[0],
                "count": row[1],
                "word_count": row[2],
                "extra_occurrences": occurrences,
            }
            for row, occurrences in sorted(duplicates.items())
        ],
        "kafka_offsets": offsets,
        "input_loss": False,
        "sink_duplicates_observed": bool(duplicates),
        "semantics": "at-least-once",
        "exactly_once": False,
        "files": [str(path) for path in files],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
