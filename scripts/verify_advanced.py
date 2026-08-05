"""Verify manifest-visible exactly-once rows and Kafka committed offsets."""

from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Sequence
from pathlib import Path

from _advanced_demo import (
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_JOBMANAGER_URL,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_TOPIC,
    EXPECTED_ROWS,
    canonical_rows,
    load_job_id,
    pending_transaction_count,
    visible_rows,
)
from _demo import kafka_client_options
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition

from pystream.client import JobManagerClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify advanced exactly-once output")
    parser.add_argument("--job-id")
    parser.add_argument("--jobmanager-url", default=DEFAULT_JOBMANAGER_URL)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--min-attempt", type=int, default=0)
    parser.add_argument("--min-epoch", type=int, default=0)
    return parser


async def kafka_offsets(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
) -> list[dict[str, int]]:
    metadata = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=None,
        enable_auto_commit=False,
        **kafka_client_options(),
    )
    offsets = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        **kafka_client_options(),
    )
    await metadata.start()
    try:
        await offsets.start()
        try:
            for _ in range(120):
                await metadata.topics()
                partitions = metadata.partitions_for_topic(topic)
                if partitions:
                    break
                await asyncio.sleep(0.25)
            else:
                raise TimeoutError(f"Kafka topic metadata timed out: {topic}")
            topic_partitions = [
                TopicPartition(topic, partition) for partition in sorted(partitions)
            ]
            end_offsets = await metadata.end_offsets(topic_partitions)
            result: list[dict[str, int]] = []
            for topic_partition in topic_partitions:
                committed = await offsets.committed(topic_partition)
                if committed is None:
                    raise RuntimeError(
                        f"Partition {topic_partition.partition} has no committed offset"
                    )
                end = end_offsets[topic_partition]
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
            await offsets.stop()
    finally:
        await metadata.stop()


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.min_attempt < 0 or args.min_epoch < 0:
        raise ValueError("--min-attempt and --min-epoch must be non-negative")
    job_id = load_job_id(args.output_root, args.job_id)
    status = JobManagerClient(args.jobmanager_url, timeout=30.0).status(job_id)
    attempt = status.get("attempt")
    epoch = status.get("coordinator_epoch")
    checkpoint = status.get("checkpoint")
    if (
        status.get("status") != "RUNNING"
        or not isinstance(attempt, int)
        or attempt < args.min_attempt
        or not isinstance(epoch, int)
        or epoch < args.min_epoch
        or not isinstance(checkpoint, dict)
        or checkpoint.get("last_finalized_id") is None
    ):
        raise RuntimeError(f"Advanced job is not fully finalized: {status}")
    rows, manifests = visible_rows(args.output_root, job_id)
    if rows != EXPECTED_ROWS:
        raise RuntimeError(
            "Manifest-visible rows differ from expected rows: "
            f"missing={dict(EXPECTED_ROWS - rows)}, extra={dict(rows - EXPECTED_ROWS)}"
        )
    offsets = asyncio.run(
        kafka_offsets(
            args.bootstrap_servers,
            args.topic,
            f"pystream-{job_id}-words",
        )
    )
    if any(item["lag"] != 0 for item in offsets):
        raise RuntimeError(f"Kafka lag is non-zero: {offsets}")
    pending_count = pending_transaction_count(args.output_root, job_id)
    if pending_count > 1:
        raise RuntimeError(f"Unexpected orphan pending transactions: {pending_count}")
    summary = {
        "job_id": job_id,
        "status": status["status"],
        "attempt": attempt,
        "coordinator_epoch": epoch,
        "checkpoint": checkpoint,
        "visible_rows": canonical_rows(rows),
        "manifest_checkpoint_ids": [manifest["checkpoint_id"] for manifest in manifests],
        "kafka_offsets": offsets,
        "pending_transactions": pending_count,
        "committed_output_exact": True,
        "semantics": "exactly-once",
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
