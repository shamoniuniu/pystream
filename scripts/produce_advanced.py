"""Reset and populate the deterministic advanced acceptance topic."""

from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Sequence

from _advanced_demo import DEFAULT_BOOTSTRAP_SERVERS, DEFAULT_TOPIC
from _demo import kafka_client_options
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

MESSAGES: tuple[tuple[int, dict[str, object]], ...] = (
    (
        0,
        {
            "input_id": "advanced-apple-1",
            "word": "APPLE",
            "count": 1,
            "event_time": "2026-07-29T00:00:01Z",
        },
    ),
    (
        1,
        {
            "input_id": "advanced-pie-1",
            "word": "pie",
            "count": 1,
            "event_time": "2026-07-29T00:00:02Z",
        },
    ),
    (
        0,
        {
            "input_id": "advanced-apple-2",
            "word": "apple",
            "count": 1,
            "event_time": "2026-07-29T00:00:03Z",
        },
    ),
    (
        0,
        {
            "input_id": "advanced-clock-1",
            "word": "clock",
            "count": 1,
            "event_time": "2026-07-29T00:00:08Z",
        },
    ),
    (
        1,
        {
            "input_id": "advanced-clock-2",
            "word": "clock",
            "count": 1,
            "event_time": "2026-07-29T00:00:08Z",
        },
    ),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Produce advanced acceptance input")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--keep-topic", action="store_true")
    return parser


async def reset_topic(bootstrap_servers: str, topic: str) -> None:
    admin = AIOKafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        **kafka_client_options(),
    )
    await admin.start()
    try:
        topics = await admin.list_topics()
        if topic in topics:
            await admin.delete_topics([topic])
            for _ in range(120):
                if topic not in await admin.list_topics():
                    break
                await asyncio.sleep(0.25)
            else:
                raise TimeoutError(f"Kafka topic deletion timed out: {topic}")
        await admin.create_topics(
            [
                NewTopic(
                    name=topic,
                    num_partitions=2,
                    replication_factor=1,
                )
            ]
        )
    finally:
        await admin.close()


async def produce(bootstrap_servers: str, topic: str) -> list[dict[str, object]]:
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        **kafka_client_options(),
        value_serializer=lambda value: json.dumps(
            value,
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    await producer.start()
    produced: list[dict[str, object]] = []
    try:
        for partition, message in MESSAGES:
            metadata = await producer.send_and_wait(topic, message, partition=partition)
            produced.append(
                {
                    "input_id": message["input_id"],
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                }
            )
    finally:
        await producer.stop()
    return produced


async def _run(args: argparse.Namespace) -> dict[str, object]:
    if not args.keep_topic:
        await reset_topic(args.bootstrap_servers, args.topic)
    return {
        "topic": args.topic,
        "messages": await produce(args.bootstrap_servers, args.topic),
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    print(json.dumps(asyncio.run(_run(args)), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
