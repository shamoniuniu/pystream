"""重置中级 topic，并按阶段写入确定性事件时间数据。"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Sequence

from _demo import kafka_client_options
from _intermediate_demo import DEFAULT_BOOTSTRAP_SERVERS, DEFAULT_TOPIC
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

Message = tuple[int, dict[str, object]]

BASELINE_MESSAGES: tuple[Message, ...] = (
    (
        0,
        {
            "input_id": "baseline-apple-1",
            "word": "APPLE",
            "count": 1,
            "event_time": "2026-07-29T00:00:01Z",
        },
    ),
    (
        1,
        {
            "input_id": "baseline-pie-1",
            "word": "pie",
            "count": 1,
            "event_time": "2026-07-29T00:00:02Z",
        },
    ),
    (
        0,
        {
            "input_id": "baseline-apple-2",
            "word": "apple",
            "count": 1,
            "event_time": "2026-07-29T00:00:03Z",
        },
    ),
    (
        0,
        {
            "input_id": "baseline-clock-1",
            "word": "clock",
            "count": 1,
            "event_time": "2026-07-29T00:00:08Z",
        },
    ),
    (
        1,
        {
            "input_id": "baseline-clock-2",
            "word": "clock",
            "count": 1,
            "event_time": "2026-07-29T00:00:08Z",
        },
    ),
)
RECOVERY_MESSAGES: tuple[Message, ...] = (
    (
        0,
        {
            "input_id": "recovery-banana-1",
            "word": "banana",
            "count": 1,
            "event_time": "2026-07-29T00:00:08.500Z",
        },
    ),
    (
        1,
        {
            "input_id": "recovery-kiwi-1",
            "word": "kiwi",
            "count": 1,
            "event_time": "2026-07-29T00:00:09Z",
        },
    ),
    (
        0,
        {
            "input_id": "recovery-banana-2",
            "word": "BANANA",
            "count": 1,
            "event_time": "2026-07-29T00:00:09.500Z",
        },
    ),
    (
        0,
        {
            "input_id": "recovery-next-1",
            "word": "next-a",
            "count": 1,
            "event_time": "2026-07-29T00:00:13Z",
        },
    ),
    (
        1,
        {
            "input_id": "recovery-next-2",
            "word": "next-b",
            "count": 1,
            "event_time": "2026-07-29T00:00:13Z",
        },
    ),
)
PHASE_MESSAGES = {
    "baseline": BASELINE_MESSAGES,
    "recovery": RECOVERY_MESSAGES,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="生产中级演示事件时间数据")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--phase", choices=tuple(PHASE_MESSAGES), required=True)
    parser.add_argument("--partitions", type=int, default=2)
    parser.add_argument(
        "--keep-topic",
        action="store_true",
        help="baseline 阶段不重置 topic; recovery 阶段始终保留现有 topic",
    )
    return parser


async def reset_topic(bootstrap_servers: str, topic: str, partitions: int) -> None:
    """删除并重建 topic，保证 baseline 从空输入开始。"""
    if partitions != 2:
        raise ValueError("中级确定性数据集要求 --partitions=2")
    admin = AIOKafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        **kafka_client_options(),
    )
    await admin.start()
    try:
        topics = await admin.list_topics()
        if topic in topics:
            await admin.delete_topics([topic])
            for _ in range(60):
                if topic not in await admin.list_topics():
                    break
                await asyncio.sleep(0.25)
            else:
                raise TimeoutError(f"等待 Kafka 删除 topic {topic!r} 超时")
        await admin.create_topics(
            [
                NewTopic(
                    name=topic,
                    num_partitions=partitions,
                    replication_factor=1,
                )
            ]
        )
    finally:
        await admin.close()


async def produce(
    bootstrap_servers: str,
    topic: str,
    messages: Sequence[Message],
) -> list[dict[str, object]]:
    """按指定 partition 写入消息并返回可审计的 offset 清单。"""
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        **kafka_client_options(),
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    await producer.start()
    produced: list[dict[str, object]] = []
    try:
        for partition, message in messages:
            metadata = await producer.send_and_wait(
                topic,
                message,
                partition=partition,
            )
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


async def _run(args: argparse.Namespace) -> None:
    if args.phase == "baseline" and not args.keep_topic:
        await reset_topic(args.bootstrap_servers, args.topic, args.partitions)
    messages = PHASE_MESSAGES[args.phase]
    produced = await produce(args.bootstrap_servers, args.topic, messages)
    print(
        json.dumps(
            {
                "phase": args.phase,
                "topic": args.topic,
                "messages": produced,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    asyncio.run(_run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
