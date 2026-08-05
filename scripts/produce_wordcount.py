"""重置 WordCount topic 并写入题目中的三条 JSON 消息。"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Sequence

from _demo import kafka_client_options
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

DEFAULT_BOOTSTRAP_SERVERS = os.getenv(
    "PYSTREAM_KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
DEFAULT_MESSAGES = (
    {"word": "APPLE", "count": 1},
    {"word": "pie", "count": 1},
    {"word": "apple", "count": 1},
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="生产 WordCount 示例数据")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default="words")
    parser.add_argument("--partitions", type=int, default=2)
    parser.add_argument(
        "--keep-topic",
        action="store_true",
        help="不重置 topic; 默认删除并重建以保证演示可重复",
    )
    return parser


async def reset_topic(bootstrap_servers: str, topic: str, partitions: int) -> None:
    """删除并重建 topic，避免历史消息污染重复演示。"""
    if partitions <= 0:
        raise ValueError("--partitions 必须大于 0")
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
    messages: Sequence[dict[str, object]] = DEFAULT_MESSAGES,
) -> None:
    """使用确认级别 all 顺序写入 JSON 消息。"""
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
    try:
        for message in messages:
            await producer.send_and_wait(topic, message)
    finally:
        await producer.stop()


async def _run(args: argparse.Namespace) -> None:
    if not args.keep_topic:
        await reset_topic(args.bootstrap_servers, args.topic, args.partitions)
    await produce(args.bootstrap_servers, args.topic)
    print(f"已向 {args.topic} 写入 {len(DEFAULT_MESSAGES)} 条消息")


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    asyncio.run(_run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
