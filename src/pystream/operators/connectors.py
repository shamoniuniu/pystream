"""Kafka JSON Source 与 CSV 文件 Sink。

Source 使用可注入的异步 consumer 工厂，生产环境延迟导入 ``aiokafka``，测试环境
则使用内存 fake。所有 Source subtasks 共享同一消费组但使用不同 client id；
``enable_auto_commit=False`` 明确保持第一阶段的手动 offset 模式。

Sink 复用 :class:`~pystream.operators.base.BaseOperator` 生命周期，将每个 subtask
写入独立 CSV 分片。第一阶段采用普通追加写，不提供恢复或 Exactly-once 语义。
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import re
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, TextIO, cast

from pystream.api import EventTimeExecutionConfig, FileSinkConfig, KafkaSourceConfig
from pystream.checkpoint import CheckpointError, decode_state, encode_state
from pystream.common import (
    JsonPointerError,
    JsonValue,
    MessageType,
    RecordEnvelope,
    resolve_json_pointer,
)
from pystream.observability import log_event
from pystream.operators.base import (
    BaseOperator,
    OperatorContext,
    OperatorState,
    RecordT,
)
from pystream.operators.errors import (
    BadRecordError,
    FileSinkError,
    KafkaSourceError,
    RecordValidationError,
    UnsupportedStateOperation,
)

_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_RFC3339 = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    r"(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)


class KafkaMessage(Protocol):
    """Source 从 Kafka client 读取的最小消息结构。"""

    topic: str
    partition: int
    offset: int
    value: bytes | bytearray | memoryview | str


class AsyncKafkaConsumer(Protocol):
    """隔离具体 Kafka client 的异步消费契约。"""

    async def start(self) -> None:
        """建立 broker 连接并加入消费组。"""

    async def stop(self) -> None:
        """离开消费组并释放网络资源。"""

    async def commit(self, offsets: dict[object, object] | None = None) -> None:
        """提交当前已消费 offset。"""

    def __aiter__(self) -> AsyncIterator[KafkaMessage]:
        """持续返回 Kafka 消息。"""


KafkaConsumerFactory = Callable[..., AsyncKafkaConsumer]
TopicPartitionFactory = Callable[[str, int], object]
FileOpener = Callable[[Path], TextIO]
PayloadValidator = Callable[[JsonValue], object]


@dataclass(slots=True)
class _PartitionEventTimeState:
    """一个 Kafka partition 的 Watermark 生成状态。"""

    last_activity: float
    max_event_time: datetime | None = None


def _create_aiokafka_consumer(*topics: str, **kwargs: Any) -> AsyncKafkaConsumer:
    """延迟创建生产 consumer，使 fake 测试不依赖正在运行的 broker。"""
    try:
        from aiokafka import AIOKafkaConsumer
    except ImportError as exc:  # pragma: no cover - 干净运行环境应安装项目依赖
        raise KafkaSourceError("缺少 aiokafka 依赖, 无法启动 Kafka Source") from exc
    return cast(AsyncKafkaConsumer, AIOKafkaConsumer(*topics, **kwargs))


def _create_topic_partition(topic: str, partition: int) -> object:
    """延迟创建 TopicPartition，使 fake 测试无需依赖具体 Kafka 类型。"""
    try:
        from aiokafka.structs import TopicPartition
    except ImportError as exc:  # pragma: no cover - 干净运行环境应安装项目依赖
        raise KafkaSourceError("缺少 aiokafka 依赖, 无法分配 Kafka partition") from exc
    return TopicPartition(topic, partition)


def _open_text_append(path: Path) -> TextIO:
    """以 UTF-8、newline 透明模式打开追加文件。"""
    return path.open("a", encoding="utf-8", newline="")


def _require_safe_segment(value: str, *, field: str) -> str:
    if _SAFE_SEGMENT.fullmatch(value) is None or value in {".", ".."}:
        raise ValueError(f"{field} 只能包含字母、数字、点、下划线和连字符, 且不能是路径片段")
    return value


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"不支持非有限 JSON 常量 {value}")


class KafkaJsonSource:
    """将 Kafka JSON 消息转换为可追踪的 :class:`RecordEnvelope`。"""

    def __init__(
        self,
        context: OperatorContext,
        *,
        job_id: str,
        config: KafkaSourceConfig,
        consumer_factory: KafkaConsumerFactory = _create_aiokafka_consumer,
        topic_partition_factory: TopicPartitionFactory = _create_topic_partition,
        source_parallelism: int = 1,
        payload_validator: PayloadValidator | None = None,
        event_time_strategy: EventTimeExecutionConfig | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.context = context
        self.job_id = _require_safe_segment(job_id, field="job_id")
        self.config = config
        self._consumer_factory = consumer_factory
        self._topic_partition_factory = topic_partition_factory
        self._source_parallelism = source_parallelism
        self._payload_validator = payload_validator
        self._event_time_strategy = event_time_strategy
        self._logger = logger or logging.getLogger(__name__)
        self._consumer: AsyncKafkaConsumer | None = None
        self._state = OperatorState.CREATED
        self._records_read = 0
        self._records_replayed = 0
        self._bad_records = 0
        self._watermarks_emitted = 0
        self._partition_event_times: dict[tuple[str, int], _PartitionEventTimeState] = {}
        self._last_watermark: datetime | None = None
        self._next_offsets: dict[tuple[str, int], int] = {}
        self._checkpoint_partitions: tuple[object, ...] = ()
        self._pending_restore: dict[
            tuple[str, int],
            tuple[int | None, datetime | None],
        ] = {}
        self._restore_applied: set[tuple[str, int]] = set()
        self._paused = False
        self._resume_event = asyncio.Event()
        self._resume_event.set()
        if (
            isinstance(source_parallelism, bool)
            or not isinstance(source_parallelism, int)
            or source_parallelism <= 0
        ):
            raise ValueError("source_parallelism 必须是正整数")
        if context.subtask_index >= source_parallelism:
            raise ValueError("Source subtask_index 必须小于 source_parallelism")
        if (config.event_time is None) != (event_time_strategy is None):
            raise ValueError("Source event_time 提取器与 execution.event_time 策略必须同时配置")

    @property
    def state(self) -> OperatorState:
        """返回异步 Source 生命周期状态。"""
        return self._state

    @property
    def group_id(self) -> str:
        """返回显式 group id 或作业内所有 Source subtasks 共享的默认值。"""
        return self.config.group_id or f"pystream-{self.job_id}-{self.context.operator_id}"

    @property
    def metrics(self) -> dict[str, int]:
        """返回连接器级输入与坏记录计数。"""
        metrics = {
            "records_read": self._records_read,
            "bad_records": self._bad_records,
        }
        if self._records_replayed:
            metrics["records_replayed"] = self._records_replayed
        if self._event_time_strategy is not None:
            metrics["watermarks_emitted"] = self._watermarks_emitted
        return metrics

    async def open(self) -> None:
        """创建 consumer，并以关闭自动提交的方式加入消费组。"""
        if self._state is not OperatorState.CREATED:
            raise KafkaSourceError(f"无法从 {self._state} 打开 Kafka Source")
        client_id = f"{self.group_id}-{self.context.operator_id}-{self.context.subtask_index}"
        consumer: AsyncKafkaConsumer | None = None
        try:
            consumer = self._consumer_factory(
                self.config.topic,
                bootstrap_servers=self.config.bootstrap_servers,
                group_id=self.group_id,
                client_id=client_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )
            await consumer.start()
            self._assign_partitions(consumer)
        except Exception as exc:
            if consumer is not None:
                with suppress(Exception):
                    await consumer.stop()
            self._consumer = None
            raise KafkaSourceError(
                f"启动 Kafka Source 失败 topic={self.config.topic!r}: {exc}"
            ) from exc
        self._consumer = consumer
        self._state = OperatorState.OPEN

    def _assign_partitions(self, consumer: AsyncKafkaConsumer) -> None:
        """多并发 Source 按 partition 编号静态分片，避免正常部署 rebalance 重放。"""
        if self._source_parallelism == 1:
            return
        partitions_for_topic = getattr(consumer, "partitions_for_topic", None)
        unsubscribe = getattr(consumer, "unsubscribe", None)
        assign = getattr(consumer, "assign", None)
        if not callable(partitions_for_topic) or not callable(unsubscribe) or not callable(assign):
            raise KafkaSourceError("Kafka consumer 不支持静态 partition 分配")
        partitions = partitions_for_topic(self.config.topic)
        if not partitions:
            raise KafkaSourceError(f"Kafka topic {self.config.topic!r} 没有可分配 partition")
        selected = sorted(
            partition
            for partition in partitions
            if partition % self._source_parallelism == self.context.subtask_index
        )
        if not selected:
            raise KafkaSourceError(
                f"Source parallelism={self._source_parallelism} 超过 topic partition 分配能力"
            )
        unsubscribe()
        assigned = [
            self._topic_partition_factory(self.config.topic, partition) for partition in selected
        ]
        assign(assigned)
        log_event(
            self._logger,
            logging.INFO,
            "source_partitions_assigned",
            "Kafka Source 已完成确定性 partition 分配",
            component="kafka_source",
            job_id=self.job_id,
            operator_id=self.context.operator_id,
            subtask=self.context.subtask_index,
            partitions=selected,
            source_parallelism=self._source_parallelism,
        )

    async def records(self) -> AsyncIterator[RecordEnvelope]:
        """持续读取消息；skip 策略隔离坏记录，fail 策略立即终止任务。"""
        consumer = self._require_open()
        if self._event_time_strategy is None:
            try:
                self._refresh_assignment(consumer)
                async for message in consumer:
                    await self._resume_event.wait()
                    restored = self._refresh_assignment(consumer)
                    if (message.topic, message.partition) in restored:
                        continue
                    if not self._record_consumed(message):
                        continue
                    record = self._decode(message)
                    if record is not None:
                        self._records_read += 1
                        yield record
            except BadRecordError:
                raise
            except Exception as exc:
                raise KafkaSourceError(
                    f"消费 Kafka topic={self.config.topic!r} 失败: {exc}"
                ) from exc
            return

        iterator = consumer.__aiter__()
        pending: asyncio.Task[KafkaMessage] | None = None
        poll_interval = min(max(self._event_time_strategy.idle_timeout_seconds / 2, 0.01), 1.0)
        try:
            self._refresh_assignment(consumer)
            while True:
                if pending is None:
                    pending = asyncio.create_task(anext(iterator))
                done, _ = await asyncio.wait({pending}, timeout=poll_interval)
                if not done:
                    self._refresh_assignment(consumer)
                    watermark = self._next_watermark()
                    if watermark is not None:
                        yield watermark
                    continue
                try:
                    message = pending.result()
                except StopAsyncIteration:
                    return
                finally:
                    pending = None
                await self._resume_event.wait()
                restored = self._refresh_assignment(consumer)
                if (message.topic, message.partition) in restored:
                    continue
                if not self._record_consumed(message):
                    continue
                record = self._decode(message)
                if record is not None:
                    self._records_read += 1
                    self._observe_event_time(message, record)
                    yield record
                    watermark = self._next_watermark()
                    if watermark is not None:
                        yield watermark
        except BadRecordError:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise KafkaSourceError(f"消费 Kafka topic={self.config.topic!r} 失败: {exc}") from exc
        finally:
            if pending is not None and not pending.done():
                pending.cancel()
                with suppress(asyncio.CancelledError):
                    await pending

    async def commit(self) -> None:
        """显式提交 offset；第一阶段绝不在后台自动提交。"""
        consumer = self._require_open()
        try:
            await consumer.commit()
        except Exception as exc:
            raise KafkaSourceError(f"提交 Kafka offset 失败: {exc}") from exc

    async def pause(self) -> None:
        """暂停当前 assignment，供停流 Checkpoint 排空数据。"""
        consumer = self._require_open()
        assignment = getattr(consumer, "assignment", None)
        pause = getattr(consumer, "pause", None)
        if not callable(assignment) or not callable(pause):
            raise KafkaSourceError("Kafka consumer 不支持 pause/assignment")
        partitions = tuple(assignment())
        if partitions:
            pause(*partitions)
        self._checkpoint_partitions = partitions
        self._paused = True
        self._resume_event.clear()

    async def resume(self) -> None:
        """恢复当前 assignment 的消费。"""
        consumer = self._require_open()
        assignment = getattr(consumer, "assignment", None)
        resume = getattr(consumer, "resume", None)
        if not callable(assignment) or not callable(resume):
            raise KafkaSourceError("Kafka consumer 不支持 resume/assignment")
        partitions = tuple(assignment())
        if partitions:
            resume(*partitions)
        self._checkpoint_partitions = ()
        self._paused = False
        self._resume_event.set()

    def snapshot_state(self) -> bytes:
        """返回分区 next offset 和 Watermark 基线。"""
        if not self._paused:
            raise KafkaSourceError("Kafka Source 只有在 pause 后才能 snapshot")
        partitions = []
        assigned = sorted(
            self._checkpoint_partitions,
            key=lambda item: (item.topic, item.partition),
        )
        for assigned_partition in assigned:
            topic = assigned_partition.topic
            partition = assigned_partition.partition
            key = (topic, partition)
            event_state = self._partition_event_times.get(key)
            partitions.append(
                {
                    "topic": topic,
                    "partition": partition,
                    "next_offset": self._next_offsets.get(key),
                    "max_event_time": (
                        event_state.max_event_time.isoformat()
                        if event_state is not None and event_state.max_event_time is not None
                        else None
                    ),
                }
            )
        return encode_state(
            "kafka-source",
            {
                "topic": self.config.topic,
                "partitions": partitions,
                "last_watermark": (
                    self._last_watermark.isoformat() if self._last_watermark is not None else None
                ),
            },
        )

    async def restore_state(self, snapshot: bytes) -> None:
        """按当前 consumer assignment 恢复分区位置和事件时间状态。"""
        consumer = self._require_open()
        try:
            state = decode_state(snapshot, "kafka-source")
        except CheckpointError as exc:
            raise KafkaSourceError(f"Kafka Source snapshot 非法: {exc}") from exc
        if set(state) != {"topic", "partitions", "last_watermark"}:
            raise KafkaSourceError("Kafka Source snapshot 字段集合不匹配")
        if state["topic"] != self.config.topic:
            raise KafkaSourceError("Kafka Source snapshot topic 不匹配")
        raw_partitions = state["partitions"]
        if not isinstance(raw_partitions, list):
            raise KafkaSourceError("Kafka Source snapshot partitions 必须是 array")
        restored: dict[tuple[str, int], tuple[int | None, datetime | None]] = {}
        for index, item in enumerate(raw_partitions):
            if not isinstance(item, dict) or set(item) != {
                "topic",
                "partition",
                "next_offset",
                "max_event_time",
            }:
                raise KafkaSourceError(f"Kafka Source partitions[{index}] 字段错误")
            topic = item["topic"]
            partition = item["partition"]
            next_offset = item["next_offset"]
            if (
                not isinstance(topic, str)
                or isinstance(partition, bool)
                or not isinstance(partition, int)
                or partition < 0
                or (
                    next_offset is not None
                    and (
                        isinstance(next_offset, bool)
                        or not isinstance(next_offset, int)
                        or next_offset < 0
                    )
                )
            ):
                raise KafkaSourceError(f"Kafka Source partitions[{index}] 身份或 offset 非法")
            raw_max_event_time = item["max_event_time"]
            max_event_time = (
                None
                if raw_max_event_time is None
                else _parse_snapshot_datetime(raw_max_event_time, "max_event_time")
            )
            key = (topic, partition)
            if key in restored:
                raise KafkaSourceError("Kafka Source snapshot 包含重复 partition")
            restored[key] = (next_offset, max_event_time)
        raw_watermark = state["last_watermark"]
        last_watermark = (
            None
            if raw_watermark is None
            else _parse_snapshot_datetime(raw_watermark, "last_watermark")
        )
        self._pending_restore = restored
        self._restore_applied.clear()
        self._last_watermark = last_watermark
        self._refresh_assignment(consumer)

    async def commit_checkpoint(self) -> None:
        """提交 snapshot 中记录的精确 next offsets。"""
        consumer = self._require_open()
        if not self._paused:
            raise KafkaSourceError("Kafka Source 只有在 pause 后才能提交 Checkpoint")
        assignment = getattr(consumer, "assignment", None)
        if not callable(assignment):
            raise KafkaSourceError("Kafka consumer 不支持 assignment")
        mapping: dict[object, object] = {}
        try:
            from aiokafka.structs import OffsetAndMetadata
        except ImportError:  # pragma: no cover - 生产依赖包含 aiokafka
            OffsetAndMetadata = None  # type: ignore[assignment,misc]
        for partition in self._checkpoint_partitions:
            key = (partition.topic, partition.partition)
            offset = self._next_offsets.get(key)
            if offset is None:
                continue
            mapping[partition] = (
                OffsetAndMetadata(offset, "") if OffsetAndMetadata is not None else offset
            )
        try:
            await consumer.commit(mapping)
        except Exception as exc:
            raise KafkaSourceError(f"提交 Checkpoint Kafka offset 失败: {exc}") from exc

    async def close(self) -> None:
        """停止 consumer；重复关闭保持安全。"""
        consumer = self._consumer
        self._consumer = None
        if consumer is not None:
            try:
                await consumer.stop()
            except Exception as exc:
                self._state = OperatorState.CLOSED
                raise KafkaSourceError(f"关闭 Kafka Source 失败: {exc}") from exc
        self._state = OperatorState.CLOSED

    def _require_open(self) -> AsyncKafkaConsumer:
        if self._state is not OperatorState.OPEN or self._consumer is None:
            raise KafkaSourceError(f"Kafka Source 必须处于 open 状态, 当前为 {self._state}")
        return self._consumer

    def _decode(self, message: KafkaMessage) -> RecordEnvelope | None:
        location = f"topic={message.topic} partition={message.partition} offset={message.offset}"
        try:
            value = message.value
            if isinstance(value, str):
                text = value
            elif isinstance(value, (bytes, bytearray, memoryview)):
                text = bytes(value).decode("utf-8")
            else:
                raise TypeError("value 必须是 UTF-8 bytes 或字符串")
            payload = json.loads(text, parse_constant=_reject_json_constant)
            if self._payload_validator is not None:
                try:
                    self._payload_validator(cast(JsonValue, payload))
                except Exception as exc:
                    raise ValueError(f"payload validator 拒绝记录: {exc}") from exc
            event_time = None
            extractor = self.config.event_time
            if extractor is not None:
                raw_event_time = resolve_json_pointer(
                    cast(JsonValue, payload),
                    extractor.pointer,
                )
                if not isinstance(raw_event_time, str):
                    raise ValueError("event_time JSON Pointer 必须指向 RFC3339 字符串")
                event_time = _parse_rfc3339(raw_event_time)
            return RecordEnvelope(
                record_id=f"{message.topic}:{message.partition}:{message.offset}",
                payload=cast(JsonValue, payload),
                processing_time=self.context.clock.now(),
                event_time=event_time,
                headers={
                    "source_topic": message.topic,
                    "source_partition": message.partition,
                    "source_offset": message.offset,
                },
            )
        except (
            JsonPointerError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
        ) as exc:
            self._bad_records += 1
            error = BadRecordError(f"Kafka 坏记录 {location}: {exc}")
            if self.config.bad_record_policy == "fail":
                raise error from exc
            log_event(
                self._logger,
                logging.WARNING,
                "bad_record_skipped",
                f"跳过 Kafka 坏记录 {location}",
                component="kafka_source",
                job_id=self.job_id,
                operator_id=self.context.operator_id,
                subtask=self.context.subtask_index,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
                error=f"{type(exc).__name__}: {exc}",
            )
            return None

    def _refresh_assignment(
        self,
        consumer: AsyncKafkaConsumer,
    ) -> set[tuple[str, int]]:
        assignment = getattr(consumer, "assignment", None)
        if not callable(assignment):
            return set()
        now = self.context.clock.monotonic()
        assigned: dict[tuple[str, int], object] = {}
        for partition in assignment():
            topic = getattr(partition, "topic", None)
            index = getattr(partition, "partition", None)
            if isinstance(topic, str) and isinstance(index, int) and not isinstance(index, bool):
                assigned[(topic, index)] = partition
                self._partition_event_times.setdefault(
                    (topic, index),
                    _PartitionEventTimeState(last_activity=now),
                )
        checkpoint_keys = {
            (partition.topic, partition.partition) for partition in self._checkpoint_partitions
        }
        for key in tuple(self._partition_event_times):
            if key not in assigned and key not in checkpoint_keys:
                del self._partition_event_times[key]
                self._next_offsets.pop(key, None)

        restored: set[tuple[str, int]] = set()
        seek = getattr(consumer, "seek", None)
        for key, partition in assigned.items():
            if key not in self._pending_restore or key in self._restore_applied:
                continue
            next_offset, max_event_time = self._pending_restore[key]
            if next_offset is not None:
                if not callable(seek):
                    raise KafkaSourceError("Kafka consumer 不支持 restore seek")
                seek(partition, next_offset)
                self._next_offsets[key] = next_offset
                restored.add(key)
            else:
                self._next_offsets.pop(key, None)
            self._partition_event_times[key] = _PartitionEventTimeState(
                last_activity=now,
                max_event_time=max_event_time,
            )
            self._restore_applied.add(key)
        return restored

    def _observe_event_time(
        self,
        message: KafkaMessage,
        record: RecordEnvelope,
    ) -> None:
        if record.event_time is None:  # pragma: no cover - 构造契约保证
            raise KafkaSourceError("事件时间 Source 产生了 null event_time")
        key = (message.topic, message.partition)
        state = self._partition_event_times.setdefault(
            key,
            _PartitionEventTimeState(last_activity=self.context.clock.monotonic()),
        )
        state.last_activity = self.context.clock.monotonic()
        if state.max_event_time is None or record.event_time > state.max_event_time:
            state.max_event_time = record.event_time

    def _record_consumed(self, message: KafkaMessage) -> bool:
        key = (message.topic, message.partition)
        next_offset = message.offset + 1
        previous = self._next_offsets.get(key)
        if previous is not None and next_offset <= previous:
            self._records_replayed += 1
            log_event(
                self._logger,
                logging.WARNING,
                "source_replay_skipped",
                "Kafka Source 跳过当前 Runtime 已处理的重放消息",
                component="kafka_source",
                job_id=self.job_id,
                operator_id=self.context.operator_id,
                subtask=self.context.subtask_index,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
                next_offset=previous,
            )
            return False
        self._next_offsets[key] = next_offset
        return True

    def _next_watermark(self) -> RecordEnvelope | None:
        strategy = self._event_time_strategy
        if strategy is None or not self._partition_event_times:
            return None
        now = self.context.clock.monotonic()
        active = [
            state
            for state in self._partition_event_times.values()
            if now - state.last_activity < strategy.idle_timeout_seconds
        ]
        if not active or any(state.max_event_time is None for state in active):
            return None
        delay = timedelta(milliseconds=strategy.max_out_of_orderness_milliseconds)
        candidate = min(cast(datetime, state.max_event_time) - delay for state in active)
        if self._last_watermark is not None and candidate <= self._last_watermark:
            return None
        self._last_watermark = candidate
        self._watermarks_emitted += 1
        log_event(
            self._logger,
            logging.INFO,
            "watermark_emitted",
            "Kafka Source 已生成事件时间 Watermark",
            component="kafka_source",
            job_id=self.job_id,
            operator_id=self.context.operator_id,
            subtask=self.context.subtask_index,
            watermark=candidate.isoformat(),
            active_partitions=len(active),
        )
        return RecordEnvelope(
            record_id=(
                f"watermark:{self.context.operator_id}:"
                f"{self.context.subtask_index}:{self._watermarks_emitted}"
            ),
            payload={},
            processing_time=self.context.clock.now(),
            event_time=candidate,
            message_type=MessageType.WATERMARK,
            headers={"active_partitions": len(active)},
        )


def _parse_rfc3339(value: str) -> datetime:
    if _RFC3339.fullmatch(value) is None:
        raise ValueError("event_time 必须是带时区的 RFC3339 时间")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("event_time 不是合法 RFC3339 时间") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("event_time 必须包含时区")
    return parsed.astimezone(UTC)


def _parse_snapshot_datetime(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise KafkaSourceError(f"Kafka Source snapshot {field_name} 必须是字符串")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise KafkaSourceError(
            f"Kafka Source snapshot {field_name} 不是合法 ISO-8601 时间"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise KafkaSourceError(f"Kafka Source snapshot {field_name} 必须包含时区")
    return parsed.astimezone(UTC)


class FileSinkOperator(BaseOperator):
    """将窗口结果追加到当前 subtask 独占的 CSV 分片。"""

    def __init__(
        self,
        context: OperatorContext,
        *,
        job_id: str,
        config: FileSinkConfig,
        file_opener: FileOpener = _open_text_append,
    ) -> None:
        super().__init__(context)
        self.job_id = _require_safe_segment(job_id, field="job_id")
        _require_safe_segment(context.operator_id, field="operator_id")
        self.config = config
        self._file_opener = file_opener
        self._file: TextIO | None = None
        self._writer: Any = None
        self._records_written = 0
        root = Path(config.output_path).resolve()
        parent = (root / self.job_id / context.operator_id).resolve()
        if not parent.is_relative_to(root):
            raise ValueError("文件 Sink 目标路径不能逃逸 output_path")
        self._output_path = parent / f"part-{context.subtask_index:05d}.csv"

    @property
    def output_path(self) -> Path:
        """返回当前 subtask 的确定性输出文件路径。"""
        return self._output_path

    @property
    def metrics(self) -> dict[str, int]:
        """返回已成功刷新到文件的记录数。"""
        return {"records_written": self._records_written}

    def open(self) -> None:
        """创建隔离目录并打开当前 subtask 的追加文件。"""
        if self.state is not OperatorState.CREATED:
            super().open()
            return
        try:
            self._output_path.parent.mkdir(parents=True, exist_ok=True)
            file_handle = self._file_opener(self._output_path)
            writer = csv.writer(file_handle, lineterminator="\n")
        except (OSError, csv.Error) as exc:
            raise FileSinkError(f"打开文件 Sink {self._output_path} 失败: {exc}") from exc
        self._file = file_handle
        self._writer = writer
        super().open()

    def process(self, record: RecordT) -> list[RecordT]:
        """按配置列写 CSV；缺省保持 ``window_end,word,count``。"""
        self._require_open()
        if self._writer is None or self._file is None:  # pragma: no cover - 生命周期保证
            raise FileSinkError("文件 Sink 资源尚未初始化")
        if self.config.columns is None:
            payload = record.payload
            if not isinstance(payload, dict):
                raise RecordValidationError("文件 Sink payload 必须是 JSON object")
            window_end = record.headers.get("window_end")
            word = payload.get("word")
            count = payload.get("count")
            if not isinstance(window_end, str) or not window_end:
                raise RecordValidationError("文件 Sink 记录缺少非空 headers.window_end")
            if not isinstance(word, str) or not word:
                raise RecordValidationError("文件 Sink 记录缺少非空 payload.word")
            if isinstance(count, bool) or not isinstance(count, int):
                raise RecordValidationError("文件 Sink payload.count 必须是整数")
            row: tuple[object, ...] = (window_end, word, count)
        else:
            if not isinstance(record, RecordEnvelope):
                raise RecordValidationError("配置 columns 的文件 Sink 只接受 RecordEnvelope")
            document = cast(JsonValue, record.to_dict())
            try:
                row = tuple(
                    _csv_cell(resolve_json_pointer(document, pointer))
                    for pointer in self.config.columns
                )
            except JsonPointerError as exc:
                raise RecordValidationError(f"文件 Sink columns 解析失败: {exc}") from exc
        try:
            self._writer.writerow(row)
            self._file.flush()
        except (OSError, csv.Error) as exc:
            raise FileSinkError(f"写入文件 Sink {self._output_path} 失败: {exc}") from exc
        self._records_written += 1
        return []

    def close(self) -> None:
        """关闭输出文件；关闭错误向 TaskRuntime 传播。"""
        file_handle = self._file
        self._file = None
        self._writer = None
        error: OSError | None = None
        if file_handle is not None:
            try:
                file_handle.close()
            except OSError as exc:
                error = exc
        super().close()
        if error is not None:
            raise FileSinkError(f"关闭文件 Sink {self._output_path} 失败: {error}") from error

    def begin_transaction(self) -> None:
        """预留第三阶段事务边界；第一阶段明确不支持。"""
        raise UnsupportedStateOperation("第一阶段文件 Sink 不支持事务 begin")

    def pre_commit(self) -> None:
        """预留第三阶段事务边界；第一阶段明确不支持。"""
        raise UnsupportedStateOperation("第一阶段文件 Sink 不支持事务 pre-commit")

    def commit_transaction(self) -> None:
        """预留第三阶段事务边界；第一阶段明确不支持。"""
        raise UnsupportedStateOperation("第一阶段文件 Sink 不支持事务 commit")

    def abort_transaction(self) -> None:
        """预留第三阶段事务边界；第一阶段明确不支持。"""
        raise UnsupportedStateOperation("第一阶段文件 Sink 不支持事务 abort")


def _csv_cell(value: JsonValue) -> object:
    """把 JSON 值稳定转换为单个 CSV 单元格。"""
    if isinstance(value, (dict, list, bool)) or value is None:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    return value


__all__ = [
    "AsyncKafkaConsumer",
    "FileSinkOperator",
    "KafkaConsumerFactory",
    "KafkaJsonSource",
    "KafkaMessage",
    "PayloadValidator",
]
