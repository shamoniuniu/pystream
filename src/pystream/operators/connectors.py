"""Kafka JSON Source 与 CSV 文件 Sink。

Source 使用可注入的异步 consumer 工厂，生产环境延迟导入 ``aiokafka``，测试环境
则使用内存 fake。所有 Source subtasks 共享同一消费组但使用不同 client id；
``enable_auto_commit=False`` 明确保持第一阶段的手动 offset 模式。

Sink 复用 :class:`~pystream.operators.base.BaseOperator` 生命周期，将每个 subtask
写入独立 CSV 分片。第一阶段采用普通追加写，不提供恢复或 Exactly-once 语义。
"""

from __future__ import annotations

import csv
import json
import logging
import re
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any, Protocol, TextIO, cast

from pystream.api import FileSinkConfig, KafkaSourceConfig
from pystream.common import JsonValue, RecordEnvelope
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

    async def commit(self) -> None:
        """提交当前已消费 offset。"""

    def __aiter__(self) -> AsyncIterator[KafkaMessage]:
        """持续返回 Kafka 消息。"""


KafkaConsumerFactory = Callable[..., AsyncKafkaConsumer]
FileOpener = Callable[[Path], TextIO]
PayloadValidator = Callable[[JsonValue], object]


def _create_aiokafka_consumer(*topics: str, **kwargs: Any) -> AsyncKafkaConsumer:
    """延迟创建生产 consumer，使 fake 测试不依赖正在运行的 broker。"""
    try:
        from aiokafka import AIOKafkaConsumer
    except ImportError as exc:  # pragma: no cover - 干净运行环境应安装项目依赖
        raise KafkaSourceError("缺少 aiokafka 依赖, 无法启动 Kafka Source") from exc
    return cast(AsyncKafkaConsumer, AIOKafkaConsumer(*topics, **kwargs))


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
        payload_validator: PayloadValidator | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.context = context
        self.job_id = _require_safe_segment(job_id, field="job_id")
        self.config = config
        self._consumer_factory = consumer_factory
        self._payload_validator = payload_validator
        self._logger = logger or logging.getLogger(__name__)
        self._consumer: AsyncKafkaConsumer | None = None
        self._state = OperatorState.CREATED
        self._records_read = 0
        self._bad_records = 0

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
        return {
            "records_read": self._records_read,
            "bad_records": self._bad_records,
        }

    async def open(self) -> None:
        """创建 consumer，并以关闭自动提交的方式加入消费组。"""
        if self._state is not OperatorState.CREATED:
            raise KafkaSourceError(f"无法从 {self._state} 打开 Kafka Source")
        client_id = f"{self.group_id}-{self.context.operator_id}-{self.context.subtask_index}"
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
        except Exception as exc:
            self._consumer = None
            raise KafkaSourceError(
                f"启动 Kafka Source 失败 topic={self.config.topic!r}: {exc}"
            ) from exc
        self._consumer = consumer
        self._state = OperatorState.OPEN

    async def records(self) -> AsyncIterator[RecordEnvelope]:
        """持续读取消息；skip 策略隔离坏记录，fail 策略立即终止任务。"""
        consumer = self._require_open()
        try:
            async for message in consumer:
                record = self._decode(message)
                if record is not None:
                    self._records_read += 1
                    yield record
        except BadRecordError:
            raise
        except Exception as exc:
            raise KafkaSourceError(f"消费 Kafka topic={self.config.topic!r} 失败: {exc}") from exc

    async def commit(self) -> None:
        """显式提交 offset；第一阶段绝不在后台自动提交。"""
        consumer = self._require_open()
        try:
            await consumer.commit()
        except Exception as exc:
            raise KafkaSourceError(f"提交 Kafka offset 失败: {exc}") from exc

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
            return RecordEnvelope(
                record_id=f"{message.topic}:{message.partition}:{message.offset}",
                payload=cast(JsonValue, payload),
                processing_time=self.context.clock.now(),
                headers={
                    "source_topic": message.topic,
                    "source_partition": message.partition,
                    "source_offset": message.offset,
                },
            )
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
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
        """校验并写入 ``window_end,word,count``，每条记录立即 flush。"""
        self._require_open()
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
        if self._writer is None or self._file is None:  # pragma: no cover - 生命周期保证
            raise FileSinkError("文件 Sink 资源尚未初始化")
        try:
            self._writer.writerow((window_end, word, count))
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


__all__ = [
    "AsyncKafkaConsumer",
    "FileSinkOperator",
    "KafkaConsumerFactory",
    "KafkaJsonSource",
    "KafkaMessage",
    "PayloadValidator",
]
