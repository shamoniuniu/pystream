"""Kafka JSON Source 与 CSV 文件 Sink 的离线契约测试。"""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from pystream.api import EventTimeExecutionConfig, FileSinkConfig, KafkaSourceConfig
from pystream.checkpoint import decode_state
from pystream.common import ChangeKind, MessageType, RecordEnvelope
from pystream.operators import (
    BadRecordError,
    FileSinkError,
    FileSinkOperator,
    KafkaJsonSource,
    KafkaSourceError,
    ManualClock,
    OperatorContext,
    OperatorState,
    RecordValidationError,
    UnsupportedStateOperation,
)


@dataclass(frozen=True)
class FakeMessage:
    """满足 Source 最小消息协议的测试消息。"""

    value: bytes | bytearray | memoryview | str
    topic: str = "words"
    partition: int = 0
    offset: int = 0


@dataclass(frozen=True)
class FakePartition:
    """模拟 aiokafka TopicPartition。"""

    topic: str
    partition: int


class FakeConsumer:
    """可观察启停、提交和有限消息流的异步 consumer。"""

    def __init__(
        self,
        messages: list[FakeMessage] | None = None,
        *,
        assigned_partitions: set[FakePartition] | None = None,
        start_error: Exception | None = None,
        commit_error: Exception | None = None,
        stop_error: Exception | None = None,
    ) -> None:
        self.messages = messages or []
        self.assigned_partitions = (
            assigned_partitions
            if assigned_partitions is not None
            else {FakePartition(message.topic, message.partition) for message in self.messages}
        )
        self.start_error = start_error
        self.commit_error = commit_error
        self.stop_error = stop_error
        self.started = False
        self.stopped = False
        self.commits = 0
        self.commit_payloads: list[dict[object, object] | None] = []
        self.paused: list[tuple[FakePartition, ...]] = []
        self.resumed: list[tuple[FakePartition, ...]] = []
        self.seek_calls: list[tuple[FakePartition, int]] = []

    async def start(self) -> None:
        if self.start_error is not None:
            raise self.start_error
        self.started = True

    async def stop(self) -> None:
        self.stopped = True
        if self.stop_error is not None:
            raise self.stop_error

    async def commit(self, offsets: dict[object, object] | None = None) -> None:
        self.commits += 1
        self.commit_payloads.append(offsets)
        if self.commit_error is not None:
            raise self.commit_error

    def assignment(self) -> set[FakePartition]:
        """返回消息集中全部分区，便于 Watermark 在首条记录前建模。"""
        return set(self.assigned_partitions)

    def pause(self, *partitions: FakePartition) -> None:
        self.paused.append(partitions)

    def resume(self, *partitions: FakePartition) -> None:
        self.resumed.append(partitions)

    def seek(self, partition: FakePartition, offset: int) -> None:
        self.seek_calls.append((partition, offset))

    def __aiter__(self):
        async def iterate():
            for message in self.messages:
                yield message

        return iterate()


class RecordingConsumerFactory:
    """记录 Kafka client 构造参数并按次返回 fake consumer。"""

    def __init__(self, *consumers: FakeConsumer) -> None:
        self.consumers = list(consumers)
        self.calls: list[tuple[tuple[str, ...], dict[str, Any]]] = []

    def __call__(self, *topics: str, **kwargs: Any) -> FakeConsumer:
        self.calls.append((topics, kwargs))
        return self.consumers.pop(0)


def fixed_context(
    *,
    operator_id: str = "words",
    subtask: int = 0,
) -> OperatorContext:
    """构造固定处理时间的算子上下文。"""
    return OperatorContext(
        operator_id,
        subtask_index=subtask,
        clock=ManualClock(datetime(2026, 7, 26, 12, 0, tzinfo=UTC)),
    )


def source_config(**changes: Any) -> KafkaSourceConfig:
    """构造 Kafka Source 配置。"""
    values: dict[str, Any] = {
        "connector": "kafka",
        "topic": "words",
        "bootstrap_servers": "broker:9092",
    }
    values.update(changes)
    return KafkaSourceConfig(**values)


def validate_wordcount_payload(payload: Any) -> None:
    """模拟作业提供的 WordCount Source payload validator。"""
    if not isinstance(payload, dict):
        raise ValueError("输入必须是 JSON object")
    word = payload.get("word")
    count = payload.get("count")
    if not isinstance(word, str) or not word:
        raise ValueError("word 必须是非空字符串")
    if isinstance(count, bool) or not isinstance(count, int):
        raise ValueError("count 必须是整数")


@pytest.mark.asyncio
async def test_source_subtasks_share_group_disable_auto_commit_and_use_distinct_clients() -> None:
    first = FakeConsumer()
    second = FakeConsumer()
    factory = RecordingConsumerFactory(first, second)
    source0 = KafkaJsonSource(
        fixed_context(subtask=0),
        job_id="job-1",
        config=source_config(),
        consumer_factory=factory,
    )
    source1 = KafkaJsonSource(
        fixed_context(subtask=1),
        job_id="job-1",
        config=source_config(),
        consumer_factory=factory,
    )

    await source0.open()
    await source1.open()

    first_call, second_call = factory.calls
    assert first_call[0] == ("words",)
    assert first_call[1]["bootstrap_servers"] == "broker:9092"
    assert first_call[1]["group_id"] == second_call[1]["group_id"]
    assert first_call[1]["group_id"] == "pystream-job-1-words"
    assert first_call[1]["client_id"] != second_call[1]["client_id"]
    assert first_call[1]["enable_auto_commit"] is False
    assert first_call[1]["auto_offset_reset"] == "earliest"
    assert first.started and second.started

    await source0.commit()
    await source0.close()
    await source1.close()

    assert first.commits == 1
    assert first.stopped and second.stopped
    assert source0.state is OperatorState.CLOSED


@pytest.mark.asyncio
async def test_source_checkpoint_pauses_snapshots_exact_offsets_and_resumes() -> None:
    consumer = FakeConsumer(
        [
            FakeMessage(b'{"word":"apple","count":1}', partition=0, offset=3),
            FakeMessage(b'{"word":"pie","count":1}', partition=1, offset=7),
        ]
    )
    source = KafkaJsonSource(
        fixed_context(),
        job_id="job-1",
        config=source_config(),
        consumer_factory=RecordingConsumerFactory(consumer),
    )
    await source.open()
    records = [record async for record in source.records()]
    assert len(records) == 2

    with pytest.raises(KafkaSourceError, match="pause"):
        source.snapshot_state()
    with pytest.raises(KafkaSourceError, match="pause"):
        await source.commit_checkpoint()

    await source.pause()
    assert set(consumer.paused[-1]) == {
        FakePartition("words", 0),
        FakePartition("words", 1),
    }
    state = decode_state(source.snapshot_state(), "kafka-source")
    assert state == {
        "topic": "words",
        "partitions": [
            {
                "topic": "words",
                "partition": 0,
                "next_offset": 4,
                "max_event_time": None,
            },
            {
                "topic": "words",
                "partition": 1,
                "next_offset": 8,
                "max_event_time": None,
            },
        ],
        "last_watermark": None,
    }

    await source.commit_checkpoint()
    committed = consumer.commit_payloads[-1]
    assert committed is not None
    assert {
        (partition.topic, partition.partition): getattr(value, "offset", value)
        for partition, value in committed.items()
    } == {("words", 0): 4, ("words", 1): 8}

    await source.resume()
    assert set(consumer.resumed[-1]) == {
        FakePartition("words", 0),
        FakePartition("words", 1),
    }
    await source.close()


@pytest.mark.asyncio
async def test_source_restore_seeks_current_assignment_and_preserves_watermark_baseline() -> None:
    config = source_config(event_time={"pointer": "/event_time"})
    strategy = EventTimeExecutionConfig(
        max_out_of_orderness="2s",
        idle_timeout="30s",
    )
    original = KafkaJsonSource(
        fixed_context(),
        job_id="job-1",
        config=config,
        event_time_strategy=strategy,
        consumer_factory=RecordingConsumerFactory(
            FakeConsumer(
                [
                    FakeMessage(
                        b'{"word":"apple","event_time":"2026-07-26T12:00:05Z"}',
                        partition=0,
                        offset=3,
                    ),
                    FakeMessage(
                        b'{"word":"pie","event_time":"2026-07-26T12:00:07Z"}',
                        partition=1,
                        offset=7,
                    ),
                ]
            )
        ),
    )
    await original.open()
    _ = [record async for record in original.records()]
    await original.pause()
    snapshot = original.snapshot_state()
    await original.close()

    restored_consumer = FakeConsumer(
        [
            FakeMessage(
                b'{"word":"older","event_time":"2026-07-26T12:00:02Z"}',
                partition=1,
                offset=8,
            )
        ],
        assigned_partitions={FakePartition("words", 1)},
    )
    restored = KafkaJsonSource(
        fixed_context(),
        job_id="job-1",
        config=config,
        event_time_strategy=strategy,
        consumer_factory=RecordingConsumerFactory(restored_consumer),
    )
    await restored.open()
    await restored.restore_state(snapshot)

    assert restored_consumer.seek_calls == [(FakePartition("words", 1), 8)]
    records = [record async for record in restored.records()]
    assert [
        record.event_time for record in records if record.message_type is MessageType.WATERMARK
    ] == [datetime(2026, 7, 26, 12, 0, 5, tzinfo=UTC)]
    await restored.close()


@pytest.mark.asyncio
async def test_source_uses_explicit_group_id_when_configured() -> None:
    consumer = FakeConsumer()
    factory = RecordingConsumerFactory(consumer)
    source = KafkaJsonSource(
        fixed_context(),
        job_id="job-1",
        config=source_config(group_id="course-demo"),
        consumer_factory=factory,
    )

    await source.open()
    await source.close()

    assert factory.calls[0][1]["group_id"] == "course-demo"


@pytest.mark.asyncio
async def test_source_builds_traceable_envelope_with_utc_processing_time() -> None:
    message = FakeMessage(
        b'{"word":"APPLE","count":1}',
        topic="words",
        partition=2,
        offset=17,
    )
    consumer = FakeConsumer([message])
    source = KafkaJsonSource(
        fixed_context(),
        job_id="job-1",
        config=source_config(),
        consumer_factory=RecordingConsumerFactory(consumer),
    )
    await source.open()

    records = [record async for record in source.records()]

    assert records == [
        RecordEnvelope(
            record_id="words:2:17",
            payload={"word": "APPLE", "count": 1},
            processing_time=datetime(2026, 7, 26, 12, 0, tzinfo=UTC),
            headers={
                "source_topic": "words",
                "source_partition": 2,
                "source_offset": 17,
            },
        )
    ]
    assert source.metrics == {"records_read": 1, "bad_records": 0}
    await source.close()


@pytest.mark.asyncio
async def test_event_time_source_提取时间并按partition生成有限乱序watermark() -> None:
    consumer = FakeConsumer(
        [
            FakeMessage(
                b'{"word":"apple","count":1,"event_time":"2026-07-26T12:00:01Z"}',
                partition=0,
                offset=0,
            ),
            FakeMessage(
                b'{"word":"pie","count":1,"event_time":"2026-07-26T12:00:04+00:00"}',
                partition=1,
                offset=0,
            ),
            FakeMessage(
                b'{"word":"apple","count":1,"event_time":"2026-07-26T12:00:03Z"}',
                partition=0,
                offset=1,
            ),
        ]
    )
    source = KafkaJsonSource(
        fixed_context(),
        job_id="job-1",
        config=source_config(event_time={"pointer": "/event_time", "format": "rfc3339"}),
        event_time_strategy=EventTimeExecutionConfig(
            max_out_of_orderness="2s",
            idle_timeout="30s",
        ),
        consumer_factory=RecordingConsumerFactory(consumer),
    )
    await source.open()

    records = [record async for record in source.records()]
    data = [record for record in records if record.message_type is MessageType.DATA]
    watermarks = [
        record.event_time for record in records if record.message_type is MessageType.WATERMARK
    ]

    assert [record.event_time for record in data] == [
        datetime(2026, 7, 26, 12, 0, 1, tzinfo=UTC),
        datetime(2026, 7, 26, 12, 0, 4, tzinfo=UTC),
        datetime(2026, 7, 26, 12, 0, 3, tzinfo=UTC),
    ]
    assert watermarks == [
        datetime(2026, 7, 26, 11, 59, 59, tzinfo=UTC),
        datetime(2026, 7, 26, 12, 0, 1, tzinfo=UTC),
    ]
    assert source.metrics == {
        "records_read": 3,
        "bad_records": 0,
        "watermarks_emitted": 2,
    }


@pytest.mark.asyncio
async def test_event_time解析错误遵循_skip策略() -> None:
    consumer = FakeConsumer(
        [
            FakeMessage(
                b'{"word":"bad","count":1,"event_time":"2026-07-26 12:00:01"}',
                offset=1,
            ),
            FakeMessage(
                b'{"word":"ok","count":1,"event_time":"2026-07-26T12:00:02Z"}',
                offset=2,
            ),
        ]
    )
    source = KafkaJsonSource(
        fixed_context(),
        job_id="job-1",
        config=source_config(
            bad_record_policy="skip",
            event_time={"pointer": "/event_time"},
        ),
        event_time_strategy=EventTimeExecutionConfig(
            max_out_of_orderness="0s",
            idle_timeout="30s",
        ),
        consumer_factory=RecordingConsumerFactory(consumer),
    )
    await source.open()

    records = [record async for record in source.records()]

    assert [record.record_id for record in records if record.message_type is MessageType.DATA] == [
        "words:0:2"
    ]
    assert source.metrics["bad_records"] == 1


@pytest.mark.asyncio
async def test_skip_policy_logs_coordinates_and_continues(caplog) -> None:
    consumer = FakeConsumer(
        [
            FakeMessage(b"not-json", partition=1, offset=8),
            FakeMessage(b'{"word":"pie","count":1}', partition=1, offset=9),
        ]
    )
    source = KafkaJsonSource(
        fixed_context(),
        job_id="job-1",
        config=source_config(bad_record_policy="skip"),
        consumer_factory=RecordingConsumerFactory(consumer),
    )
    await source.open()

    with caplog.at_level(logging.WARNING):
        records = [record async for record in source.records()]

    assert [record.record_id for record in records] == ["words:1:9"]
    assert source.metrics == {"records_read": 1, "bad_records": 1}
    log_record = next(record for record in caplog.records if record.event == "bad_record_skipped")
    assert (log_record.topic, log_record.partition, log_record.offset) == ("words", 1, 8)
    assert "topic=words partition=1 offset=8" in log_record.getMessage()
    await source.close()


@pytest.mark.asyncio
async def test_skip_policy_跳过业务结构非法_payload_并记录来源(caplog) -> None:
    consumer = FakeConsumer(
        [
            FakeMessage(b"{}", partition=2, offset=10),
            FakeMessage(b'{"word":1,"count":1}', partition=2, offset=11),
            FakeMessage(b'{"word":"pie","count":"1"}', partition=2, offset=12),
            FakeMessage(b'{"word":"pie","count":1}', partition=2, offset=13),
        ]
    )
    source = KafkaJsonSource(
        fixed_context(),
        job_id="job-1",
        config=source_config(bad_record_policy="skip"),
        consumer_factory=RecordingConsumerFactory(consumer),
        payload_validator=validate_wordcount_payload,
    )
    await source.open()

    with caplog.at_level(logging.WARNING):
        records = [record async for record in source.records()]

    assert [record.record_id for record in records] == ["words:2:13"]
    assert source.metrics == {"records_read": 1, "bad_records": 3}
    skipped = [record for record in caplog.records if record.event == "bad_record_skipped"]
    assert [(record.topic, record.partition, record.offset) for record in skipped] == [
        ("words", 2, 10),
        ("words", 2, 11),
        ("words", 2, 12),
    ]
    await source.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        b"{}",
        b'{"word":1,"count":1}',
        b'{"word":"pie","count":"1"}',
    ],
)
async def test_fail_policy_拒绝业务结构非法_payload_并包含来源(payload: bytes) -> None:
    consumer = FakeConsumer([FakeMessage(payload, partition=3, offset=21)])
    source = KafkaJsonSource(
        fixed_context(),
        job_id="job-1",
        config=source_config(bad_record_policy="fail"),
        consumer_factory=RecordingConsumerFactory(consumer),
        payload_validator=validate_wordcount_payload,
    )
    await source.open()

    with pytest.raises(BadRecordError, match=r"topic=words partition=3 offset=21"):
        _ = [record async for record in source.records()]

    assert source.metrics == {"records_read": 0, "bad_records": 1}
    await source.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_value",
    [
        b"not-json",
        b"\xff",
        b'{"number":NaN}',
        object(),
    ],
)
async def test_fail_policy_rejects_bad_values_with_source_coordinates(bad_value: Any) -> None:
    consumer = FakeConsumer([FakeMessage(bad_value, partition=3, offset=21)])
    source = KafkaJsonSource(
        fixed_context(),
        job_id="job-1",
        config=source_config(bad_record_policy="fail"),
        consumer_factory=RecordingConsumerFactory(consumer),
    )
    await source.open()

    with pytest.raises(
        BadRecordError,
        match=r"topic=words partition=3 offset=21",
    ):
        _ = [record async for record in source.records()]

    assert source.metrics == {"records_read": 0, "bad_records": 1}
    await source.close()


@pytest.mark.asyncio
async def test_source_start_commit_and_close_errors_are_propagated() -> None:
    start_source = KafkaJsonSource(
        fixed_context(),
        job_id="job-1",
        config=source_config(),
        consumer_factory=RecordingConsumerFactory(
            FakeConsumer(start_error=ConnectionError("unavailable"))
        ),
    )
    with pytest.raises(KafkaSourceError, match="启动"):
        await start_source.open()
    assert start_source.state is OperatorState.CREATED

    commit_consumer = FakeConsumer(commit_error=ConnectionError("commit failed"))
    commit_source = KafkaJsonSource(
        fixed_context(),
        job_id="job-1",
        config=source_config(),
        consumer_factory=RecordingConsumerFactory(commit_consumer),
    )
    await commit_source.open()
    with pytest.raises(KafkaSourceError, match="提交"):
        await commit_source.commit()
    await commit_source.close()

    stop_consumer = FakeConsumer(stop_error=ConnectionError("stop failed"))
    stop_source = KafkaJsonSource(
        fixed_context(),
        job_id="job-1",
        config=source_config(),
        consumer_factory=RecordingConsumerFactory(stop_consumer),
    )
    await stop_source.open()
    with pytest.raises(KafkaSourceError, match="关闭"):
        await stop_source.close()
    assert stop_source.state is OperatorState.CLOSED


@pytest.mark.asyncio
async def test_source_requires_open_for_read_and_commit() -> None:
    source = KafkaJsonSource(
        fixed_context(),
        job_id="job-1",
        config=source_config(),
        consumer_factory=RecordingConsumerFactory(FakeConsumer()),
    )

    with pytest.raises(KafkaSourceError, match="open"):
        _ = [record async for record in source.records()]
    with pytest.raises(KafkaSourceError, match="open"):
        await source.commit()


def sink_record(
    *,
    word: Any = "apple",
    count: Any = 2,
    window_end: Any = "2026/07/26T12:05:00",
) -> RecordEnvelope:
    """构造 WordCount 窗口输出记录。"""
    return RecordEnvelope(
        record_id="words:0:1",
        payload={"word": word, "count": count},
        processing_time=datetime(2026, 7, 26, 12, 5, tzinfo=UTC),
        key="apple",
        headers={"window_end": window_end},
    )


def sink_config(tmp_path: Path) -> FileSinkConfig:
    """构造临时文件 Sink 配置。"""
    return FileSinkConfig(
        connector="file",
        format="csv",
        output_path=str(tmp_path),
    )


def test_file_sink_writes_unheaded_csv_and_flushes_immediately(tmp_path: Path) -> None:
    sink = FileSinkOperator(
        fixed_context(operator_id="output"),
        job_id="job-1",
        config=sink_config(tmp_path),
    )
    sink.open()

    assert sink.process(sink_record(word="apple,pie", count=3)) == []

    assert sink.output_path == tmp_path / "job-1" / "output" / "part-00000.csv"
    with sink.output_path.open(encoding="utf-8", newline="") as file_handle:
        assert list(csv.reader(file_handle)) == [["2026/07/26T12:05:00", "apple,pie", "3"]]
    assert sink.metrics == {"records_written": 1}
    sink.close()


def test_file_sink_columns_支持通用changelog输出(tmp_path: Path) -> None:
    config = FileSinkConfig(
        connector="file",
        format="csv",
        output_path=str(tmp_path),
        columns=[
            "/headers/window_end",
            "/payload/count",
            "/payload/word_count",
            "/change_kind",
        ],
    )
    sink = FileSinkOperator(
        fixed_context(operator_id="output"),
        job_id="job-1",
        config=config,
    )
    sink.open()
    generic = RecordEnvelope(
        record_id="words:0:1",
        payload={"count": 2, "word_count": 1},
        processing_time=datetime(2026, 7, 26, 12, 5, tzinfo=UTC),
        key=2,
        change_kind=ChangeKind.UPDATE_AFTER,
        headers={"window_end": "2026/07/26T12:05:00"},
    )

    assert sink.process(generic) == []
    sink.close()

    with sink.output_path.open(encoding="utf-8", newline="") as file_handle:
        assert list(csv.reader(file_handle)) == [["2026/07/26T12:05:00", "2", "1", "UPDATE_AFTER"]]


def test_file_sink_columns_路径不存在时失败(tmp_path: Path) -> None:
    sink = FileSinkOperator(
        fixed_context(operator_id="output"),
        job_id="job-1",
        config=FileSinkConfig(
            connector="file",
            output_path=str(tmp_path),
            columns=["/payload/missing"],
        ),
    )
    sink.open()

    with pytest.raises(RecordValidationError, match="columns"):
        sink.process(sink_record())
    sink.close()


def test_parallel_file_sinks_use_distinct_partition_files(tmp_path: Path) -> None:
    sink0 = FileSinkOperator(
        fixed_context(operator_id="output", subtask=0),
        job_id="job-1",
        config=sink_config(tmp_path),
    )
    sink1 = FileSinkOperator(
        fixed_context(operator_id="output", subtask=1),
        job_id="job-1",
        config=sink_config(tmp_path),
    )

    sink0.open()
    sink1.open()
    sink0.process(sink_record(word="apple"))
    sink1.process(sink_record(word="pie"))
    sink0.close()
    sink1.close()

    assert sink0.output_path.name == "part-00000.csv"
    assert sink1.output_path.name == "part-00001.csv"
    assert sink0.output_path.read_text(encoding="utf-8").endswith(",apple,2\n")
    assert sink1.output_path.read_text(encoding="utf-8").endswith(",pie,2\n")


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        (
            RecordEnvelope(
                record_id="1",
                payload="not-an-object",
                processing_time=datetime(2026, 7, 26, tzinfo=UTC),
            ),
            "payload",
        ),
        (sink_record(word="", count=1), "word"),
        (sink_record(count=True), "count"),
        (sink_record(window_end=""), "window_end"),
    ],
)
def test_file_sink_rejects_invalid_wordcount_rows(
    tmp_path: Path,
    record: RecordEnvelope,
    expected: str,
) -> None:
    sink = FileSinkOperator(
        fixed_context(operator_id="output"),
        job_id="job-1",
        config=sink_config(tmp_path),
    )
    sink.open()

    with pytest.raises(RecordValidationError, match=expected):
        sink.process(record)

    assert sink.metrics == {"records_written": 0}
    sink.close()


class FailingFlushFile(io.StringIO):
    """在 flush 时模拟磁盘错误。"""

    def flush(self) -> None:
        raise OSError("disk full")


def test_file_sink_propagates_open_and_flush_failures(tmp_path: Path) -> None:
    def fail_open(_path: Path):
        raise OSError("permission denied")

    open_failure = FileSinkOperator(
        fixed_context(operator_id="output"),
        job_id="job-open",
        config=sink_config(tmp_path),
        file_opener=fail_open,
    )
    with pytest.raises(FileSinkError, match="打开"):
        open_failure.open()

    flush_failure = FileSinkOperator(
        fixed_context(operator_id="output"),
        job_id="job-write",
        config=sink_config(tmp_path),
        file_opener=lambda _path: FailingFlushFile(),
    )
    flush_failure.open()
    with pytest.raises(FileSinkError, match="写入"):
        flush_failure.process(sink_record())
    assert flush_failure.metrics == {"records_written": 0}
    flush_failure.close()


@pytest.mark.parametrize("job_id", ["../escape", "/absolute", ".", "with/slash"])
def test_file_sink_and_source_reject_unsafe_job_id(tmp_path: Path, job_id: str) -> None:
    with pytest.raises(ValueError, match="job_id"):
        FileSinkOperator(
            fixed_context(operator_id="output"),
            job_id=job_id,
            config=sink_config(tmp_path),
        )
    with pytest.raises(ValueError, match="job_id"):
        KafkaJsonSource(
            fixed_context(),
            job_id=job_id,
            config=source_config(),
            consumer_factory=RecordingConsumerFactory(FakeConsumer()),
        )


def test_first_phase_file_sink_transaction_boundaries_are_explicitly_unsupported(
    tmp_path: Path,
) -> None:
    sink = FileSinkOperator(
        fixed_context(operator_id="output"),
        job_id="job-1",
        config=sink_config(tmp_path),
    )

    for method in (
        sink.begin_transaction,
        sink.pre_commit,
        sink.commit_transaction,
        sink.abort_transaction,
    ):
        with pytest.raises(UnsupportedStateOperation, match="第一阶段"):
            method()
