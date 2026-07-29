"""TaskRuntime 多端口流水线、HASH Shuffle 和失败传播测试。"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pystream.api import FileSinkConfig, OperatorType, Partitioning
from pystream.common import MessageType, RecordEnvelope
from pystream.control import (
    ArtifactDescriptor,
    PhysicalChannel,
    TaskDeployment,
    TaskEndpoint,
    TaskInstance,
    TaskStatus,
)
from pystream.operators import (
    FileSinkOperator,
    KeyByOperator,
    ManualClock,
    MapOperator,
    OperatorContext,
    ReduceWindowOperator,
)
from pystream.runtime import (
    BoundedDataChannel,
    ChannelIdentity,
    DataPlaneServer,
    TaskRuntime,
    TaskRuntimeState,
    hello_frame,
    write_frame,
)

AT_WINDOW_START = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
AT_WINDOW_END = datetime(2026, 7, 26, 12, 0, 1, tzinfo=UTC)
ARTIFACT = ArtifactDescriptor("job-1", "0" * 64, 0)


class MemorySource:
    """先发送固定记录，再等待测试释放结束信号。"""

    def __init__(self, records: list[RecordEnvelope]) -> None:
        self._records = records
        self.release = asyncio.Event()
        self.opened = False
        self.closed = False
        self.commits = 0

    async def open(self) -> None:
        self.opened = True

    async def records(self):
        for record in self._records:
            yield record
        await self.release.wait()

    async def commit(self) -> None:
        self.commits += 1

    async def close(self) -> None:
        self.closed = True


class CommitFailingSource(MemorySource):
    """在 Runtime 尝试提交已发送记录时抛出连接错误。"""

    async def records(self):
        for record in self._records:
            yield record

    async def commit(self) -> None:
        raise ConnectionError("commit unavailable")


class SlowFlushFile:
    """包装真实临时文件，并在 flush 时模拟慢文件系统。"""

    def __init__(self, path: Path, delay: float) -> None:
        self._file = path.open("a", encoding="utf-8", newline="")
        self._delay = delay

    def write(self, value: str) -> int:
        return self._file.write(value)

    def flush(self) -> None:
        time.sleep(self._delay)
        self._file.flush()

    def close(self) -> None:
        self._file.close()


def task(
    operator_id: str,
    operator_type: OperatorType,
    subtask: int = 0,
    parallelism: int = 1,
) -> TaskInstance:
    return TaskInstance(
        task_id=f"job-1:{operator_id}:{subtask}",
        job_id="job-1",
        operator_id=operator_id,
        operator_type=operator_type,
        subtask_index=subtask,
        parallelism=parallelism,
        status=TaskStatus.DEPLOYING,
        worker_id=f"worker-{operator_id}-{subtask}",
        slot_index=0,
    )


def channel(
    source: TaskInstance,
    target: TaskInstance,
    partitioning: Partitioning,
    target_server: DataPlaneServer,
) -> PhysicalChannel:
    return PhysicalChannel(
        channel_id=f"{source.task_id}->{target.task_id}",
        source_task_id=source.task_id,
        target_task_id=target.task_id,
        partitioning=partitioning,
        target_endpoint=TaskEndpoint(
            task_id=target.task_id,
            host="127.0.0.1",
            port=target_server.bound_port,
        ),
    )


def deployment(
    task_instance: TaskInstance,
    *,
    incoming: tuple[PhysicalChannel, ...] = (),
    outgoing: tuple[PhysicalChannel, ...] = (),
) -> TaskDeployment:
    return TaskDeployment(task_instance, ARTIFACT, incoming, outgoing)


def record(record_id: str, word: str) -> RecordEnvelope:
    return RecordEnvelope(
        record_id=record_id,
        payload={"word": word, "count": 1},
        processing_time=AT_WINDOW_START,
    )


def sink_record(index: int) -> RecordEnvelope:
    return RecordEnvelope(
        record_id=f"words:0:{index}",
        payload={"word": f"word-{index}", "count": 1},
        processing_time=AT_WINDOW_END,
        key=f"word-{index}",
        headers={"window_end": "2026/07/26T12:00:01"},
    )


async def wait_until(predicate, timeout: float = 2.0) -> None:
    async def poll() -> None:
        while not predicate():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


@pytest.mark.asyncio
async def test_loopback_多runtime完成_map_hash_reduce_sink(tmp_path: Path) -> None:
    servers = [DataPlaneServer("127.0.0.1", 0) for _ in range(6)]
    for server in servers:
        await server.start()

    source_task = task("words", OperatorType.SOURCE)
    map_task = task("normalize", OperatorType.MAP)
    key_task = task("by_word", OperatorType.KEY_BY)
    reduce_tasks = [task("totals", OperatorType.REDUCE, index, 2) for index in range(2)]
    sink_task = task("output", OperatorType.SINK)

    source_to_map = channel(source_task, map_task, Partitioning.FORWARD, servers[1])
    map_to_key = channel(map_task, key_task, Partitioning.FORWARD, servers[2])
    key_to_reduce = tuple(
        channel(key_task, reduce_task, Partitioning.HASH, servers[3 + index])
        for index, reduce_task in enumerate(reduce_tasks)
    )
    reduce_to_sink = tuple(
        channel(reduce_task, sink_task, Partitioning.REBALANCE, servers[5])
        for reduce_task in reduce_tasks
    )

    sink = FileSinkOperator(
        OperatorContext("output"),
        job_id="job-1",
        config=FileSinkConfig(
            connector="file",
            format="csv",
            output_path=str(tmp_path / "output"),
        ),
    )
    sink_runtime = TaskRuntime(
        deployment(sink_task, incoming=reduce_to_sink),
        servers[5],
        operator=sink,
        timer_interval=0.02,
    )

    reduce_clocks = [ManualClock(AT_WINDOW_START), ManualClock(AT_WINDOW_START)]
    reduce_runtimes = [
        TaskRuntime(
            deployment(
                reduce_task,
                incoming=(key_to_reduce[index],),
                outgoing=(reduce_to_sink[index],),
            ),
            servers[3 + index],
            operator=ReduceWindowOperator(
                OperatorContext("totals", index, reduce_clocks[index]),
                lambda left, right: {
                    "word": left["word"],
                    "count": left["count"] + right["count"],
                },
                window_size_seconds=1,
            ),
            timer_interval=0.02,
        )
        for index, reduce_task in enumerate(reduce_tasks)
    ]
    key_runtime = TaskRuntime(
        deployment(
            key_task,
            incoming=(map_to_key,),
            outgoing=key_to_reduce,
        ),
        servers[2],
        operator=KeyByOperator(
            OperatorContext("by_word"),
            lambda payload: payload["word"],
        ),
    )
    map_runtime = TaskRuntime(
        deployment(
            map_task,
            incoming=(source_to_map,),
            outgoing=(map_to_key,),
        ),
        servers[1],
        operator=MapOperator(
            OperatorContext("normalize"),
            lambda payload: {
                "word": payload["word"].lower(),
                "count": payload["count"],
            },
        ),
    )
    source = MemorySource(
        [
            record("words:0:0", "APPLE"),
            record("words:0:1", "pie"),
            record("words:0:2", "apple"),
        ]
    )
    source_runtime = TaskRuntime(
        deployment(source_task, outgoing=(source_to_map,)),
        servers[0],
        source=source,
    )
    runtimes = [
        sink_runtime,
        *reduce_runtimes,
        key_runtime,
        map_runtime,
        source_runtime,
    ]
    try:
        for runtime in runtimes:
            await runtime.start()

        await wait_until(lambda: sum(item.snapshot.records_in for item in reduce_runtimes) == 3)
        for clock in reduce_clocks:
            clock.set(AT_WINDOW_END)
        for runtime in reduce_runtimes:
            await runtime.trigger_timers()
        await wait_until(lambda: sink_runtime.snapshot.records_in == 2)

        source.release.set()
        await asyncio.gather(*(runtime.wait() for runtime in runtimes))

        rows = sink.output_path.read_text(encoding="utf-8").splitlines()
        assert set(rows) == {
            "2026/07/26T12:00:01,apple,2",
            "2026/07/26T12:00:01,pie,1",
        }
        assert key_runtime.snapshot.records_out == 3
        assert sum(item.snapshot.records_in for item in reduce_runtimes) == 3
        assert key_runtime.snapshot.output_queue_capacity == 2 * 1_024
        assert key_runtime.snapshot.max_output_queue_depth <= 1_024
        assert sink_runtime.snapshot.operator_metrics == {"records_written": 2}
        assert all(item.state is TaskRuntimeState.STOPPED for item in runtimes)
        assert source.opened and source.closed
        assert source.commits == 3
    finally:
        for runtime in reversed(runtimes):
            await runtime.stop()
        for server in servers:
            await server.close()


@pytest.mark.asyncio
async def test_source_提交_offset_失败使_taskruntime_失败并上报() -> None:
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    source_task = task("words", OperatorType.SOURCE)
    source = CommitFailingSource([record("words:0:0", "apple")])
    failures: list[str] = []

    async def report(_: str, __: str, error: BaseException) -> None:
        failures.append(str(error))

    runtime = TaskRuntime(
        deployment(source_task),
        server,
        source=source,
        failure_callback=report,
    )

    try:
        await runtime.start()
        await wait_until(
            lambda: runtime.state in {TaskRuntimeState.FAILED, TaskRuntimeState.STOPPED}
        )

        assert runtime.state is TaskRuntimeState.FAILED
        assert failures == ["commit unavailable"]
        assert source.closed
    finally:
        await runtime.stop()
        await server.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message_type",
    [
        MessageType.BARRIER,
        MessageType.CHECKPOINT_COMPLETE,
    ],
)
async def test_未启用控制消息经独立帧传输后不会进入业务_udf(
    message_type: MessageType,
) -> None:
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    upstream = task("upstream", OperatorType.MAP)
    target = task("target", OperatorType.MAP)
    incoming = channel(upstream, target, Partitioning.FORWARD, server)
    udf_calls: list[object] = []

    def business_udf(payload):
        udf_calls.append(payload)
        return payload

    runtime = TaskRuntime(
        deployment(target, incoming=(incoming,)),
        server,
        operator=MapOperator(OperatorContext("target"), business_udf),
    )
    await runtime.start()
    _, writer = await asyncio.open_connection("127.0.0.1", server.bound_port)
    output = BoundedDataChannel(
        writer,
        ChannelIdentity("job-1", upstream.task_id, target.task_id),
        queue_capacity=1,
        batch_size=1,
    )
    await output.start()
    control = RecordEnvelope(
        record_id=f"control-{message_type.value}",
        payload={},
        processing_time=AT_WINDOW_START,
        message_type=message_type,
        checkpoint_id=1 if message_type is not MessageType.WATERMARK else None,
    )

    try:
        await output.send(control)
        await wait_until(lambda: bool(udf_calls) or runtime.state is TaskRuntimeState.FAILED)

        assert udf_calls == []
        assert runtime.state is TaskRuntimeState.FAILED
        assert runtime.snapshot.error is not None
        assert f"尚未处理控制消息 {message_type.value}" in runtime.snapshot.error
    finally:
        await output.abort()
        await runtime.stop()
        await server.close()


@pytest.mark.asyncio
async def test_watermark经control帧推进且不进入业务_udf() -> None:
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    upstream = task("upstream", OperatorType.MAP)
    target = task("target", OperatorType.MAP)
    incoming = channel(upstream, target, Partitioning.FORWARD, server)
    udf_calls: list[object] = []
    runtime = TaskRuntime(
        deployment(target, incoming=(incoming,)),
        server,
        operator=MapOperator(
            OperatorContext("target"),
            lambda payload: udf_calls.append(payload) or payload,
        ),
    )
    await runtime.start()
    _, writer = await asyncio.open_connection("127.0.0.1", server.bound_port)
    output = BoundedDataChannel(
        writer,
        ChannelIdentity("job-1", upstream.task_id, target.task_id),
        queue_capacity=1,
        batch_size=1,
    )
    await output.start()
    watermark = RecordEnvelope(
        record_id="watermark-1",
        payload={},
        processing_time=AT_WINDOW_START,
        event_time=AT_WINDOW_START,
        message_type=MessageType.WATERMARK,
    )

    try:
        await output.send(watermark)
        await wait_until(lambda: runtime._last_output_watermark == AT_WINDOW_START)

        assert udf_calls == []
        assert runtime.state is TaskRuntimeState.RUNNING
    finally:
        await output.abort()
        await runtime.stop()
        await server.close()


@pytest.mark.asyncio
async def test_multi_input_watermark取活跃最小值并排除idle输入() -> None:
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    left = task("left", OperatorType.MAP)
    right = task("right", OperatorType.MAP)
    target = task("target", OperatorType.REDUCE)
    incoming = (
        channel(left, target, Partitioning.REBALANCE, server),
        channel(right, target, Partitioning.REBALANCE, server),
    )
    clock = ManualClock(AT_WINDOW_START)
    operator = ReduceWindowOperator(
        OperatorContext("target", clock=clock),
        lambda left_value, right_value: {
            "word": left_value["word"],
            "count": left_value["count"] + right_value["count"],
        },
        window_size_seconds=5,
        time_characteristic="event",
    )
    runtime = TaskRuntime(
        deployment(target, incoming=incoming),
        server,
        operator=operator,
        watermark_idle_timeout=5,
        monotonic_clock=clock.monotonic,
        timer_interval=0.01,
    )
    await runtime.start()
    identities = tuple(
        ChannelIdentity("job-1", item.source_task_id, target.task_id) for item in incoming
    )

    def control(index: int, timestamp: datetime) -> RecordEnvelope:
        return RecordEnvelope(
            record_id=f"watermark-{index}",
            payload={},
            processing_time=AT_WINDOW_START,
            event_time=timestamp,
            message_type=MessageType.WATERMARK,
        )

    try:
        await runtime.accept_control(identities[0], control(1, AT_WINDOW_START))
        await runtime.accept_control(identities[1], control(2, AT_WINDOW_END))
        await wait_until(lambda: runtime._last_output_watermark == AT_WINDOW_START)

        clock.advance(4)
        await runtime.accept_control(
            identities[1],
            control(3, datetime(2026, 7, 26, 12, 0, 6, tzinfo=UTC)),
        )
        await wait_until(
            lambda: (
                runtime._input_watermarks[identities[1]]
                == datetime(2026, 7, 26, 12, 0, 6, tzinfo=UTC)
            )
        )
        clock.advance(2)
        await runtime._refresh_idle_inputs()

        assert identities[0] in runtime._idle_inputs
        assert identities[1] not in runtime._idle_inputs
        assert runtime._last_output_watermark == datetime(2026, 7, 26, 12, 0, 6, tzinfo=UTC)
    finally:
        await runtime.stop()
        await server.close()


@pytest.mark.asyncio
async def test_慢_file_sink_使_taskruntime_输入队列背压且记录不丢失(
    tmp_path: Path,
) -> None:
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    upstream = task("upstream", OperatorType.REDUCE)
    target = task("output", OperatorType.SINK)
    incoming = channel(upstream, target, Partitioning.FORWARD, server)
    flush_delay = 0.02
    records = tuple(sink_record(index) for index in range(8))
    sink = FileSinkOperator(
        OperatorContext("output"),
        job_id="job-1",
        config=FileSinkConfig(
            connector="file",
            format="csv",
            output_path=str(tmp_path / "output"),
        ),
        file_opener=lambda path: SlowFlushFile(path, flush_delay),
    )
    runtime = TaskRuntime(
        deployment(target, incoming=(incoming,)),
        server,
        operator=sink,
        input_queue_capacity=1,
        timer_interval=0.01,
    )
    identity = ChannelIdentity("job-1", upstream.task_id, target.task_id)
    await runtime.start()

    try:
        started = time.perf_counter()
        await runtime.accept_records(identity, records)
        elapsed = time.perf_counter() - started

        assert elapsed >= flush_delay * (len(records) - 2)
        assert runtime.snapshot.input_queue_capacity == 1
        assert runtime.snapshot.input_queue_depth <= runtime.snapshot.input_queue_capacity

        await runtime.input_closed(identity)
        await runtime.wait()

        rows = sink.output_path.read_text(encoding="utf-8").splitlines()
        assert rows == [f"2026/07/26T12:00:01,word-{index},1" for index in range(len(records))]
        assert runtime.snapshot.operator_metrics == {"records_written": len(records)}
    finally:
        await runtime.stop()
        await server.close()


@pytest.mark.asyncio
async def test_异常_eof_使目标任务失败并上报() -> None:
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    upstream = task("upstream", OperatorType.MAP)
    target = task("target", OperatorType.MAP)
    incoming = channel(upstream, target, Partitioning.FORWARD, server)
    failures: list[tuple[str, str, str]] = []

    async def report(job_id: str, task_id: str, error: BaseException) -> None:
        failures.append((job_id, task_id, str(error)))

    runtime = TaskRuntime(
        deployment(target, incoming=(incoming,)),
        server,
        operator=MapOperator(OperatorContext("target"), lambda payload: payload),
        failure_callback=report,
    )
    await runtime.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", server.bound_port)
    del reader
    await write_frame(
        writer,
        hello_frame(ChannelIdentity("job-1", upstream.task_id, target.task_id)),
    )
    writer.close()
    await writer.wait_closed()

    await wait_until(lambda: runtime.state is TaskRuntimeState.FAILED)
    assert failures and failures[0][:2] == ("job-1", target.task_id)
    assert "连接" in failures[0][2] or "关闭" in failures[0][2]
    await runtime.stop()
    await server.close()
