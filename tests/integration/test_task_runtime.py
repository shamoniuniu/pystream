"""TaskRuntime 多端口流水线、HASH Shuffle 和失败传播测试。"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pystream.api import FileSinkConfig, OperatorType, Partitioning
from pystream.checkpoint import (
    LocalCheckpointStore,
    TaskSnapshotDescriptor,
    decode_state,
    encode_state,
)
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
    RuntimeLifecycleError,
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


class CheckpointMemorySource(MemorySource):
    """记录停流 Checkpoint 生命周期的内存 Source。"""

    def __init__(self, records: list[RecordEnvelope]) -> None:
        super().__init__(records)
        self.paused = False
        self.resumes = 0
        self.checkpoint_commits = 0
        self.frozen: dict[int, bytes] = {}
        self.aborted: list[int] = []

    async def pause(self) -> None:
        self.paused = True

    async def resume(self) -> None:
        self.paused = False
        self.resumes += 1

    def snapshot_state(self) -> bytes:
        assert self.paused
        return encode_state("memory-source", {"records": len(self._records)})

    def snapshot_checkpoint(self, checkpoint_id: int) -> bytes:
        assert self.paused
        snapshot = self.snapshot_state()
        self.frozen[checkpoint_id] = snapshot
        return snapshot

    async def commit_checkpoint(self, checkpoint_id: int | None = None) -> None:
        if checkpoint_id is None:
            assert self.paused
        else:
            assert not self.paused
            assert self.frozen.pop(checkpoint_id) is not None
        self.checkpoint_commits += 1

    def abort_checkpoint(self, checkpoint_id: int) -> None:
        self.frozen.pop(checkpoint_id, None)
        self.aborted.append(checkpoint_id)


class RestoringMemorySource(CheckpointMemorySource):
    """记录 TaskRuntime 在启动执行循环前传入的合并 Source 状态。"""

    def __init__(self) -> None:
        super().__init__([])
        self.restored_snapshot: bytes | None = None

    async def restore_state(self, snapshot: bytes) -> None:
        self.restored_snapshot = snapshot


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
    restore: tuple[TaskSnapshotDescriptor, ...] = (),
    coordinator_epoch: int = 0,
) -> TaskDeployment:
    return TaskDeployment(
        task_instance,
        ARTIFACT,
        incoming,
        outgoing,
        restore,
        coordinator_epoch=coordinator_epoch,
    )


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


def checkpoint_drain(checkpoint_id: int, source_task_id: str) -> RecordEnvelope:
    return RecordEnvelope(
        record_id=f"checkpoint-drain:{source_task_id}:{checkpoint_id}",
        payload={},
        processing_time=AT_WINDOW_START,
        message_type=MessageType.CHECKPOINT_DRAIN,
        checkpoint_id=checkpoint_id,
    )


def checkpoint_barrier(checkpoint_id: int, source_task_id: str) -> RecordEnvelope:
    return RecordEnvelope(
        record_id=f"checkpoint-barrier:{source_task_id}:{checkpoint_id}",
        payload={},
        processing_time=AT_WINDOW_START,
        message_type=MessageType.BARRIER,
        checkpoint_id=checkpoint_id,
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
async def test_checkpoint按data_drain顺序完成全链路快照和source提交(
    tmp_path: Path,
) -> None:
    source_server = DataPlaneServer("127.0.0.1", 0)
    target_server = DataPlaneServer("127.0.0.1", 0)
    await source_server.start()
    await target_server.start()
    source_task = task("words", OperatorType.SOURCE)
    target_task = task("normalize", OperatorType.MAP)
    source_to_target = channel(
        source_task,
        target_task,
        Partitioning.FORWARD,
        target_server,
    )
    store = LocalCheckpointStore(tmp_path / "checkpoints")
    source = CheckpointMemorySource([record("words:0:0", "APPLE")])
    target_runtime = TaskRuntime(
        deployment(target_task, incoming=(source_to_target,)),
        target_server,
        operator=MapOperator(
            OperatorContext("normalize"),
            lambda payload: {
                "word": payload["word"].lower(),
                "count": payload["count"],
            },
        ),
        checkpoint_enabled=True,
        checkpoint_store=store,
    )
    source_runtime = TaskRuntime(
        deployment(source_task, outgoing=(source_to_target,)),
        source_server,
        source=source,
        checkpoint_enabled=True,
        checkpoint_store=store,
    )
    try:
        await target_runtime.start()
        await source_runtime.start()
        await wait_until(lambda: target_runtime.snapshot.records_in == 1)

        await target_runtime.arm_checkpoint(1)
        await source_runtime.arm_checkpoint(1)
        source_descriptor = await source_runtime.trigger_checkpoint(1)
        target_descriptor = await asyncio.wait_for(
            target_runtime.wait_checkpoint(1),
            timeout=2,
        )

        source_state = store.read_task_snapshot(source_descriptor)
        target_state = store.read_task_snapshot(target_descriptor)
        assert source_state["kind"] == "source"
        assert target_state["kind"] == "operator"
        assert target_state["input_watermarks"] == [
            {"upstream_task_id": source_task.task_id, "watermark": None}
        ]
        manifest = store.complete_checkpoint(
            job_id="job-1",
            checkpoint_id=1,
            attempt_id=0,
            expected_task_ids={source_task.task_id, target_task.task_id},
            snapshots=(source_descriptor, target_descriptor),
        )
        assert {item.task_id for item in manifest.snapshots} == {
            source_task.task_id,
            target_task.task_id,
        }

        await target_runtime.complete_checkpoint(1)
        await source_runtime.complete_checkpoint(1)
        assert source.checkpoint_commits == 1
        assert source.resumes == 1
        assert not source.paused

        source.release.set()
        await asyncio.gather(source_runtime.wait(), target_runtime.wait())
    finally:
        await source_runtime.stop()
        await target_runtime.stop()
        await source_server.close()
        await target_server.close()


@pytest.mark.asyncio
async def test_aligned_barrier_source注入后立即恢复并完成全链路快照(
    tmp_path: Path,
) -> None:
    source_server = DataPlaneServer("127.0.0.1", 0)
    target_server = DataPlaneServer("127.0.0.1", 0)
    await source_server.start()
    await target_server.start()
    source_task = task("words", OperatorType.SOURCE)
    target_task = task("normalize", OperatorType.MAP)
    source_to_target = channel(
        source_task,
        target_task,
        Partitioning.FORWARD,
        target_server,
    )
    store = LocalCheckpointStore(tmp_path / "checkpoints")
    source = CheckpointMemorySource([record("words:0:0", "APPLE")])
    target_runtime = TaskRuntime(
        deployment(target_task, incoming=(source_to_target,)),
        target_server,
        operator=MapOperator(OperatorContext("normalize"), lambda payload: payload),
        checkpoint_enabled=True,
        aligned_checkpoints=True,
        checkpoint_store=store,
    )
    source_runtime = TaskRuntime(
        deployment(source_task, outgoing=(source_to_target,)),
        source_server,
        source=source,
        checkpoint_enabled=True,
        aligned_checkpoints=True,
        checkpoint_store=store,
    )
    try:
        await target_runtime.start()
        await source_runtime.start()
        await wait_until(lambda: target_runtime.snapshot.records_in == 1)

        await target_runtime.arm_checkpoint(1)
        await source_runtime.arm_checkpoint(1)
        source_descriptor = await source_runtime.trigger_checkpoint(1)
        target_descriptor = await asyncio.wait_for(
            target_runtime.wait_checkpoint(1),
            timeout=2,
        )

        assert source.resumes == 1
        assert not source.paused
        assert set(source.frozen) == {1}
        assert source_runtime.snapshot.source_pause_duration_ms >= 0
        assert target_runtime.snapshot.barrier_blocked_inputs == 1
        assert target_server.metrics["barrier_gate_waits"] == 1
        assert target_server.metrics["barrier_gated_connections"] == 0

        store.complete_checkpoint(
            job_id="job-1",
            checkpoint_id=1,
            attempt_id=0,
            expected_task_ids={source_task.task_id, target_task.task_id},
            snapshots=(source_descriptor, target_descriptor),
        )
        await target_runtime.complete_checkpoint(1)
        await source_runtime.complete_checkpoint(1)

        assert source.checkpoint_commits == 1
        assert source.frozen == {}
        assert source.resumes == 1

        source.release.set()
        await asyncio.gather(source_runtime.wait(), target_runtime.wait())
    finally:
        await source_runtime.stop()
        await target_runtime.stop()
        await source_server.close()
        await target_server.close()


@pytest.mark.asyncio
async def test_aligned_barrier_逐输入gate且未对齐通道继续处理(
    tmp_path: Path,
) -> None:
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    left = task("left", OperatorType.MAP)
    right = task("right", OperatorType.MAP)
    target = task("target", OperatorType.MAP)
    incoming = (
        channel(left, target, Partitioning.REBALANCE, server),
        channel(right, target, Partitioning.REBALANCE, server),
    )
    seen: list[str] = []
    runtime = TaskRuntime(
        deployment(target, incoming=incoming),
        server,
        operator=MapOperator(
            OperatorContext("target"),
            lambda payload: seen.append(payload["word"]) or payload,
        ),
        checkpoint_enabled=True,
        aligned_checkpoints=True,
        checkpoint_store=LocalCheckpointStore(tmp_path / "checkpoints"),
    )
    outputs: list[BoundedDataChannel] = []
    try:
        await runtime.start()
        await runtime.arm_checkpoint(1)
        for upstream in (left, right):
            _, writer = await asyncio.open_connection("127.0.0.1", server.bound_port)
            output = BoundedDataChannel(
                writer,
                ChannelIdentity("job-1", upstream.task_id, target.task_id),
                queue_capacity=8,
                batch_size=1,
            )
            await output.start()
            outputs.append(output)

        await outputs[0].send(record("left-pre", "left-pre"))
        await outputs[0].send(checkpoint_barrier(1, left.task_id))
        await outputs[0].send(record("left-post", "left-post"))
        await wait_until(
            lambda: seen == ["left-pre"] and server.metrics["barrier_gated_connections"] == 1
        )

        await outputs[1].send(record("right-pre", "right-pre"))
        await wait_until(lambda: seen == ["left-pre", "right-pre"])
        assert "left-post" not in seen

        await outputs[1].send(checkpoint_barrier(1, right.task_id))
        descriptor = await asyncio.wait_for(runtime.wait_checkpoint(1), timeout=2)
        await wait_until(lambda: seen == ["left-pre", "right-pre", "left-post"])

        assert descriptor.checkpoint_id == 1
        assert runtime.snapshot.barrier_blocked_inputs == 2
        assert server.metrics["max_barrier_gated_connections"] == 2
        assert server.metrics["barrier_gated_connections"] == 0

        await runtime.complete_checkpoint(1)
        for output in outputs:
            await output.close()
        await runtime.wait()
    finally:
        for output in outputs:
            await output.abort()
        await runtime.stop()
        await server.close()


@pytest.mark.asyncio
async def test_aligned_barrier_abort解除gate并拒绝错序编号(tmp_path: Path) -> None:
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    left = task("left", OperatorType.MAP)
    right = task("right", OperatorType.MAP)
    target = task("target", OperatorType.MAP)
    incoming = (
        channel(left, target, Partitioning.REBALANCE, server),
        channel(right, target, Partitioning.REBALANCE, server),
    )
    seen: list[str] = []
    runtime = TaskRuntime(
        deployment(target, incoming=incoming),
        server,
        operator=MapOperator(
            OperatorContext("target"),
            lambda payload: seen.append(payload["word"]) or payload,
        ),
        checkpoint_enabled=True,
        aligned_checkpoints=True,
        checkpoint_store=LocalCheckpointStore(tmp_path / "checkpoints"),
    )
    outputs: list[BoundedDataChannel] = []
    identities = tuple(
        ChannelIdentity("job-1", item.source_task_id, target.task_id) for item in incoming
    )
    try:
        await runtime.start()
        await runtime.arm_checkpoint(2)
        for identity in identities:
            _, writer = await asyncio.open_connection("127.0.0.1", server.bound_port)
            output = BoundedDataChannel(writer, identity, queue_capacity=4, batch_size=1)
            await output.start()
            outputs.append(output)

        await outputs[0].send(checkpoint_barrier(2, left.task_id))
        await outputs[0].send(record("left-after-abort", "left-after-abort"))
        await wait_until(lambda: server.metrics["barrier_gated_connections"] == 1)

        await runtime.abort_checkpoint(2)
        await wait_until(lambda: seen == ["left-after-abort"])
        assert server.metrics["barrier_gated_connections"] == 0
        assert runtime.state is TaskRuntimeState.RUNNING

        await runtime.arm_checkpoint(3)
        with pytest.raises(RuntimeLifecycleError, match="未 arm"):
            await runtime.accept_control(
                identities[0],
                checkpoint_barrier(4, left.task_id),
            )
        await runtime.abort_checkpoint(3)

        await runtime.arm_checkpoint(5)
        await outputs[0].send(checkpoint_barrier(5, left.task_id))
        await wait_until(lambda: server.metrics["barrier_gated_connections"] == 1)
        await runtime.stop()
        await wait_until(lambda: server.metrics["barrier_gated_connections"] == 0)
    finally:
        for output in outputs:
            await output.abort()
        await runtime.stop()
        await server.close()


@pytest.mark.asyncio
async def test_aligned_barrier_未对齐输入断连使任务失败并释放gate(
    tmp_path: Path,
) -> None:
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    left = task("left", OperatorType.MAP)
    right = task("right", OperatorType.MAP)
    target = task("target", OperatorType.MAP)
    incoming = (
        channel(left, target, Partitioning.REBALANCE, server),
        channel(right, target, Partitioning.REBALANCE, server),
    )
    runtime = TaskRuntime(
        deployment(target, incoming=incoming),
        server,
        operator=MapOperator(OperatorContext("target"), lambda payload: payload),
        checkpoint_enabled=True,
        aligned_checkpoints=True,
        checkpoint_store=LocalCheckpointStore(tmp_path / "checkpoints"),
    )
    outputs: list[BoundedDataChannel] = []
    try:
        await runtime.start()
        await runtime.arm_checkpoint(1)
        for upstream in (left, right):
            _, writer = await asyncio.open_connection("127.0.0.1", server.bound_port)
            output = BoundedDataChannel(
                writer,
                ChannelIdentity("job-1", upstream.task_id, target.task_id),
            )
            await output.start()
            outputs.append(output)
        await wait_until(lambda: server.active_connection_count == 2)

        await outputs[0].send(checkpoint_barrier(1, left.task_id))
        await wait_until(lambda: server.metrics["barrier_gated_connections"] == 1)
        await outputs[1].abort()

        await wait_until(lambda: runtime.state is TaskRuntimeState.FAILED)
        await wait_until(lambda: server.metrics["barrier_gated_connections"] == 0)
        assert runtime.snapshot.error is not None
        assert "入通道" in runtime.snapshot.error
    finally:
        for output in outputs:
            await output.abort()
        await runtime.stop()
        await server.close()


@pytest.mark.asyncio
async def test_aligned_barrier_重复输入使任务失败(tmp_path: Path) -> None:
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    upstream = task("upstream", OperatorType.MAP)
    target = task("target", OperatorType.MAP)
    incoming = channel(upstream, target, Partitioning.FORWARD, server)
    runtime = TaskRuntime(
        deployment(target, incoming=(incoming,)),
        server,
        operator=MapOperator(OperatorContext("target"), lambda payload: payload),
        checkpoint_enabled=True,
        aligned_checkpoints=True,
        checkpoint_store=LocalCheckpointStore(tmp_path / "checkpoints"),
    )
    output: BoundedDataChannel | None = None
    try:
        await runtime.start()
        await runtime.arm_checkpoint(1)
        _, writer = await asyncio.open_connection("127.0.0.1", server.bound_port)
        output = BoundedDataChannel(
            writer,
            ChannelIdentity("job-1", upstream.task_id, target.task_id),
        )
        await output.start()
        await output.send(checkpoint_barrier(1, upstream.task_id))
        await asyncio.wait_for(runtime.wait_checkpoint(1), timeout=2)

        await output.send(checkpoint_barrier(1, upstream.task_id))
        await wait_until(lambda: runtime.state is TaskRuntimeState.FAILED)

        assert runtime.snapshot.error is not None
        assert "重复 BARRIER" in runtime.snapshot.error
    finally:
        if output is not None:
            await output.abort()
        await runtime.stop()
        await server.close()


@pytest.mark.asyncio
async def test_checkpoint多输入收齐与abort后的延迟drain隔离(tmp_path: Path) -> None:
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    left = task("left", OperatorType.MAP)
    right = task("right", OperatorType.MAP)
    target = task("target", OperatorType.MAP)
    incoming = (
        channel(left, target, Partitioning.REBALANCE, server),
        channel(right, target, Partitioning.REBALANCE, server),
    )
    runtime = TaskRuntime(
        deployment(target, incoming=incoming),
        server,
        operator=MapOperator(OperatorContext("target"), lambda payload: payload),
        checkpoint_enabled=True,
        checkpoint_store=LocalCheckpointStore(tmp_path / "checkpoints"),
    )
    identities = tuple(
        ChannelIdentity("job-1", item.source_task_id, target.task_id) for item in incoming
    )
    try:
        await runtime.start()
        await runtime.arm_checkpoint(1)
        await runtime.accept_control(
            identities[0],
            checkpoint_drain(1, left.task_id),
        )
        await wait_until(lambda: identities[0] in runtime._checkpoint_drains)
        assert not runtime._checkpoint_ready.is_set()

        await runtime.abort_checkpoint(1)
        await runtime.accept_control(
            identities[1],
            checkpoint_drain(1, right.task_id),
        )
        await runtime._input_queue.join()
        assert runtime.state is TaskRuntimeState.RUNNING

        await runtime.arm_checkpoint(2)
        await runtime.accept_control(
            identities[0],
            checkpoint_drain(2, left.task_id),
        )
        await runtime.accept_control(
            identities[1],
            checkpoint_drain(2, right.task_id),
        )
        descriptor = await asyncio.wait_for(runtime.wait_checkpoint(2), timeout=2)
        assert descriptor.checkpoint_id == 2
        await runtime.complete_checkpoint(2)
        assert runtime.state is TaskRuntimeState.RUNNING
    finally:
        await runtime.stop()
        await server.close()


@pytest.mark.asyncio
async def test_source_restore合并operator分区并保留当前task_watermark(
    tmp_path: Path,
) -> None:
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    store = LocalCheckpointStore(tmp_path / "checkpoints")
    source_task = task("words", OperatorType.SOURCE, parallelism=2)
    source_task.attempt_id = 1
    source_task.restored_checkpoint_id = 5

    def source_descriptor(
        task_id: str,
        partition: int,
        watermark: datetime,
    ) -> TaskSnapshotDescriptor:
        inner = encode_state(
            "kafka-source",
            {
                "topic": "words",
                "partitions": [
                    {
                        "topic": "words",
                        "partition": partition,
                        "next_offset": partition + 10,
                        "max_event_time": watermark.isoformat(),
                    }
                ],
                "last_watermark": watermark.isoformat(),
            },
        )
        return store.write_task_snapshot(
            job_id="job-1",
            checkpoint_id=5,
            attempt_id=0,
            task_id=task_id,
            operator_id="words",
            state={
                "kind": "source",
                "snapshot": inner.decode("utf-8"),
                "input_watermarks": [],
                "last_output_watermark": None,
            },
        )

    primary = source_descriptor(source_task.task_id, 0, AT_WINDOW_START)
    secondary = source_descriptor("job-1:words:1", 1, AT_WINDOW_END)
    source = RestoringMemorySource()
    runtime = TaskRuntime(
        deployment(source_task, restore=(primary, secondary)),
        server,
        source=source,
        checkpoint_enabled=True,
        checkpoint_store=store,
    )
    try:
        await runtime.start()
        assert source.restored_snapshot is not None
        restored = decode_state(source.restored_snapshot, "kafka-source")
        assert [item["partition"] for item in restored["partitions"]] == [0, 1]
        assert restored["last_watermark"] == AT_WINDOW_START.isoformat()
    finally:
        await runtime.stop()
        await server.close()


@pytest.mark.asyncio
async def test_operator_restore在执行前恢复每输入和输出watermark(tmp_path: Path) -> None:
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    store = LocalCheckpointStore(tmp_path / "checkpoints")
    upstream = task("upstream", OperatorType.MAP)
    target = task("target", OperatorType.MAP)
    target.attempt_id = 1
    target.restored_checkpoint_id = 6
    incoming = channel(upstream, target, Partitioning.FORWARD, server)
    descriptor = store.write_task_snapshot(
        job_id="job-1",
        checkpoint_id=6,
        attempt_id=0,
        task_id=target.task_id,
        operator_id=target.operator_id,
        state={
            "kind": "operator",
            "snapshot": encode_state("stateless-operator", {}).decode("utf-8"),
            "input_watermarks": [
                {
                    "upstream_task_id": upstream.task_id,
                    "watermark": AT_WINDOW_START.isoformat(),
                }
            ],
            "last_output_watermark": AT_WINDOW_START.isoformat(),
        },
    )
    runtime = TaskRuntime(
        deployment(target, incoming=(incoming,), restore=(descriptor,)),
        server,
        operator=MapOperator(OperatorContext("target"), lambda payload: payload),
        checkpoint_enabled=True,
        checkpoint_store=store,
    )
    identity = ChannelIdentity(
        "job-1",
        upstream.task_id,
        target.task_id,
        attempt_id=1,
    )
    try:
        await runtime.start()
        assert runtime._input_watermarks[identity] == AT_WINDOW_START
        assert runtime._last_output_watermark == AT_WINDOW_START
        assert runtime._idle_inputs == set()
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
        expected = (
            "at_least_once Runtime 不接受 BARRIER"
            if message_type is MessageType.BARRIER
            else f"尚未处理控制消息 {message_type.value}"
        )
        assert expected in runtime.snapshot.error
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
async def test_data_plane_拒绝旧coordinator_epoch连接() -> None:
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    upstream = task("upstream", OperatorType.MAP)
    target = task("target", OperatorType.MAP)
    incoming = channel(upstream, target, Partitioning.FORWARD, server)
    runtime = TaskRuntime(
        deployment(target, incoming=(incoming,), coordinator_epoch=4),
        server,
        operator=MapOperator(OperatorContext("target"), lambda payload: payload),
    )
    await runtime.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", server.bound_port)
    del reader
    await write_frame(
        writer,
        hello_frame(
            ChannelIdentity(
                "job-1",
                upstream.task_id,
                target.task_id,
                coordinator_epoch=3,
            )
        ),
    )
    try:
        await wait_until(lambda: server.metrics["connection_errors"] == 1)
        assert runtime.state is TaskRuntimeState.RUNNING
        assert server.active_connection_count == 0
    finally:
        writer.close()
        await writer.wait_closed()
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
