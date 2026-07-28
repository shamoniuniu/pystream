"""有界数据通道的顺序、背压、批量和清理测试。"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from pystream.common import RecordEnvelope
from pystream.runtime import (
    BoundedDataChannel,
    ChannelError,
    ChannelIdentity,
    ChannelState,
    FrameType,
    IncrementalFrameDecoder,
    records_from_data_batch,
)


class ControlledWriter:
    """可阻塞或失败指定 drain 调用的内存 writer。"""

    def __init__(self, *, block_from_call: int | None = None, fail_on_call: int | None = None):
        self.buffer = bytearray()
        self.block_from_call = block_from_call
        self.fail_on_call = fail_on_call
        self.drain_calls = 0
        self.blocked = asyncio.Event()
        self.release = asyncio.Event()
        self.closed = False
        self.waited = False

    def write(self, data: bytes) -> None:
        self.buffer.extend(data)

    async def drain(self) -> None:
        self.drain_calls += 1
        if self.fail_on_call == self.drain_calls:
            raise ConnectionError("simulated downstream failure")
        if (
            self.block_from_call is not None
            and self.drain_calls >= self.block_from_call
            and not self.release.is_set()
        ):
            self.blocked.set()
            await self.release.wait()

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        self.waited = True


def record(index: int) -> RecordEnvelope:
    """创建顺序可观测记录。"""
    return RecordEnvelope(
        record_id=f"topic:0:{index}",
        payload={"index": index},
        key=f"key-{index % 2}",
        processing_time=datetime(2026, 7, 26, 12, 0, tzinfo=UTC),
    )


def decoded_frames(writer: ControlledWriter):
    """解码 writer 中的全部完成帧。"""
    decoder = IncrementalFrameDecoder()
    frames = decoder.feed_data(writer.buffer)
    assert decoder.buffered_bytes == 0
    return frames


@pytest.mark.asyncio
async def test_channel_发送_hello_批次_eos_并保持顺序():
    writer = ControlledWriter()
    channel = BoundedDataChannel(
        writer,
        ChannelIdentity("job-1", "map-0", "reduce-0"),
        queue_capacity=8,
        batch_size=3,
    )

    await channel.start()
    await channel.send_many([record(index) for index in range(7)])
    await channel.close()

    frames = decoded_frames(writer)
    data_frames = [frame for frame in frames if frame.frame_type is FrameType.DATA_BATCH]
    restored = [item.record_id for frame in data_frames for item in records_from_data_batch(frame)]
    assert frames[0].frame_type is FrameType.HELLO
    assert frames[-1].frame_type is FrameType.END_OF_STREAM
    assert restored == [f"topic:0:{index}" for index in range(7)]
    assert channel.records_sent == 7
    assert channel.batches_sent == len(data_frames)
    assert channel.state is ChannelState.CLOSED
    assert writer.closed and writer.waited


@pytest.mark.asyncio
async def test_慢下游使有界队列产生可观察背压():
    writer = ControlledWriter(block_from_call=2)
    channel = BoundedDataChannel(
        writer,
        ChannelIdentity("job-1", "map-0", "reduce-0"),
        queue_capacity=1,
        batch_size=1,
    )
    await channel.start()

    await channel.send(record(0))
    await asyncio.wait_for(writer.blocked.wait(), timeout=1)
    await channel.send(record(1))
    blocked_send = asyncio.create_task(channel.send(record(2)))
    await asyncio.sleep(0.02)

    assert not blocked_send.done()
    assert channel.queue_size == channel.queue_capacity == 1

    writer.release.set()
    await asyncio.wait_for(blocked_send, timeout=1)
    await channel.close()

    frames = decoded_frames(writer)
    restored = [
        item.record_id
        for frame in frames
        if frame.frame_type is FrameType.DATA_BATCH
        for item in records_from_data_batch(frame)
    ]
    assert restored == ["topic:0:0", "topic:0:1", "topic:0:2"]
    assert channel.max_queue_depth <= channel.queue_capacity


@pytest.mark.asyncio
async def test_发送失败会传播并关闭_writer():
    writer = ControlledWriter(fail_on_call=2)
    channel = BoundedDataChannel(
        writer,
        ChannelIdentity("job-1", "map-0", "reduce-0"),
        queue_capacity=1,
        batch_size=1,
    )
    await channel.start()
    await channel.send(record(0))

    with pytest.raises(ChannelError, match="数据通道已失败"):
        await channel.wait_closed()
    with pytest.raises(ChannelError, match="失败"):
        await channel.send(record(1))

    await channel.abort()
    assert writer.closed and writer.waited
    assert channel.state is ChannelState.CLOSED


@pytest.mark.asyncio
async def test_abort_不发送_eos_且可重复调用():
    writer = ControlledWriter()
    channel = BoundedDataChannel(
        writer,
        ChannelIdentity("job-1", "map-0", "reduce-0"),
    )
    await channel.start()

    await channel.abort()
    await channel.abort()

    assert [frame.frame_type for frame in decoded_frames(writer)] == [FrameType.HELLO]
    assert channel.state is ChannelState.CLOSED


@pytest.mark.asyncio
async def test_未启动通道_close_只清理资源():
    writer = ControlledWriter()
    channel = BoundedDataChannel(
        writer,
        ChannelIdentity("job-1", "map-0", "reduce-0"),
    )

    await channel.close()

    assert channel.state is ChannelState.CLOSED
    assert writer.closed and writer.waited
    assert writer.buffer == b""
