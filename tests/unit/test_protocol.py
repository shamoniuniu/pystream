"""长度前缀数据帧、握手与异步读写测试。"""

from __future__ import annotations

import asyncio
import json
import struct
from datetime import UTC, datetime

import pytest

from pystream.common import ChangeKind, MessageType, RecordEnvelope
from pystream.runtime.protocol import (
    ChannelIdentity,
    ConnectionClosedError,
    Frame,
    FrameTooLargeError,
    FrameType,
    HandshakeError,
    IncrementalFrameDecoder,
    MalformedFrameError,
    VersionMismatchError,
    control_frame,
    data_batch_frame,
    encode_frame,
    end_of_stream_frame,
    error_frame,
    heartbeat_frame,
    hello_frame,
    read_frame,
    record_from_control,
    records_from_data_batch,
    validate_hello,
    write_frame,
)


class MemoryWriter:
    """记录 write/drain/close 调用的最小异步 writer。"""

    def __init__(self) -> None:
        self.buffer = bytearray()
        self.drain_calls = 0
        self.closed = False
        self.waited = False

    def write(self, data: bytes) -> None:
        self.buffer.extend(data)

    async def drain(self) -> None:
        self.drain_calls += 1

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        self.waited = True


def record(index: int = 0, *, message_type: MessageType = MessageType.DATA) -> RecordEnvelope:
    """创建稳定记录。"""
    return RecordEnvelope(
        record_id=f"words:0:{index}",
        payload={"word": "apple", "count": index + 1},
        key="apple",
        processing_time=datetime(2026, 7, 26, 12, 0, tzinfo=UTC),
        event_time=(
            datetime(2026, 7, 26, 11, 59, tzinfo=UTC)
            if message_type is MessageType.WATERMARK
            else None
        ),
        message_type=message_type,
        change_kind=ChangeKind.INSERT,
    )


def test_增量解码支持逐字节半包():
    expected = heartbeat_frame(8)
    encoded = encode_frame(expected)
    decoder = IncrementalFrameDecoder()
    actual = []

    for byte in encoded:
        actual.extend(decoder.feed_data(bytes([byte])))

    assert actual == [expected]
    assert decoder.buffered_bytes == 0


def test_增量解码一次恢复多个粘包并保留尾部半包():
    first = heartbeat_frame(1)
    second = error_frame("BAD_RECORD", "invalid json")
    third = end_of_stream_frame()
    first_two = encode_frame(first) + encode_frame(second)
    third_bytes = encode_frame(third)
    decoder = IncrementalFrameDecoder()

    frames = decoder.feed_data(first_two + third_bytes[:6])

    assert frames == [first, second]
    assert decoder.buffered_bytes == 6
    assert decoder.feed_data(third_bytes[6:]) == [third]


def test_非法长度与超限帧在分配_body_前被拒绝():
    decoder = IncrementalFrameDecoder(max_frame_size=16)

    with pytest.raises(MalformedFrameError, match="不能为 0"):
        decoder.feed_data(struct.pack(">I", 0))

    with pytest.raises(FrameTooLargeError, match="超过上限"):
        decoder.feed_data(struct.pack(">I", 17))

    with pytest.raises(FrameTooLargeError, match="超过上限"):
        encode_frame(error_frame("X", "long message"), max_frame_size=8)


@pytest.mark.parametrize(
    "body",
    [
        b"not-json",
        json.dumps([]).encode(),
        json.dumps({"version": 1, "type": "HEARTBEAT"}).encode(),
        json.dumps(
            {
                "version": 1,
                "type": "UNKNOWN",
                "payload": {},
            }
        ).encode(),
    ],
)
def test_非法_json_结构和未知帧类型被拒绝(body):
    encoded = struct.pack(">I", len(body)) + body

    with pytest.raises(MalformedFrameError):
        IncrementalFrameDecoder().feed_data(encoded)


def test_协议版本不兼容被拒绝():
    body = json.dumps(
        {
            "version": 1,
            "type": "HEARTBEAT",
            "payload": {"sequence": 1},
        }
    ).encode()

    with pytest.raises(VersionMismatchError, match="收到 1"):
        IncrementalFrameDecoder().feed_data(struct.pack(">I", len(body)) + body)


def test_hello_校验完整身份和首帧类型():
    expected = ChannelIdentity("job-1", "map-0", "reduce-1")

    assert validate_hello(hello_frame(expected), expected) == expected

    with pytest.raises(HandshakeError, match="身份不匹配"):
        validate_hello(
            hello_frame(ChannelIdentity("job-1", "map-1", "reduce-1")),
            expected,
        )
    with pytest.raises(HandshakeError, match="首帧必须"):
        validate_hello(heartbeat_frame(0), expected)


def test_data_batch和control分别往返且禁止混装():
    data = record(0)
    watermark = record(1, message_type=MessageType.WATERMARK)

    assert records_from_data_batch(data_batch_frame([data])) == (data,)
    assert record_from_control(control_frame(watermark)) == watermark
    with pytest.raises(MalformedFrameError, match="只能包含 DATA"):
        data_batch_frame([data, watermark])
    with pytest.raises(MalformedFrameError, match="不能包含 DATA"):
        control_frame(data)


def test_data_batch_拒绝空批次_超限和非法记录():
    with pytest.raises(MalformedFrameError, match="至少包含"):
        data_batch_frame([])
    with pytest.raises(MalformedFrameError, match="超过上限"):
        data_batch_frame([record(0), record(1)], max_batch_records=1)
    with pytest.raises(MalformedFrameError, match="只能包含 records"):
        records_from_data_batch(Frame(FrameType.DATA_BATCH, {"records": [], "other": 1}))

    invalid = record().to_dict()
    invalid["processing_time"] = "bad-time"
    with pytest.raises(MalformedFrameError, match="非法记录"):
        records_from_data_batch(Frame(FrameType.DATA_BATCH, {"records": [invalid]}))


def test_所有基础帧类型可构造():
    identity = ChannelIdentity("job", "upstream-0", "downstream-0")

    assert hello_frame(identity).frame_type is FrameType.HELLO
    assert data_batch_frame([record()]).frame_type is FrameType.DATA_BATCH
    assert end_of_stream_frame().frame_type is FrameType.END_OF_STREAM
    assert error_frame("FAIL", "failed").frame_type is FrameType.ERROR
    assert heartbeat_frame(1).frame_type is FrameType.HEARTBEAT


def test_直接构造帧和_error_也执行类型校验():
    with pytest.raises(MalformedFrameError, match="FrameType"):
        Frame("HEARTBEAT", {})
    with pytest.raises(MalformedFrameError, match="非空字符串"):
        error_frame(1, "failed")


@pytest.mark.asyncio
async def test_async_write_调用_drain_且_read_frame_可往返():
    writer = MemoryWriter()
    expected = heartbeat_frame(5)

    await write_frame(writer, expected)
    reader = asyncio.StreamReader()
    reader.feed_data(writer.buffer)
    reader.feed_eof()

    assert writer.drain_calls == 1
    assert await read_frame(reader) == expected


@pytest.mark.asyncio
async def test_async_read_区分前缀和_body_中途断连():
    reader = asyncio.StreamReader()
    reader.feed_data(b"\x00\x00")
    reader.feed_eof()
    with pytest.raises(ConnectionClosedError, match="帧长度"):
        await read_frame(reader)

    reader = asyncio.StreamReader()
    reader.feed_data(struct.pack(">I", 10) + b"short")
    reader.feed_eof()
    with pytest.raises(ConnectionClosedError, match="期望 10, 实际 5"):
        await read_frame(reader)
