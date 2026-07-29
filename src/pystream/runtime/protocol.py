"""PyStream 数据面的版本化长度前缀协议。

每个帧由四字节大端无符号长度和 UTF-8 JSON body 组成。该模块同时提供同步增量
解码与 ``asyncio`` StreamReader/StreamWriter 读写，便于单元测试和真实 TCP 通道复用。
"""

from __future__ import annotations

import asyncio
import json
import struct
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, cast

from pystream.common import JsonValue, MessageType, RecordEnvelope, RecordValidationError

PROTOCOL_VERSION = 2
DEFAULT_MAX_FRAME_SIZE = 8 * 1024 * 1024
DEFAULT_MAX_BATCH_RECORDS = 1_000
_LENGTH_PREFIX_SIZE = 4


class ProtocolError(ValueError):
    """数据帧违反协议契约。"""


class FrameTooLargeError(ProtocolError):
    """帧长度超过本地安全上限。"""


class MalformedFrameError(ProtocolError):
    """帧 body 不是合法且完整的协议 JSON。"""


class VersionMismatchError(ProtocolError):
    """远端使用了不兼容的协议版本。"""


class HandshakeError(ProtocolError):
    """HELLO 身份与预期连接不一致。"""


class ConnectionClosedError(ProtocolError):
    """连接在完整帧读取完成前关闭。"""


class FrameType(StrEnum):
    """第一阶段数据面支持的帧类型。"""

    HELLO = "HELLO"
    DATA_BATCH = "DATA_BATCH"
    CONTROL = "CONTROL"
    END_OF_STREAM = "END_OF_STREAM"
    ERROR = "ERROR"
    HEARTBEAT = "HEARTBEAT"


class AsyncFrameWriter(Protocol):
    """``asyncio.StreamWriter`` 所需的最小结构接口。"""

    def write(self, data: bytes) -> None: ...

    async def drain(self) -> None: ...

    def close(self) -> None: ...

    async def wait_closed(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ChannelIdentity:
    """HELLO 中声明的一条有向任务通道身份。"""

    job_id: str
    upstream_task_id: str
    downstream_task_id: str
    attempt_id: int = 0

    def __post_init__(self) -> None:
        for field_name in ("job_id", "upstream_task_id", "downstream_task_id"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value:
                raise HandshakeError(f"{field_name} 必须是非空字符串")
        if (
            isinstance(self.attempt_id, bool)
            or not isinstance(self.attempt_id, int)
            or self.attempt_id < 0
        ):
            raise HandshakeError("attempt_id 必须是非负整数")

    def to_payload(self) -> dict[str, JsonValue]:
        """转换为 HELLO payload。"""
        return {
            "job_id": self.job_id,
            "upstream_task_id": self.upstream_task_id,
            "downstream_task_id": self.downstream_task_id,
            "attempt_id": self.attempt_id,
        }


@dataclass(frozen=True, slots=True)
class Frame:
    """一个完成结构校验的协议帧。"""

    frame_type: FrameType
    payload: dict[str, JsonValue]
    version: int = PROTOCOL_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.frame_type, FrameType):
            raise MalformedFrameError("frame_type 必须是 FrameType")
        if isinstance(self.version, bool) or not isinstance(self.version, int):
            raise VersionMismatchError("协议版本必须是整数")
        if self.version != PROTOCOL_VERSION:
            raise VersionMismatchError(
                f"协议版本不兼容: 收到 {self.version}, 期望 {PROTOCOL_VERSION}"
            )
        if not isinstance(self.payload, dict) or not all(
            isinstance(key, str) for key in self.payload
        ):
            raise MalformedFrameError("帧 payload 必须是字符串键的 JSON object")
        try:
            json.dumps(self.payload, ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError, OverflowError) as exc:
            raise MalformedFrameError(f"帧 payload 必须可 JSON 序列化: {exc}") from exc

    def to_dict(self) -> dict[str, JsonValue]:
        """转换为可编码协议对象。"""
        return {
            "version": self.version,
            "type": self.frame_type.value,
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, document: object) -> Frame:
        """从不可信 JSON 对象构造并校验帧。"""
        if not isinstance(document, dict):
            raise MalformedFrameError("帧 body 必须是 JSON object")
        expected = {"version", "type", "payload"}
        missing = expected - document.keys()
        extra = document.keys() - expected
        if missing:
            raise MalformedFrameError("帧缺少字段: " + ", ".join(sorted(missing)))
        if extra:
            raise MalformedFrameError("帧包含未知字段: " + ", ".join(sorted(extra)))
        try:
            frame_type = FrameType(document["type"])
        except (TypeError, ValueError) as exc:
            raise MalformedFrameError(f"不支持的帧类型: {document['type']!r}") from exc
        payload = document["payload"]
        if not isinstance(payload, dict):
            raise MalformedFrameError("帧 payload 必须是 JSON object")
        version = document["version"]
        if isinstance(version, bool) or not isinstance(version, int):
            raise VersionMismatchError("协议版本必须是整数")
        return cls(
            frame_type=frame_type,
            payload=cast(dict[str, JsonValue], payload),
            version=version,
        )


def encode_frame(frame: Frame, *, max_frame_size: int = DEFAULT_MAX_FRAME_SIZE) -> bytes:
    """编码完整长度前缀帧，并在写入前执行大小限制。"""
    if max_frame_size <= 0:
        raise ValueError("max_frame_size 必须大于 0")
    body = json.dumps(
        frame.to_dict(),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(body) > max_frame_size:
        raise FrameTooLargeError(f"帧 body {len(body)} bytes 超过上限 {max_frame_size}")
    return struct.pack(">I", len(body)) + body


def decode_frame_body(body: bytes, *, max_frame_size: int = DEFAULT_MAX_FRAME_SIZE) -> Frame:
    """解码单个不含长度前缀的 UTF-8 JSON body。"""
    if not body:
        raise MalformedFrameError("帧 body 不能为空")
    if len(body) > max_frame_size:
        raise FrameTooLargeError(f"帧 body {len(body)} bytes 超过上限 {max_frame_size}")
    try:
        document = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MalformedFrameError(f"帧 body 不是合法 UTF-8 JSON: {exc}") from exc
    return Frame.from_dict(document)


class IncrementalFrameDecoder:
    """将任意分片字节流增量恢复为完整 Frame。"""

    def __init__(self, *, max_frame_size: int = DEFAULT_MAX_FRAME_SIZE) -> None:
        if max_frame_size <= 0:
            raise ValueError("max_frame_size 必须大于 0")
        self._max_frame_size = max_frame_size
        self._buffer = bytearray()

    @property
    def buffered_bytes(self) -> int:
        """返回尚未组成完整帧的字节数。"""
        return len(self._buffer)

    def feed_data(self, data: bytes | bytearray | memoryview) -> list[Frame]:
        """追加一段字节并返回其中所有完整帧。"""
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("data 必须是 bytes-like 对象")
        self._buffer.extend(data)
        frames: list[Frame] = []
        while len(self._buffer) >= _LENGTH_PREFIX_SIZE:
            body_size = struct.unpack(">I", self._buffer[:_LENGTH_PREFIX_SIZE])[0]
            if body_size == 0:
                self._buffer.clear()
                raise MalformedFrameError("长度前缀不能为 0")
            if body_size > self._max_frame_size:
                self._buffer.clear()
                raise FrameTooLargeError(
                    f"长度前缀 {body_size} bytes 超过上限 {self._max_frame_size}"
                )
            frame_size = _LENGTH_PREFIX_SIZE + body_size
            if len(self._buffer) < frame_size:
                break
            body = bytes(self._buffer[_LENGTH_PREFIX_SIZE:frame_size])
            del self._buffer[:frame_size]
            frames.append(decode_frame_body(body, max_frame_size=self._max_frame_size))
        return frames


async def read_frame(
    reader: asyncio.StreamReader,
    *,
    max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
) -> Frame:
    """从异步字节流读取一个完整帧。"""
    try:
        prefix = await reader.readexactly(_LENGTH_PREFIX_SIZE)
    except asyncio.IncompleteReadError as exc:
        raise ConnectionClosedError("连接在读取帧长度前关闭") from exc
    body_size = struct.unpack(">I", prefix)[0]
    if body_size == 0:
        raise MalformedFrameError("长度前缀不能为 0")
    if body_size > max_frame_size:
        raise FrameTooLargeError(f"长度前缀 {body_size} bytes 超过上限 {max_frame_size}")
    try:
        body = await reader.readexactly(body_size)
    except asyncio.IncompleteReadError as exc:
        raise ConnectionClosedError(
            f"连接在读取帧 body 时关闭: 期望 {body_size}, 实际 {len(exc.partial)}"
        ) from exc
    return decode_frame_body(body, max_frame_size=max_frame_size)


async def write_frame(
    writer: AsyncFrameWriter,
    frame: Frame,
    *,
    max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
) -> None:
    """写入一个帧并等待底层发送缓冲区 drain，传播基础背压。"""
    writer.write(encode_frame(frame, max_frame_size=max_frame_size))
    await writer.drain()


def hello_frame(identity: ChannelIdentity) -> Frame:
    """创建通道握手帧。"""
    return Frame(FrameType.HELLO, identity.to_payload())


def validate_hello(frame: Frame, expected: ChannelIdentity) -> ChannelIdentity:
    """校验首帧类型、字段和三元通道身份。"""
    if frame.frame_type is not FrameType.HELLO:
        raise HandshakeError(f"首帧必须是 HELLO, 实际为 {frame.frame_type.value}")
    required = {"job_id", "upstream_task_id", "downstream_task_id", "attempt_id"}
    missing = required - frame.payload.keys()
    extra = frame.payload.keys() - required
    if missing or extra:
        details = []
        if missing:
            details.append("缺少 " + ", ".join(sorted(missing)))
        if extra:
            details.append("未知 " + ", ".join(sorted(extra)))
        raise HandshakeError("HELLO 字段错误: " + "; ".join(details))
    try:
        actual = ChannelIdentity(
            job_id=cast(str, frame.payload["job_id"]),
            upstream_task_id=cast(str, frame.payload["upstream_task_id"]),
            downstream_task_id=cast(str, frame.payload["downstream_task_id"]),
            attempt_id=cast(int, frame.payload["attempt_id"]),
        )
    except (AttributeError, HandshakeError) as exc:
        raise HandshakeError(f"HELLO 身份字段无效: {exc}") from exc
    if actual != expected:
        raise HandshakeError(f"HELLO 身份不匹配: 收到 {actual}, 期望 {expected}")
    return actual


def data_batch_frame(
    records: list[RecordEnvelope] | tuple[RecordEnvelope, ...],
    *,
    max_batch_records: int = DEFAULT_MAX_BATCH_RECORDS,
) -> Frame:
    """创建有记录数上限的 DATA_BATCH。"""
    if not records:
        raise MalformedFrameError("DATA_BATCH 至少包含一条记录")
    if len(records) > max_batch_records:
        raise MalformedFrameError(f"DATA_BATCH 记录数 {len(records)} 超过上限 {max_batch_records}")
    if any(record.message_type is not MessageType.DATA for record in records):
        raise MalformedFrameError("DATA_BATCH 只能包含 DATA 记录")
    return Frame(
        FrameType.DATA_BATCH,
        {"records": [record.to_dict() for record in records]},
    )


def records_from_data_batch(
    frame: Frame,
    *,
    max_batch_records: int = DEFAULT_MAX_BATCH_RECORDS,
) -> tuple[RecordEnvelope, ...]:
    """从 DATA_BATCH 恢复记录，并保留 DATA 与未来控制消息的类型。"""
    if frame.frame_type is not FrameType.DATA_BATCH:
        raise MalformedFrameError(f"期望 DATA_BATCH, 实际为 {frame.frame_type.value}")
    if set(frame.payload) != {"records"}:
        raise MalformedFrameError("DATA_BATCH payload 只能包含 records")
    records = frame.payload["records"]
    if not isinstance(records, list) or not records:
        raise MalformedFrameError("DATA_BATCH records 必须是非空数组")
    if len(records) > max_batch_records:
        raise MalformedFrameError(f"DATA_BATCH 记录数 {len(records)} 超过上限 {max_batch_records}")
    try:
        return tuple(RecordEnvelope.from_dict(item) for item in records)
    except RecordValidationError as exc:
        raise MalformedFrameError(f"DATA_BATCH 包含非法记录: {exc}") from exc


def control_frame(record: RecordEnvelope) -> Frame:
    """创建单条有序控制消息帧。"""
    if record.message_type is MessageType.DATA:
        raise MalformedFrameError("CONTROL 不能包含 DATA 记录")
    return Frame(FrameType.CONTROL, {"record": record.to_dict()})


def record_from_control(frame: Frame) -> RecordEnvelope:
    """从 CONTROL frame 恢复并校验非 DATA 记录。"""
    if frame.frame_type is not FrameType.CONTROL:
        raise MalformedFrameError(f"期望 CONTROL, 实际为 {frame.frame_type.value}")
    if set(frame.payload) != {"record"}:
        raise MalformedFrameError("CONTROL payload 只能包含 record")
    try:
        record = RecordEnvelope.from_dict(frame.payload["record"])
    except RecordValidationError as exc:
        raise MalformedFrameError(f"CONTROL 包含非法记录: {exc}") from exc
    if record.message_type is MessageType.DATA:
        raise MalformedFrameError("CONTROL 不能包含 DATA 记录")
    return record


def end_of_stream_frame() -> Frame:
    """创建正常输入结束帧。"""
    return Frame(FrameType.END_OF_STREAM, {})


def error_frame(code: str, message: str) -> Frame:
    """创建可诊断的远端错误帧。"""
    if not isinstance(code, str) or not isinstance(message, str) or not code or not message:
        raise MalformedFrameError("ERROR 的 code 和 message 必须是非空字符串")
    return Frame(FrameType.ERROR, {"code": code, "message": message})


def heartbeat_frame(sequence: int) -> Frame:
    """创建单调序号心跳帧。"""
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
        raise MalformedFrameError("HEARTBEAT sequence 必须是非负整数")
    return Frame(FrameType.HEARTBEAT, {"sequence": sequence})


__all__ = [
    "DEFAULT_MAX_BATCH_RECORDS",
    "DEFAULT_MAX_FRAME_SIZE",
    "PROTOCOL_VERSION",
    "AsyncFrameWriter",
    "ChannelIdentity",
    "ConnectionClosedError",
    "Frame",
    "FrameTooLargeError",
    "FrameType",
    "HandshakeError",
    "IncrementalFrameDecoder",
    "MalformedFrameError",
    "ProtocolError",
    "VersionMismatchError",
    "control_frame",
    "data_batch_frame",
    "decode_frame_body",
    "encode_frame",
    "end_of_stream_frame",
    "error_frame",
    "heartbeat_frame",
    "hello_frame",
    "read_frame",
    "record_from_control",
    "records_from_data_batch",
    "validate_hello",
    "write_frame",
]
