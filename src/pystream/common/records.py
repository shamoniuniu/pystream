"""跨节点传输的记录信封与预留控制字段。

记录信封是数据面和算子共享的稳定契约。第一阶段只发送 ``DATA`` 和
``INSERT``，其余枚举值为后续 Watermark、Checkpoint 和 Retract 保留。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import TypeAlias

JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


class RecordValidationError(ValueError):
    """记录信封字段不符合公共数据契约。"""


class MessageType(StrEnum):
    """记录或控制消息类型。"""

    DATA = "DATA"
    WATERMARK = "WATERMARK"
    CHECKPOINT_DRAIN = "CHECKPOINT_DRAIN"
    BARRIER = "BARRIER"
    CHECKPOINT_COMPLETE = "CHECKPOINT_COMPLETE"


class ChangeKind(StrEnum):
    """Changelog 变更类型；第一阶段固定使用 INSERT。"""

    INSERT = "INSERT"
    UPDATE_BEFORE = "UPDATE_BEFORE"
    UPDATE_AFTER = "UPDATE_AFTER"
    DELETE = "DELETE"


def utc_now() -> datetime:
    """返回带 UTC 时区的当前时间，便于 Source 注入和测试替换。"""
    return datetime.now(UTC)


def _ensure_utc(value: datetime | None, field_name: str) -> None:
    if value is None:
        return
    if value.tzinfo is None or value.utcoffset() is None:
        raise RecordValidationError(f"{field_name} 必须是带时区的 UTC 时间")
    if value.utcoffset() != UTC.utcoffset(value):
        raise RecordValidationError(f"{field_name} 必须使用 UTC 时区")


def _ensure_json(value: object, field_name: str) -> None:
    try:
        json.dumps(value, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RecordValidationError(f"{field_name} 必须可 JSON 序列化: {exc}") from exc


def _parse_datetime(value: object, field_name: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RecordValidationError(f"{field_name} 必须是 ISO-8601 字符串或 null")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RecordValidationError(f"{field_name} 不是合法 ISO-8601 时间") from exc
    _ensure_utc(parsed, field_name)
    return parsed


@dataclass(frozen=True, slots=True)
class RecordEnvelope:
    """算子间传输的一条业务记录及其系统元数据。"""

    record_id: str
    payload: JsonValue
    processing_time: datetime = field(default_factory=utc_now)
    key: JsonValue = None
    event_time: datetime | None = None
    message_type: MessageType = MessageType.DATA
    change_kind: ChangeKind = ChangeKind.INSERT
    checkpoint_id: int | None = None
    headers: dict[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.record_id, str) or not self.record_id:
            raise RecordValidationError("record_id 必须是非空字符串")
        if not isinstance(self.message_type, MessageType):
            raise RecordValidationError("message_type 必须是 MessageType")
        if not isinstance(self.change_kind, ChangeKind):
            raise RecordValidationError("change_kind 必须是 ChangeKind")
        if self.checkpoint_id is not None and (
            isinstance(self.checkpoint_id, bool)
            or not isinstance(self.checkpoint_id, int)
            or self.checkpoint_id < 0
        ):
            raise RecordValidationError("checkpoint_id 必须是非负整数或 null")
        if not isinstance(self.processing_time, datetime):
            raise RecordValidationError("processing_time 必须是带时区的 UTC 时间")
        _ensure_utc(self.processing_time, "processing_time")
        _ensure_utc(self.event_time, "event_time")
        _ensure_json(self.payload, "payload")
        _ensure_json(self.key, "key")
        if not isinstance(self.headers, dict) or not all(
            isinstance(key, str) for key in self.headers
        ):
            raise RecordValidationError("headers 必须是字符串键的 JSON object")
        _ensure_json(self.headers, "headers")

    def with_key(self, key: JsonValue) -> RecordEnvelope:
        """返回设置了分区 key 的新信封，不修改原记录。"""
        return replace(self, key=key)

    def with_payload(self, payload: JsonValue) -> RecordEnvelope:
        """返回替换业务 payload 的新信封，同时保留系统元数据。"""
        return replace(self, payload=payload)

    def to_dict(self) -> dict[str, JsonValue]:
        """转换为可稳定编码为 JSON 的字典。"""
        return {
            "message_type": self.message_type.value,
            "record_id": self.record_id,
            "payload": self.payload,
            "key": self.key,
            "processing_time": self.processing_time.isoformat(),
            "event_time": self.event_time.isoformat() if self.event_time is not None else None,
            "change_kind": self.change_kind.value,
            "checkpoint_id": self.checkpoint_id,
            "headers": self.headers,
        }

    @classmethod
    def from_dict(cls, document: object) -> RecordEnvelope:
        """从不可信 JSON 对象恢复并严格校验记录信封。"""
        if not isinstance(document, dict):
            raise RecordValidationError("记录信封必须是 JSON object")
        expected = {
            "message_type",
            "record_id",
            "payload",
            "key",
            "processing_time",
            "event_time",
            "change_kind",
            "checkpoint_id",
            "headers",
        }
        missing = expected - document.keys()
        extra = document.keys() - expected
        if missing:
            raise RecordValidationError("记录信封缺少字段: " + ", ".join(sorted(missing)))
        if extra:
            raise RecordValidationError("记录信封包含未知字段: " + ", ".join(sorted(extra)))
        try:
            message_type = MessageType(document["message_type"])
        except (TypeError, ValueError) as exc:
            raise RecordValidationError("message_type 不受支持") from exc
        try:
            change_kind = ChangeKind(document["change_kind"])
        except (TypeError, ValueError) as exc:
            raise RecordValidationError("change_kind 不受支持") from exc
        checkpoint_id = document["checkpoint_id"]
        if checkpoint_id is not None and (
            isinstance(checkpoint_id, bool) or not isinstance(checkpoint_id, int)
        ):
            raise RecordValidationError("checkpoint_id 必须是非负整数或 null")
        headers = document["headers"]
        if not isinstance(headers, dict) or not all(isinstance(key, str) for key in headers):
            raise RecordValidationError("headers 必须是字符串键的 JSON object")
        record_id = document["record_id"]
        if not isinstance(record_id, str):
            raise RecordValidationError("record_id 必须是非空字符串")
        processing_time = _parse_datetime(document["processing_time"], "processing_time")
        if processing_time is None:
            raise RecordValidationError("processing_time 不能为 null")
        return cls(
            message_type=message_type,
            record_id=record_id,
            payload=document["payload"],
            key=document["key"],
            processing_time=processing_time,
            event_time=_parse_datetime(document["event_time"], "event_time"),
            change_kind=change_kind,
            checkpoint_id=checkpoint_id,
            headers=headers,
        )


__all__ = [
    "ChangeKind",
    "JsonScalar",
    "JsonValue",
    "MessageType",
    "RecordEnvelope",
    "RecordValidationError",
    "utc_now",
]
