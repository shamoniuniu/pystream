"""统一算子生命周期和记录适配契约。

具体算子同步执行用户 UDF，并由后续 TaskRuntime 负责网络和异步调度。本模块只
依赖记录的结构化字段，因此可兼容 Task 4 后续提供的 dataclass 或 Pydantic 信封。
"""

from __future__ import annotations

import copy
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, is_dataclass, replace
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol, TypeVar, runtime_checkable

from pystream.checkpoint import decode_state, encode_state
from pystream.common import ChangeKind
from pystream.operators.clock import Clock, SystemClock
from pystream.operators.errors import (
    OperatorLifecycleError,
    RecordValidationError,
)

JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]


@runtime_checkable
class RecordLike(Protocol):
    """算子消费和产生的最小记录信封结构。"""

    record_id: str
    payload: JsonValue
    key: JsonValue
    processing_time: datetime
    event_time: datetime | None
    change_kind: ChangeKind
    headers: dict[str, JsonValue]


RecordT = TypeVar("RecordT", bound=RecordLike)


class OperatorState(StrEnum):
    """单 Task 内算子的生命周期状态。"""

    CREATED = "created"
    OPEN = "open"
    CLOSED = "closed"


@dataclass(frozen=True)
class OperatorContext:
    """运行时注入给算子的稳定上下文。"""

    operator_id: str
    subtask_index: int = 0
    clock: Clock = field(default_factory=SystemClock)

    def __post_init__(self) -> None:
        if not self.operator_id:
            raise ValueError("operator_id 不能为空")
        if self.subtask_index < 0:
            raise ValueError("subtask_index 不能为负数")


class OperatorTask(Protocol[RecordT]):
    """TaskRuntime 可统一驱动的算子接口。"""

    @property
    def state(self) -> OperatorState:
        """返回当前生命周期状态。"""

    def open(self) -> None:
        """分配执行资源并进入 OPEN。"""

    def process(self, record: RecordT) -> list[RecordT]:
        """处理一条输入记录并返回零到多条输出。"""

    def on_timer(self) -> list[RecordT]:
        """处理当前 Clock 已到期的定时器。"""

    def on_watermark(self, watermark: datetime) -> list[RecordT]:
        """处理单调推进的事件时间 Watermark。"""

    def close(self) -> None:
        """释放资源并进入 CLOSED。"""

    def snapshot_state(self) -> bytes:
        """返回版本化状态快照。"""

    def restore_state(self, snapshot: bytes) -> None:
        """从版本化快照恢复状态。"""


class BaseOperator(ABC):
    """带严格生命周期检查的算子基类。"""

    def __init__(self, context: OperatorContext) -> None:
        self.context = context
        self._state = OperatorState.CREATED

    @property
    def state(self) -> OperatorState:
        """返回当前生命周期状态。"""
        return self._state

    def open(self) -> None:
        """只允许 CREATED 到 OPEN 的一次转换。"""
        if self._state is not OperatorState.CREATED:
            raise OperatorLifecycleError(f"无法从 {self._state} 打开算子")
        self._state = OperatorState.OPEN

    def close(self) -> None:
        """关闭算子；重复关闭是安全的。"""
        self._state = OperatorState.CLOSED

    def _require_open(self) -> None:
        if self._state is not OperatorState.OPEN:
            raise OperatorLifecycleError(f"算子必须处于 open 状态, 当前为 {self._state}")

    @abstractmethod
    def process(self, record: RecordT) -> list[RecordT]:
        """处理一条记录。"""

    def on_timer(self) -> list[RecordT]:
        """无定时器算子只校验生命周期并返回空输出。"""
        self._require_open()
        return []

    def on_watermark(self, watermark: datetime) -> list[RecordT]:
        """无事件时间状态的算子只校验生命周期。"""
        del watermark
        self._require_open()
        return []

    def snapshot_state(self) -> bytes:
        """返回无状态算子的版本化空快照。"""
        self._require_open()
        return encode_state("stateless-operator", {})

    def restore_state(self, snapshot: bytes) -> None:
        """校验无状态算子的快照。"""
        self._require_open()
        if decode_state(snapshot, "stateless-operator"):
            raise RecordValidationError("无状态算子 snapshot state 必须为空")


def validate_json_value(value: Any, *, field: str) -> None:
    """验证值能被严格 JSON 序列化，拒绝 NaN 和 Infinity。"""
    try:
        json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise RecordValidationError(f"{field} 必须可 JSON 序列化: {exc}") from exc


def canonical_json(value: Any) -> str:
    """返回稳定 JSON 表示，可用于状态 key 和后续 HASH Shuffle。"""
    validate_json_value(value, field="key")
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def clone_record(record: RecordT, **changes: Any) -> RecordT:
    """复制记录并更新字段，兼容 dataclass 与 Pydantic v2 模型。"""
    model_copy = getattr(record, "model_copy", None)
    if callable(model_copy):
        return model_copy(update=changes)
    if is_dataclass(record) and not isinstance(record, type):
        return replace(record, **changes)

    cloned = copy.copy(record)
    try:
        for field, value in changes.items():
            setattr(cloned, field, value)
    except (AttributeError, TypeError) as exc:
        raise RecordValidationError(
            "记录信封必须是 dataclass、Pydantic v2 模型或可复制的可变对象"
        ) from exc
    return cloned


__all__ = [
    "BaseOperator",
    "JsonValue",
    "OperatorContext",
    "OperatorState",
    "OperatorTask",
    "RecordLike",
    "RecordT",
    "canonical_json",
    "clone_record",
    "validate_json_value",
]
