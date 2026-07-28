"""处理时间滚动窗口与 keyed Reduce。

窗口按 Unix epoch 对齐并采用左闭右开区间。状态以 ``(window, canonical_key)``
隔离，只在 Clock 到达窗口结束时输出并立即清理，第一阶段不提供快照或恢复。
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from pystream.operators.base import (
    BaseOperator,
    JsonValue,
    RecordT,
    canonical_json,
    clone_record,
    validate_json_value,
)
from pystream.operators.clock import require_utc
from pystream.operators.errors import RecordValidationError

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, order=True)
class TimeWindow:
    """左闭右开的 UTC 时间窗口。"""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        start = require_utc(self.start, field="window.start")
        end = require_utc(self.end, field="window.end")
        if end <= start:
            raise ValueError("窗口结束时间必须晚于开始时间")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)

    def contains(self, timestamp: datetime) -> bool:
        """判断时间是否落入 ``[start, end)``。"""
        normalized = require_utc(timestamp)
        return self.start <= normalized < self.end


class TumblingProcessingTimeWindowAssigner:
    """将处理时间分配到 epoch 对齐的固定大小滚动窗口。"""

    def __init__(self, size: timedelta = timedelta(seconds=300)) -> None:
        size_microseconds = size // timedelta(microseconds=1)
        if size_microseconds <= 0:
            raise ValueError("窗口大小必须大于 0")
        self._size = size
        self._size_microseconds = size_microseconds

    @classmethod
    def from_seconds(cls, seconds: float = 300) -> TumblingProcessingTimeWindowAssigner:
        """从秒数构造窗口分配器。"""
        return cls(timedelta(seconds=seconds))

    @property
    def size(self) -> timedelta:
        """返回窗口大小。"""
        return self._size

    def assign(self, timestamp: datetime) -> TimeWindow:
        """按 epoch 计算记录唯一所属窗口。"""
        normalized = require_utc(timestamp, field="processing_time")
        elapsed_microseconds = (normalized - _EPOCH) // timedelta(microseconds=1)
        start_microseconds = (
            elapsed_microseconds // self._size_microseconds
        ) * self._size_microseconds
        start = _EPOCH + timedelta(microseconds=start_microseconds)
        return TimeWindow(start=start, end=start + self._size)


@dataclass
class _WindowState:
    key: JsonValue
    accumulator: JsonValue
    representative: Any


class ReduceWindowOperator(BaseOperator):
    """按 key 和处理时间窗口维护内存 Reduce 状态。"""

    def __init__(
        self,
        context,
        reduce_function: Callable[[Any, Any], Any],
        *,
        window_size_seconds: float = 300,
    ) -> None:
        super().__init__(context)
        if not callable(reduce_function):
            raise TypeError("reduce_function 必须可调用")
        self._reduce_function = reduce_function
        self._assigner = TumblingProcessingTimeWindowAssigner.from_seconds(window_size_seconds)
        self._windows: dict[tuple[TimeWindow, str], _WindowState] = {}

    @property
    def window_size(self) -> timedelta:
        """返回配置的窗口大小。"""
        return self._assigner.size

    @property
    def state_size(self) -> int:
        """返回活跃 ``key/window`` 状态项数量。"""
        return len(self._windows)

    @property
    def active_window_count(self) -> int:
        """返回至少含一条记录的窗口数量。"""
        return len({window for window, _ in self._windows})

    @property
    def state_metrics(self) -> dict[str, int]:
        """返回可由运行时写入日志或指标系统的状态规模。"""
        return {
            "state_entries": self.state_size,
            "active_windows": self.active_window_count,
        }

    def process(self, record: RecordT) -> list[RecordT]:
        """把记录聚合到唯一的 ``key/window`` 状态中。"""
        self._require_open()
        if record.key is None:
            raise RecordValidationError("Reduce 只接受经过 KeyBy 的记录")
        key_token = canonical_json(record.key)
        window = self._assigner.assign(record.processing_time)
        state_key = (window, key_token)
        current = self._windows.get(state_key)

        if current is None:
            validate_json_value(record.payload, field="Reduce 输入 payload")
            self._windows[state_key] = _WindowState(
                key=copy.deepcopy(record.key),
                accumulator=copy.deepcopy(record.payload),
                representative=record,
            )
            return []

        accumulator = self._reduce_function(
            copy.deepcopy(current.accumulator),
            copy.deepcopy(record.payload),
        )
        validate_json_value(accumulator, field="Reduce UDF 返回值")
        current.accumulator = copy.deepcopy(accumulator)
        return []

    def on_timer(self) -> list[RecordT]:
        """输出 Clock 已结束的非空窗口，并在输出构造后清理其状态。"""
        self._require_open()
        now = require_utc(self.context.clock.now(), field="clock.now")
        due_keys = sorted(
            (state_key for state_key in self._windows if state_key[0].end <= now),
            key=lambda item: (item[0].end, item[1]),
        )
        outputs: list[RecordT] = []
        for state_key in due_keys:
            window, _ = state_key
            state = self._windows[state_key]
            headers = dict(state.representative.headers)
            headers.update(
                {
                    "window_start": _format_window_time(window.start),
                    "window_end": _format_window_time(window.end),
                }
            )
            outputs.append(
                clone_record(
                    state.representative,
                    payload=copy.deepcopy(state.accumulator),
                    key=copy.deepcopy(state.key),
                    processing_time=now,
                    headers=headers,
                )
            )

        for state_key in due_keys:
            del self._windows[state_key]
        return outputs

    def close(self) -> None:
        """释放第一阶段的全部内存状态并关闭算子。"""
        self._windows.clear()
        super().close()


def _format_window_time(value: datetime) -> str:
    """按文件 Sink 契约格式化 UTC 窗口边界。"""
    return require_utc(value).strftime("%Y/%m/%dT%H:%M:%S")


__all__ = [
    "ReduceWindowOperator",
    "TimeWindow",
    "TumblingProcessingTimeWindowAssigner",
]
