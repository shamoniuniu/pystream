"""处理时间滚动窗口与 keyed Reduce。

窗口按 Unix epoch 对齐并采用左闭右开区间。状态以 ``(window, canonical_key)``
隔离，只在 Clock 到达窗口结束时输出并立即清理，第一阶段不提供快照或恢复。
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pystream.checkpoint import CheckpointError, decode_state, encode_state
from pystream.common import ChangeKind, RecordEnvelope
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
    """按 key 和处理时间或事件时间窗口维护内存 Reduce 状态。"""

    def __init__(
        self,
        context,
        reduce_function: Callable[[Any, Any], Any],
        *,
        window_size_seconds: float = 300,
        time_characteristic: Literal["processing", "event"] = "processing",
        emit_mode: Literal["final", "changelog"] = "final",
        retract_function: Callable[[Any, Any], Any] | None = None,
    ) -> None:
        super().__init__(context)
        if not callable(reduce_function):
            raise TypeError("reduce_function 必须可调用")
        if emit_mode not in {"final", "changelog"}:
            raise ValueError("emit_mode 必须是 final 或 changelog")
        if retract_function is not None and not callable(retract_function):
            raise TypeError("retract_function 必须可调用")
        self._reduce_function = reduce_function
        self._retract_function = retract_function
        self._assigner = TumblingProcessingTimeWindowAssigner.from_seconds(window_size_seconds)
        self._time_characteristic = time_characteristic
        self._emit_mode = emit_mode
        self._windows: dict[tuple[TimeWindow, str], _WindowState] = {}
        self._current_watermark: datetime | None = None
        self._late_records = 0
        self._changelog_records = 0
        self._retractions_applied = 0
        self._retract_state_deletes = 0

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
    def emit_mode(self) -> Literal["final", "changelog"]:
        """返回当前 Reduce 的输出模式。"""
        return self._emit_mode

    @property
    def state_metrics(self) -> dict[str, int]:
        """返回可由运行时写入日志或指标系统的状态规模。"""
        metrics = {
            "state_entries": self.state_size,
            "active_windows": self.active_window_count,
        }
        if self._time_characteristic == "event":
            metrics["late_records"] = self._late_records
        if self._emit_mode == "changelog" or self._retract_function is not None:
            metrics.update(
                {
                    "changelog_records": self._changelog_records,
                    "retractions_applied": self._retractions_applied,
                    "retract_state_deletes": self._retract_state_deletes,
                }
            )
        return metrics

    def process(self, record: RecordT) -> list[RecordT]:
        """把记录聚合到唯一的 ``key/window`` 状态中。"""
        self._require_open()
        if record.key is None:
            raise RecordValidationError("Reduce 只接受经过 KeyBy 的记录")
        key_token = canonical_json(record.key)
        timestamp = record.processing_time
        if self._time_characteristic == "event":
            if record.event_time is None:
                raise RecordValidationError("事件时间窗口要求记录包含 event_time")
            timestamp = record.event_time
            if self._current_watermark is not None and timestamp <= self._current_watermark:
                self._late_records += 1
                return []
        window = self._assigner.assign(timestamp)
        state_key = (window, key_token)
        current = self._windows.get(state_key)
        is_retraction = record.change_kind in {
            ChangeKind.UPDATE_BEFORE,
            ChangeKind.DELETE,
        }

        if current is None:
            if is_retraction:
                raise RecordValidationError("不能撤回不存在的 Reduce 状态")
            validate_json_value(record.payload, field="Reduce 输入 payload")
            state = _WindowState(
                key=copy.deepcopy(record.key),
                accumulator=copy.deepcopy(record.payload),
                representative=record,
            )
            self._windows[state_key] = state
            if self._emit_mode == "changelog":
                self._changelog_records += 1
                return [
                    clone_record(
                        record,
                        payload=copy.deepcopy(state.accumulator),
                        key=copy.deepcopy(state.key),
                        change_kind=ChangeKind.INSERT,
                    )
                ]
            return []

        previous_payload = copy.deepcopy(current.accumulator)
        if is_retraction:
            if self._retract_function is None:
                raise RecordValidationError("Reduce 收到撤回消息但没有 retract_udf")
            accumulator = self._retract_function(
                copy.deepcopy(current.accumulator),
                copy.deepcopy(record.payload),
            )
            self._retractions_applied += 1
        else:
            accumulator = self._reduce_function(
                copy.deepcopy(current.accumulator),
                copy.deepcopy(record.payload),
            )
        validate_json_value(accumulator, field="Reduce UDF 返回值")

        if accumulator is None:
            del self._windows[state_key]
            self._retract_state_deletes += 1
            if self._emit_mode == "changelog":
                self._changelog_records += 1
                return [
                    clone_record(
                        record,
                        payload=previous_payload,
                        key=copy.deepcopy(current.key),
                        change_kind=ChangeKind.DELETE,
                    )
                ]
            return []

        current.accumulator = copy.deepcopy(accumulator)
        current.representative = record
        if self._emit_mode != "changelog":
            return []
        self._changelog_records += 2
        return [
            clone_record(
                record,
                payload=previous_payload,
                key=copy.deepcopy(current.key),
                change_kind=ChangeKind.UPDATE_BEFORE,
            ),
            clone_record(
                record,
                payload=copy.deepcopy(current.accumulator),
                key=copy.deepcopy(current.key),
                change_kind=ChangeKind.UPDATE_AFTER,
            ),
        ]

    def on_timer(self) -> list[RecordT]:
        """输出 Clock 已结束的非空窗口，并在输出构造后清理其状态。"""
        self._require_open()
        if self._time_characteristic == "event":
            return []
        now = require_utc(self.context.clock.now(), field="clock.now")
        return self._emit_due(now)

    def on_watermark(self, watermark: datetime) -> list[RecordT]:
        """事件时间窗口在 Watermark 到达窗口结束时输出。"""
        self._require_open()
        normalized = require_utc(watermark, field="watermark")
        if self._time_characteristic == "processing":
            return []
        if self._current_watermark is not None and normalized < self._current_watermark:
            raise RecordValidationError("Watermark 不能回退")
        if self._current_watermark == normalized:
            return []
        self._current_watermark = normalized
        return self._emit_due(normalized)

    def _emit_due(self, trigger_time: datetime) -> list[RecordT]:
        """输出不晚于 trigger_time 的窗口并清理状态。"""
        due_keys = sorted(
            (state_key for state_key in self._windows if state_key[0].end <= trigger_time),
            key=lambda item: (item[0].end, item[1]),
        )
        outputs: list[RecordT] = []
        if self._emit_mode == "final":
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
                        processing_time=self.context.clock.now(),
                        change_kind=ChangeKind.INSERT,
                        headers=headers,
                    )
                )

        for state_key in due_keys:
            del self._windows[state_key]
        return outputs

    def snapshot_state(self) -> bytes:
        """序列化窗口、聚合值、Watermark 和 Changelog 计数。"""
        self._require_open()
        windows: list[dict[str, Any]] = []
        for (window, key_token), state in sorted(
            self._windows.items(),
            key=lambda item: (item[0][0].start, item[0][1]),
        ):
            if not isinstance(state.representative, RecordEnvelope):
                raise RecordValidationError("Reduce snapshot 只支持 RecordEnvelope")
            windows.append(
                {
                    "window_start": window.start.isoformat(),
                    "window_end": window.end.isoformat(),
                    "key_token": key_token,
                    "key": copy.deepcopy(state.key),
                    "accumulator": copy.deepcopy(state.accumulator),
                    "representative": state.representative.to_dict(),
                }
            )
        return encode_state(
            "reduce-window",
            {
                "time_characteristic": self._time_characteristic,
                "emit_mode": self._emit_mode,
                "current_watermark": (
                    self._current_watermark.isoformat()
                    if self._current_watermark is not None
                    else None
                ),
                "late_records": self._late_records,
                "changelog_records": self._changelog_records,
                "retractions_applied": self._retractions_applied,
                "retract_state_deletes": self._retract_state_deletes,
                "windows": windows,
            },
        )

    def restore_state(self, snapshot: bytes) -> None:
        """严格恢复与当前算子配置匹配的窗口状态。"""
        self._require_open()
        if self._windows:
            raise RecordValidationError("恢复前 Reduce 状态必须为空")
        try:
            state = decode_state(snapshot, "reduce-window")
        except CheckpointError as exc:
            raise RecordValidationError(f"Reduce snapshot 非法: {exc}") from exc
        expected_fields = {
            "time_characteristic",
            "emit_mode",
            "current_watermark",
            "late_records",
            "changelog_records",
            "retractions_applied",
            "retract_state_deletes",
            "windows",
        }
        if set(state) != expected_fields:
            raise RecordValidationError("Reduce snapshot 字段集合不匹配")
        if state["time_characteristic"] != self._time_characteristic:
            raise RecordValidationError("Reduce snapshot time_characteristic 不匹配")
        if state["emit_mode"] != self._emit_mode:
            raise RecordValidationError("Reduce snapshot emit_mode 不匹配")
        counters: dict[str, int] = {}
        for field_name in (
            "late_records",
            "changelog_records",
            "retractions_applied",
            "retract_state_deletes",
        ):
            value = state[field_name]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise RecordValidationError(f"Reduce snapshot {field_name} 必须是非负整数")
            counters[field_name] = value
        raw_watermark = state["current_watermark"]
        if raw_watermark is None:
            watermark = None
        elif isinstance(raw_watermark, str):
            watermark = _parse_snapshot_time(raw_watermark, "current_watermark")
        else:
            raise RecordValidationError("Reduce snapshot current_watermark 必须是字符串或 null")
        raw_windows = state["windows"]
        if not isinstance(raw_windows, list):
            raise RecordValidationError("Reduce snapshot windows 必须是 array")
        restored: dict[tuple[TimeWindow, str], _WindowState] = {}
        for index, document in enumerate(raw_windows):
            if not isinstance(document, dict) or set(document) != {
                "window_start",
                "window_end",
                "key_token",
                "key",
                "accumulator",
                "representative",
            }:
                raise RecordValidationError(f"Reduce snapshot windows[{index}] 字段集合不匹配")
            start = _parse_snapshot_time(document["window_start"], "window_start")
            end = _parse_snapshot_time(document["window_end"], "window_end")
            key_token = document["key_token"]
            if not isinstance(key_token, str) or key_token != canonical_json(document["key"]):
                raise RecordValidationError(f"Reduce snapshot windows[{index}] key_token 不匹配")
            try:
                representative = RecordEnvelope.from_dict(document["representative"])
            except Exception as exc:
                raise RecordValidationError(
                    f"Reduce snapshot windows[{index}] representative 非法: {exc}"
                ) from exc
            validate_json_value(document["accumulator"], field="snapshot accumulator")
            state_key = (TimeWindow(start, end), key_token)
            if state_key in restored:
                raise RecordValidationError("Reduce snapshot 包含重复 window/key")
            restored[state_key] = _WindowState(
                key=copy.deepcopy(document["key"]),
                accumulator=copy.deepcopy(document["accumulator"]),
                representative=representative,
            )
        self._windows = restored
        self._current_watermark = watermark
        self._late_records = counters["late_records"]
        self._changelog_records = counters["changelog_records"]
        self._retractions_applied = counters["retractions_applied"]
        self._retract_state_deletes = counters["retract_state_deletes"]

    def close(self) -> None:
        """释放第一阶段的全部内存状态并关闭算子。"""
        self._windows.clear()
        super().close()


def _format_window_time(value: datetime) -> str:
    """按文件 Sink 契约格式化 UTC 窗口边界。"""
    return require_utc(value).strftime("%Y/%m/%dT%H:%M:%S")


def _parse_snapshot_time(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise RecordValidationError(f"Reduce snapshot {field_name} 必须是字符串")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RecordValidationError(f"Reduce snapshot {field_name} 不是合法 ISO-8601 时间") from exc
    return require_utc(parsed, field=field_name)


__all__ = [
    "ReduceWindowOperator",
    "TimeWindow",
    "TumblingProcessingTimeWindowAssigner",
]
