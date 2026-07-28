"""处理时间时钟。

生产环境使用 :class:`SystemClock` 读取 UTC 时间；测试通过
:class:`ManualClock` 精确推进时间，避免依赖休眠导致窗口测试不稳定。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol


def require_utc(value: datetime, *, field: str = "timestamp") -> datetime:
    """验证时间有时区并规范化为 UTC。"""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} 必须包含时区")
    return value.astimezone(UTC)


class Clock(Protocol):
    """TaskRuntime 和算子共享的处理时间读取契约。"""

    def now(self) -> datetime:
        """返回带时区的 UTC 当前时间。"""


class SystemClock:
    """读取系统 UTC 时间的生产时钟。"""

    def now(self) -> datetime:
        """返回当前 UTC 时间。"""
        return datetime.now(UTC)


@dataclass
class ManualClock:
    """可由测试显式推进的确定性时钟。"""

    _current: datetime

    def __post_init__(self) -> None:
        self._current = require_utc(self._current, field="start")

    def now(self) -> datetime:
        """返回当前测试时间。"""
        return self._current

    def set(self, value: datetime) -> None:
        """将测试时间移动到不早于当前值的时间点。"""
        normalized = require_utc(value)
        if normalized < self._current:
            raise ValueError("ManualClock 不能向后移动")
        self._current = normalized

    def advance(self, delta: timedelta | float) -> datetime:
        """按 timedelta 或秒数向前推进并返回新时间。"""
        duration = timedelta(seconds=delta) if isinstance(delta, (int, float)) else delta
        if duration < timedelta(0):
            raise ValueError("ManualClock 不能按负时长推进")
        self._current += duration
        return self._current


__all__ = ["Clock", "ManualClock", "SystemClock", "require_utc"]
