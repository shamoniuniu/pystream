"""无状态 Map 和 KeyBy 算子。

两个算子都保持记录元数据不变，只分别更新 payload 或 key。用户函数返回值在进入
后续网络通道前执行严格 JSON 校验，避免把不可序列化错误推迟到 Shuffle 阶段。
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pystream.operators.base import (
    BaseOperator,
    RecordT,
    clone_record,
    validate_json_value,
)
from pystream.operators.errors import RecordValidationError


class MapOperator(BaseOperator):
    """执行一对一 Map；UDF 返回 ``None`` 时显式丢弃记录。"""

    def __init__(self, context, map_function: Callable[[Any], Any]) -> None:
        super().__init__(context)
        if not callable(map_function):
            raise TypeError("map_function 必须可调用")
        self._map_function = map_function

    def process(self, record: RecordT) -> list[RecordT]:
        """映射 payload，返回空列表表示过滤。"""
        self._require_open()
        result = self._map_function(record.payload)
        if result is None:
            return []
        validate_json_value(result, field="Map UDF 返回值")
        return [clone_record(record, payload=result)]


class KeyByOperator(BaseOperator):
    """执行 KeySelector 并把记录标记为 keyed。"""

    def __init__(self, context, key_selector: Callable[[Any], Any]) -> None:
        super().__init__(context)
        if not callable(key_selector):
            raise TypeError("key_selector 必须可调用")
        self._key_selector = key_selector

    def process(self, record: RecordT) -> list[RecordT]:
        """提取非空、可 JSON 序列化的 key。"""
        self._require_open()
        key = self._key_selector(record.payload)
        if key is None:
            raise RecordValidationError("KeyBy UDF 不能返回 null")
        validate_json_value(key, field="KeyBy UDF 返回值")
        return [clone_record(record, key=key)]


__all__ = ["KeyByOperator", "MapOperator"]
