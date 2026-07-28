"""WordCount 示例的用户函数。

输入消息是 ``{"word": "APPLE", "count": 1}``。Map 统一单词大小写，
KeySelector 按单词分区，Reduce 在同一处理时间窗口内累加 count。
"""

from __future__ import annotations

from typing import Any


def validate_input(value: Any) -> None:
    """校验 Kafka Source 解码后的 WordCount payload。"""
    if not isinstance(value, dict):
        raise ValueError("WordCount 输入必须是 JSON object")
    word = value.get("word")
    count = value.get("count")
    if not isinstance(word, str) or not word:
        raise ValueError("word 必须是非空字符串")
    if isinstance(count, bool) or not isinstance(count, int):
        raise ValueError("count 必须是整数")


def normalize(value: Any) -> dict[str, Any]:
    """把已经通过 Source 校验的单词转换为小写。"""
    validate_input(value)
    word = value["word"]
    count = value["count"]
    return {"word": word.lower(), "count": count}


def word_key(value: Any) -> str:
    """返回归一化后的单词作为 Shuffle key。"""
    if not isinstance(value, dict) or not isinstance(value.get("word"), str):
        raise ValueError("归一化记录必须包含字符串 word")
    return value["word"]


def add_counts(left: Any, right: Any) -> dict[str, Any]:
    """累加同 key、同窗口内两条记录的 count。"""
    if not isinstance(left, dict) or not isinstance(right, dict):
        raise ValueError("Reduce 输入必须是 JSON object")
    if left.get("word") != right.get("word"):
        raise ValueError("Reduce 只能合并相同 word")
    left_count = left.get("count")
    right_count = right.get("count")
    if (
        isinstance(left_count, bool)
        or not isinstance(left_count, int)
        or isinstance(right_count, bool)
        or not isinstance(right_count, int)
    ):
        raise ValueError("count 必须是整数")
    return {"word": left["word"], "count": left_count + right_count}
