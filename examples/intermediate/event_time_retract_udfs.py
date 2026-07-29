"""事件时间 WordCount 与 count 分布二级聚合 UDF。"""

from __future__ import annotations

from typing import Any


def validate_input(payload: Any) -> None:
    """校验中级 demo 输入结构。"""
    if not isinstance(payload, dict):
        raise ValueError("输入必须是 JSON object")
    if not isinstance(payload.get("word"), str) or not payload["word"]:
        raise ValueError("word 必须是非空字符串")
    count = payload.get("count")
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        raise ValueError("count 必须是正整数")
    if not isinstance(payload.get("event_time"), str) or not payload["event_time"]:
        raise ValueError("event_time 必须是非空 RFC3339 字符串")


def normalize(payload: dict[str, Any]) -> dict[str, Any]:
    """把 word 转为小写，事件时间已经进入 RecordEnvelope 元数据。"""
    return {
        "word": payload["word"].lower(),
        "count": payload["count"],
    }


def word_key(payload: dict[str, Any]) -> str:
    """按归一化 word 分区。"""
    return payload["word"]


def add_word_counts(
    left: dict[str, Any],
    right: dict[str, Any],
) -> dict[str, Any]:
    """累加同一 word 的窗口计数。"""
    return {
        "word": left["word"],
        "count": left["count"] + right["count"],
    }


def to_count_bucket(payload: dict[str, Any]) -> dict[str, int]:
    """把 word count 转为“count 桶中一个 word”的贡献。"""
    return {
        "count": payload["count"],
        "word_count": 1,
    }


def count_key(payload: dict[str, int]) -> int:
    """按 count 桶重新分区。"""
    return payload["count"]


def add_bucket(
    left: dict[str, int],
    right: dict[str, int],
) -> dict[str, int]:
    """增加一个 count 桶中的 word 数。"""
    if left["count"] != right["count"]:
        raise ValueError("count bucket 不一致")
    return {
        "count": left["count"],
        "word_count": left["word_count"] + right["word_count"],
    }


def remove_bucket(
    accumulator: dict[str, int],
    value: dict[str, int],
) -> dict[str, int] | None:
    """撤回一个 word 对旧 count 桶的贡献。"""
    if accumulator["count"] != value["count"]:
        raise ValueError("count bucket 不一致")
    remaining = accumulator["word_count"] - value["word_count"]
    if remaining < 0:
        raise ValueError("word_count 不能为负数")
    if remaining == 0:
        return None
    return {
        "count": accumulator["count"],
        "word_count": remaining,
    }
