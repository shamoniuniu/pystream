"""Deterministic event-time aggregation used by advanced acceptance."""

from __future__ import annotations

from typing import Any


def validate_input(payload: Any) -> None:
    if not isinstance(payload, dict):
        raise ValueError("input must be a JSON object")
    if not isinstance(payload.get("word"), str) or not payload["word"]:
        raise ValueError("word must be a non-empty string")
    count = payload.get("count")
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        raise ValueError("count must be a positive integer")
    if not isinstance(payload.get("event_time"), str) or not payload["event_time"]:
        raise ValueError("event_time must be a non-empty RFC3339 string")


def normalize(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "word": payload["word"].lower(),
        "count": payload["count"],
    }


def word_key(payload: dict[str, Any]) -> str:
    return payload["word"]


def add_word_counts(
    left: dict[str, Any],
    right: dict[str, Any],
) -> dict[str, Any]:
    return {
        "word": left["word"],
        "count": left["count"] + right["count"],
    }


def to_count_bucket(payload: dict[str, Any]) -> dict[str, int]:
    return {
        "count": payload["count"],
        "word_count": 1,
    }


def count_key(payload: dict[str, int]) -> int:
    return payload["count"]


def add_bucket(
    left: dict[str, int],
    right: dict[str, int],
) -> dict[str, int]:
    if left["count"] != right["count"]:
        raise ValueError("count bucket mismatch")
    return {
        "count": left["count"],
        "word_count": left["word_count"] + right["word_count"],
    }


def remove_bucket(
    accumulator: dict[str, int],
    value: dict[str, int],
) -> dict[str, int] | None:
    if accumulator["count"] != value["count"]:
        raise ValueError("count bucket mismatch")
    remaining = accumulator["word_count"] - value["word_count"]
    if remaining < 0:
        raise ValueError("word_count cannot be negative")
    if remaining == 0:
        return None
    return {
        "count": accumulator["count"],
        "word_count": remaining,
    }
