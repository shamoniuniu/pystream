"""算子与 Source 内部状态的版本化规范 JSON 编解码。"""

from __future__ import annotations

import json

from pystream.checkpoint.models import CHECKPOINT_SCHEMA_VERSION, CheckpointError
from pystream.common import JsonValue


def encode_state(kind: str, state: dict[str, JsonValue]) -> bytes:
    """编码一个带 schema/kind 的不可变状态文档。"""
    if not isinstance(kind, str) or not kind:
        raise CheckpointError("state kind 必须是非空字符串")
    if not isinstance(state, dict) or not all(isinstance(key, str) for key in state):
        raise CheckpointError("state 必须是字符串键的 JSON object")
    try:
        return json.dumps(
            {
                "schema_version": CHECKPOINT_SCHEMA_VERSION,
                "kind": kind,
                "state": state,
            },
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise CheckpointError(f"state 必须可严格 JSON 序列化: {exc}") from exc


def decode_state(snapshot: bytes, expected_kind: str) -> dict[str, JsonValue]:
    """解码并严格校验状态文档。"""
    if not isinstance(snapshot, bytes) or not snapshot:
        raise CheckpointError("snapshot 必须是非空 bytes")
    try:
        document = json.loads(snapshot.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CheckpointError(f"snapshot 不是合法 UTF-8 JSON: {exc}") from exc
    if not isinstance(document, dict) or set(document) != {
        "schema_version",
        "kind",
        "state",
    }:
        raise CheckpointError("snapshot 字段必须恰好为 schema_version/kind/state")
    if document["schema_version"] != CHECKPOINT_SCHEMA_VERSION:
        raise CheckpointError("snapshot schema_version 不兼容")
    if document["kind"] != expected_kind:
        raise CheckpointError(f"snapshot kind 不匹配: {document['kind']!r} != {expected_kind!r}")
    state = document["state"]
    if not isinstance(state, dict) or not all(isinstance(key, str) for key in state):
        raise CheckpointError("snapshot state 必须是字符串键的 JSON object")
    return state


__all__ = ["decode_state", "encode_state"]
