"""RFC 6901 JSON Pointer 只读解析。

Source 用它从 payload 提取事件时间，File Sink 用它选择输出列。实现只提供读取，
不会创建路径或修改用户数据。
"""

from __future__ import annotations

from pystream.common.records import JsonValue


class JsonPointerError(ValueError):
    """JSON Pointer 语法非法或无法解析到目标值。"""


def _decode_token(token: str) -> str:
    decoded: list[str] = []
    index = 0
    while index < len(token):
        character = token[index]
        if character != "~":
            decoded.append(character)
            index += 1
            continue
        if index + 1 >= len(token) or token[index + 1] not in {"0", "1"}:
            raise JsonPointerError("JSON Pointer 包含非法 ~ 转义")
        decoded.append("~" if token[index + 1] == "0" else "/")
        index += 2
    return "".join(decoded)


def validate_json_pointer(pointer: str) -> str:
    """校验 pointer 并返回原值，供配置模型复用。"""
    if not isinstance(pointer, str):
        raise JsonPointerError("JSON Pointer 必须是字符串")
    if pointer and not pointer.startswith("/"):
        raise JsonPointerError("JSON Pointer 必须为空或以 / 开头")
    for token in pointer.split("/")[1:]:
        _decode_token(token)
    return pointer


def resolve_json_pointer(document: JsonValue, pointer: str) -> JsonValue:
    """从 JSON 值读取 pointer 指向的值。"""
    validate_json_pointer(pointer)
    current = document
    if pointer == "":
        return current

    for raw_token in pointer.split("/")[1:]:
        token = _decode_token(raw_token)
        if isinstance(current, dict):
            if token not in current:
                raise JsonPointerError(f"JSON Pointer 路径不存在: {pointer}")
            current = current[token]
            continue
        if isinstance(current, list):
            if token == "-" or not token.isdigit() or (len(token) > 1 and token.startswith("0")):
                raise JsonPointerError(f"JSON Pointer 数组索引非法: {token!r}")
            index = int(token)
            if index >= len(current):
                raise JsonPointerError(f"JSON Pointer 数组索引越界: {index}")
            current = current[index]
            continue
        raise JsonPointerError(f"JSON Pointer 在标量值处无法继续解析: {pointer}")
    return current


__all__ = [
    "JsonPointerError",
    "resolve_json_pointer",
    "validate_json_pointer",
]
