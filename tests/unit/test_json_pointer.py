"""RFC 6901 JSON Pointer 读取与错误边界测试。"""

from __future__ import annotations

import pytest

from pystream.common import (
    JsonPointerError,
    resolve_json_pointer,
    validate_json_pointer,
)


def test_pointer_支持根节点_对象_数组和转义() -> None:
    document = {
        "event/time": [{"~stamp": "2026-07-29T00:00:00Z"}],
        "": "empty-key",
    }

    assert resolve_json_pointer(document, "") is document
    assert resolve_json_pointer(document, "/event~1time/0/~0stamp") == ("2026-07-29T00:00:00Z")
    assert resolve_json_pointer(document, "/") == "empty-key"


@pytest.mark.parametrize("pointer", ["event_time", "/bad~", "/bad~2escape"])
def test_pointer_拒绝非法语法(pointer: str) -> None:
    with pytest.raises(JsonPointerError):
        validate_json_pointer(pointer)


@pytest.mark.parametrize(
    "pointer",
    ["/missing", "/items/-", "/items/01", "/items/2", "/scalar/next"],
)
def test_pointer_拒绝不存在或非法路径(pointer: str) -> None:
    document = {"items": ["first"], "scalar": 1}

    with pytest.raises(JsonPointerError):
        resolve_json_pointer(document, pointer)
