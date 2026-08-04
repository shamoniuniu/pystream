"""对象存储的供应商无关条件读写端口。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class ObjectValue:
    """对象内容及其当前实体标签。"""

    content: bytes
    etag: str

    def __post_init__(self) -> None:
        if not isinstance(self.content, bytes):
            raise TypeError("object content 必须是 bytes")
        if not isinstance(self.etag, str) or not self.etag:
            raise ValueError("object etag 必须是非空字符串")


class ObjectStore(Protocol):
    """上层持久仓库所需的最小对象存储能力。"""

    def get(self, key: str) -> ObjectValue:
        """强校验读取一个对象。"""

    def put_if_absent(self, key: str, content: bytes) -> str:
        """仅当 key 不存在时创建并返回新 ETag。"""

    def put_if_match(self, key: str, content: bytes, etag: str) -> str:
        """仅当当前 ETag 匹配时替换并返回新 ETag。"""

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        """按字典序返回前缀下的全部 key。"""


__all__ = ["ObjectStore", "ObjectValue"]
