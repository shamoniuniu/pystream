"""FORWARD、REBALANCE 与稳定 HASH Shuffle 路由。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

from pystream.api import Partitioning
from pystream.common import JsonValue, RecordEnvelope


class RoutingError(ValueError):
    """记录无法按声明的分区策略路由。"""


def canonical_json(value: JsonValue) -> str:
    """生成跨进程稳定的 JSON 表示，作为 HASH 输入。"""
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise RoutingError(f"key 必须可规范化为 JSON: {exc}") from exc


def stable_hash_partition(key: JsonValue, downstream_parallelism: int) -> int:
    """使用规范 JSON 的 SHA-256 将相同 key 映射到稳定 subtask。"""
    if key is None:
        raise RoutingError("HASH 路由要求记录已经设置非 null key")
    if (
        isinstance(downstream_parallelism, bool)
        or not isinstance(downstream_parallelism, int)
        or downstream_parallelism <= 0
    ):
        raise RoutingError("downstream_parallelism 必须是正整数")
    digest = hashlib.sha256(canonical_json(key).encode("utf-8")).digest()
    return int.from_bytes(digest, byteorder="big") % downstream_parallelism


@dataclass(slots=True)
class ShuffleRouter:
    """一条上游物理通道使用的有状态分区路由器。"""

    partitioning: Partitioning
    downstream_parallelism: int
    upstream_subtask: int
    _rebalance_cursor: int = field(init=False, default=0, repr=False)

    def __post_init__(self) -> None:
        if (
            isinstance(self.downstream_parallelism, bool)
            or not isinstance(self.downstream_parallelism, int)
            or self.downstream_parallelism <= 0
        ):
            raise RoutingError("downstream_parallelism 必须是正整数")
        if (
            isinstance(self.upstream_subtask, bool)
            or not isinstance(self.upstream_subtask, int)
            or self.upstream_subtask < 0
        ):
            raise RoutingError("upstream_subtask 必须是非负整数")
        if (
            self.partitioning is Partitioning.FORWARD
            and self.upstream_subtask >= self.downstream_parallelism
        ):
            raise RoutingError("FORWARD 路由要求存在同编号的下游 subtask")
        self._rebalance_cursor = self.upstream_subtask % self.downstream_parallelism

    def route(self, record: RecordEnvelope) -> int:
        """为一条记录选择唯一的下游 subtask。"""
        if self.partitioning is Partitioning.FORWARD:
            return self.upstream_subtask
        if self.partitioning is Partitioning.REBALANCE:
            selected = self._rebalance_cursor
            self._rebalance_cursor = (selected + 1) % self.downstream_parallelism
            return selected
        if self.partitioning is Partitioning.HASH:
            return stable_hash_partition(record.key, self.downstream_parallelism)
        raise RoutingError(f"不支持的分区策略: {self.partitioning!r}")


__all__ = [
    "RoutingError",
    "ShuffleRouter",
    "canonical_json",
    "stable_hash_partition",
]
