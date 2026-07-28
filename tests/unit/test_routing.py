"""三种 Shuffle 路由的确定性和完整性测试。"""

from datetime import UTC, datetime

import pytest

from pystream.api import Partitioning
from pystream.common import RecordEnvelope
from pystream.runtime.routing import (
    RoutingError,
    ShuffleRouter,
    canonical_json,
    stable_hash_partition,
)


def record(index: int, key=None) -> RecordEnvelope:
    """创建带指定 key 的稳定记录。"""
    return RecordEnvelope(
        record_id=f"topic:0:{index}",
        payload={"index": index},
        key=key,
        processing_time=datetime(2026, 7, 26, 12, 0, tzinfo=UTC),
    )


def test_forward_始终选择同编号下游():
    router = ShuffleRouter(Partitioning.FORWARD, downstream_parallelism=3, upstream_subtask=2)

    assert [router.route(record(index)) for index in range(5)] == [2, 2, 2, 2, 2]


def test_forward_拒绝不存在的同编号下游():
    with pytest.raises(RoutingError, match="同编号"):
        ShuffleRouter(Partitioning.FORWARD, downstream_parallelism=2, upstream_subtask=2)


def test_rebalance_轮询且每条记录只选择一个目标():
    router = ShuffleRouter(Partitioning.REBALANCE, downstream_parallelism=3, upstream_subtask=0)
    inputs = [record(index) for index in range(10)]

    targets = [router.route(item) for item in inputs]

    assert targets == [0, 1, 2, 0, 1, 2, 0, 1, 2, 0]
    assert len(targets) == len(inputs)
    assert all(0 <= target < 3 for target in targets)


def test_rebalance_以_upstream_subtask_错开起点():
    router = ShuffleRouter(Partitioning.REBALANCE, downstream_parallelism=3, upstream_subtask=2)

    assert [router.route(record(index)) for index in range(4)] == [2, 0, 1, 2]


def test_hash_相同语义_key_跨实例稳定映射():
    left = {"word": "苹果", "meta": {"b": 2, "a": 1}}
    right = {"meta": {"a": 1, "b": 2}, "word": "苹果"}
    first = ShuffleRouter(Partitioning.HASH, downstream_parallelism=7, upstream_subtask=0)
    second = ShuffleRouter(Partitioning.HASH, downstream_parallelism=7, upstream_subtask=5)

    assert canonical_json(left) == canonical_json(right)
    assert first.route(record(0, left)) == second.route(record(1, right))
    assert stable_hash_partition(left, 7) == stable_hash_partition(right, 7)


def test_hash_结果与固定_sha256_基线一致():
    assert stable_hash_partition("apple", 8) == 0


def test_hash_拒绝未设置_key_的记录():
    router = ShuffleRouter(Partitioning.HASH, downstream_parallelism=2, upstream_subtask=0)

    with pytest.raises(RoutingError, match="非 null key"):
        router.route(record(0))


@pytest.mark.parametrize("parallelism", [0, -1, True, "2"])
def test_路由器拒绝非法下游并发度(parallelism):
    with pytest.raises(RoutingError, match="正整数"):
        ShuffleRouter(Partitioning.REBALANCE, parallelism, 0)

    with pytest.raises(RoutingError, match="正整数"):
        stable_hash_partition("apple", parallelism)


@pytest.mark.parametrize("subtask", [-1, True])
def test_路由器拒绝非法上游_subtask(subtask):
    with pytest.raises(RoutingError, match="非负整数"):
        ShuffleRouter(Partitioning.REBALANCE, 2, subtask)
