"""Checkpoint Store 原子完成、完整性和损坏回退测试。"""

from __future__ import annotations

from pathlib import Path

import pytest

from pystream.checkpoint import CheckpointError, LocalCheckpointStore


def write(
    store: LocalCheckpointStore,
    checkpoint_id: int,
    task_id: str,
    *,
    attempt_id: int = 1,
    value: int = 1,
):
    """写入一个稳定测试快照。"""
    return store.write_task_snapshot(
        job_id="job-1",
        checkpoint_id=checkpoint_id,
        attempt_id=attempt_id,
        task_id=task_id,
        operator_id=task_id.split("-")[0],
        state={"value": value},
    )


def test_task_snapshot和manifest往返并选择latest(tmp_path: Path) -> None:
    store = LocalCheckpointStore(tmp_path)
    first = write(store, 1, "map-0")
    second = write(store, 1, "reduce-0")

    manifest = store.complete_checkpoint(
        job_id="job-1",
        checkpoint_id=1,
        attempt_id=1,
        expected_task_ids={"map-0", "reduce-0"},
        snapshots=(second, first),
    )
    retried = store.complete_checkpoint(
        job_id="job-1",
        checkpoint_id=1,
        attempt_id=1,
        expected_task_ids={"map-0", "reduce-0"},
        snapshots=(first, second),
    )

    assert [item.task_id for item in manifest.snapshots] == ["map-0", "reduce-0"]
    assert retried == manifest
    assert store.read_task_snapshot(first) == {"value": 1}
    assert (
        store.latest_manifest(
            "job-1",
            expected_task_ids={"map-0", "reduce-0"},
        )
        == manifest
    )


def test_manifest要求完整执行图且不可覆盖(tmp_path: Path) -> None:
    store = LocalCheckpointStore(tmp_path)
    first = write(store, 1, "map-0")

    with pytest.raises(CheckpointError, match="执行图"):
        store.complete_checkpoint(
            job_id="job-1",
            checkpoint_id=1,
            attempt_id=1,
            expected_task_ids={"map-0", "reduce-0"},
            snapshots=(first,),
        )

    store.complete_checkpoint(
        job_id="job-1",
        checkpoint_id=1,
        attempt_id=1,
        expected_task_ids={"map-0"},
        snapshots=(first,),
    )
    with pytest.raises(CheckpointError, match="不可覆盖"):
        store.complete_checkpoint(
            job_id="job-1",
            checkpoint_id=1,
            attempt_id=1,
            expected_task_ids={"map-0"},
            snapshots=(first,),
            created_at=manifest_time(),
        )


def manifest_time():
    """返回与默认完成时间不同的固定 UTC 时间。"""
    from datetime import UTC, datetime

    return datetime(2026, 7, 29, tzinfo=UTC)


def test_同attempt快照幂等但不同内容不可覆盖(tmp_path: Path) -> None:
    store = LocalCheckpointStore(tmp_path)
    first = write(store, 1, "map-0", value=1)
    duplicate = write(store, 1, "map-0", value=1)

    assert duplicate == first
    with pytest.raises(CheckpointError, match="不可覆盖"):
        write(store, 1, "map-0", value=2)


def test_attempt目录隔离且abort只清理未完成attempt(tmp_path: Path) -> None:
    store = LocalCheckpointStore(tmp_path)
    first = write(store, 1, "map-0", attempt_id=1)
    second = write(store, 1, "map-0", attempt_id=2)

    assert first.relative_path != second.relative_path
    store.abort_checkpoint("job-1", 1, 2)

    assert store.read_task_snapshot(first) == {"value": 1}
    with pytest.raises(CheckpointError, match="不存在"):
        store.read_task_snapshot(second)


def test_latest跳过损坏和无manifest目录(tmp_path: Path) -> None:
    store = LocalCheckpointStore(tmp_path)
    first = write(store, 1, "map-0")
    completed = store.complete_checkpoint(
        job_id="job-1",
        checkpoint_id=1,
        attempt_id=1,
        expected_task_ids={"map-0"},
        snapshots=(first,),
    )
    _ = write(store, 2, "map-0")
    third = write(store, 3, "map-0")
    store.complete_checkpoint(
        job_id="job-1",
        checkpoint_id=3,
        attempt_id=1,
        expected_task_ids={"map-0"},
        snapshots=(third,),
    )
    snapshot_path = tmp_path / third.relative_path
    snapshot_path.write_text("corrupt", encoding="utf-8")

    assert store.latest_manifest("job-1", expected_task_ids={"map-0"}) == completed


def test_snapshot大小和路径安全边界(tmp_path: Path) -> None:
    store = LocalCheckpointStore(tmp_path, max_snapshot_size=128)

    with pytest.raises(CheckpointError, match="超过上限"):
        write(store, 1, "map-0", value=int("9" * 200))
    with pytest.raises(CheckpointError, match="job_id"):
        store.write_task_snapshot(
            job_id="../escape",
            checkpoint_id=1,
            attempt_id=1,
            task_id="map-0",
            operator_id="map",
            state={},
        )
