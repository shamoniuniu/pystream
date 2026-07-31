"""Checkpoint Store 原子完成、完整性和损坏回退测试。"""

from __future__ import annotations

from pathlib import Path

import pytest

from pystream.checkpoint import (
    CheckpointError,
    LocalCheckpointStore,
    TaskSnapshotDescriptor,
    TransactionDescriptor,
)


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


def test_task_snapshot_hash_binds_transaction_descriptor(tmp_path: Path) -> None:
    store = LocalCheckpointStore(tmp_path)
    transaction = TransactionDescriptor(
        job_id="job-1",
        checkpoint_id=7,
        attempt_id=2,
        coordinator_epoch=4,
        task_id="job-1:output:0",
        operator_id="output",
        transaction_id="tx-1",
        pending_path=("job-1/output/pending/attempt-00000002/tx-tx-1/part-00000.csv"),
        sha256="a" * 64,
        size=42,
    )
    descriptor = store.write_task_snapshot(
        job_id="job-1",
        checkpoint_id=7,
        attempt_id=2,
        coordinator_epoch=4,
        task_id="job-1:output:0",
        operator_id="output",
        state={"kind": "operator"},
        transactions=(transaction,),
    )

    assert descriptor.transactions == (transaction,)
    assert store.read_task_snapshot(descriptor) == {"kind": "operator"}

    tampered = TaskSnapshotDescriptor(
        job_id=descriptor.job_id,
        checkpoint_id=descriptor.checkpoint_id,
        attempt_id=descriptor.attempt_id,
        coordinator_epoch=descriptor.coordinator_epoch,
        task_id=descriptor.task_id,
        operator_id=descriptor.operator_id,
        relative_path=descriptor.relative_path,
        sha256=descriptor.sha256,
        size=descriptor.size,
        transactions=(
            TransactionDescriptor(
                **{
                    **transaction.to_dict(),
                    "sha256": "b" * 64,
                }
            ),
        ),
    )
    with pytest.raises(CheckpointError, match="transaction descriptor 不匹配"):
        store.read_task_snapshot(tampered)


def test_decision_finalization_roundtrip_and_decision_blocks_abort(
    tmp_path: Path,
) -> None:
    store = LocalCheckpointStore(tmp_path)
    source = write(store, 8, "words-0", attempt_id=2)
    transaction = TransactionDescriptor(
        job_id="job-1",
        checkpoint_id=8,
        attempt_id=2,
        coordinator_epoch=0,
        task_id="output-0",
        operator_id="output",
        transaction_id="tx-8",
        pending_path="job-1/output/pending/attempt-00000002/tx-tx-8/part-00000.csv",
        sha256="c" * 64,
        size=12,
    )
    sink = store.write_task_snapshot(
        job_id="job-1",
        checkpoint_id=8,
        attempt_id=2,
        task_id="output-0",
        operator_id="output",
        state={"value": 1},
        transactions=(transaction,),
    )

    decision = store.decide_checkpoint(
        job_id="job-1",
        checkpoint_id=8,
        attempt_id=2,
        coordinator_epoch=0,
        expected_task_ids={"words-0", "output-0"},
        expected_transaction_task_ids={"output-0"},
        snapshots=(sink, source),
    )
    retried = store.decide_checkpoint(
        job_id="job-1",
        checkpoint_id=8,
        attempt_id=2,
        coordinator_epoch=0,
        expected_task_ids={"words-0", "output-0"},
        expected_transaction_task_ids={"output-0"},
        snapshots=(source, sink),
    )

    assert retried == decision
    assert store.unfinalized_decisions("job-1") == (decision,)
    store.abort_checkpoint("job-1", 8, 2)
    assert store.read_decision("job-1", 8) == decision

    finalization = store.finalize_checkpoint(
        decision,
        output_manifests=("/data/output/job-1/output/manifests/checkpoint-8.json",),
    )

    assert store.read_finalization("job-1", 8) == finalization
    assert store.unfinalized_decisions("job-1") == ()
    latest = store.latest_manifest(
        "job-1",
        expected_task_ids={"words-0", "output-0"},
    )
    assert latest is not None
    assert latest.checkpoint_id == 8
    assert latest.snapshots == decision.snapshots


def test_decision_rejects_missing_or_duplicate_sink_transaction(tmp_path: Path) -> None:
    store = LocalCheckpointStore(tmp_path)
    source = write(store, 9, "words-0", attempt_id=2)
    sink_without_transaction = write(store, 9, "output-0", attempt_id=2)

    with pytest.raises(CheckpointError, match="transaction 集合"):
        store.decide_checkpoint(
            job_id="job-1",
            checkpoint_id=9,
            attempt_id=2,
            coordinator_epoch=0,
            expected_task_ids={"words-0", "output-0"},
            expected_transaction_task_ids={"output-0"},
            snapshots=(source, sink_without_transaction),
        )
    with pytest.raises(CheckpointError, match="coordinator_epoch"):
        store.write_task_snapshot(
            job_id="job-1",
            checkpoint_id=1,
            attempt_id=1,
            coordinator_epoch=True,
            task_id="map-0",
            operator_id="map",
            state={},
        )
