"""S3 Checkpoint immutable objects、decision 与 finalized 测试。"""

from __future__ import annotations

import hashlib

import pytest

from pystream.checkpoint import (
    CheckpointError,
    S3CheckpointStore,
    TransactionDescriptor,
)
from pystream.storage import (
    ObjectConflict,
    ObjectNotFound,
    ObjectStoreError,
    ObjectValue,
)


class MemoryObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, ObjectValue] = {}

    def get(self, key: str) -> ObjectValue:
        try:
            return self.objects[key]
        except KeyError as exc:
            raise ObjectNotFound(key) from exc

    def put_if_absent(self, key: str, content: bytes) -> str:
        if key in self.objects:
            raise ObjectConflict(key)
        etag = hashlib.sha256(content).hexdigest()
        self.objects[key] = ObjectValue(content, etag)
        return etag

    def put_if_match(self, key: str, content: bytes, etag: str) -> str:
        raise NotImplementedError

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        return tuple(sorted(key for key in self.objects if key.startswith(prefix)))


class ResponseLostObjectStore(MemoryObjectStore):
    def __init__(self) -> None:
        super().__init__()
        self.lose_next_response = False

    def put_if_absent(self, key: str, content: bytes) -> str:
        etag = super().put_if_absent(key, content)
        if self.lose_next_response:
            self.lose_next_response = False
            raise ObjectStoreError("simulated response loss")
        return etag


def test_s3_checkpoint_decision_finalization_and_latest_roundtrip() -> None:
    objects = MemoryObjectStore()
    store = S3CheckpointStore(objects)
    source = store.write_task_snapshot(
        job_id="job-1",
        checkpoint_id=7,
        attempt_id=2,
        coordinator_epoch=4,
        task_id="job-1:words:0",
        operator_id="words",
        state={"kind": "source"},
    )
    transaction = TransactionDescriptor(
        job_id="job-1",
        checkpoint_id=7,
        attempt_id=2,
        coordinator_epoch=4,
        task_id="job-1:output:0",
        operator_id="output",
        transaction_id="tx-7",
        pending_path="job-1/output/pending/attempt-00000002/tx-tx-7/part-00000.csv",
        sha256="a" * 64,
        size=10,
    )
    sink = store.write_task_snapshot(
        job_id="job-1",
        checkpoint_id=7,
        attempt_id=2,
        coordinator_epoch=4,
        task_id="job-1:output:0",
        operator_id="output",
        state={"kind": "operator"},
        transactions=(transaction,),
    )

    decision = store.decide_checkpoint(
        job_id="job-1",
        checkpoint_id=7,
        attempt_id=2,
        coordinator_epoch=4,
        expected_task_ids={source.task_id, sink.task_id},
        expected_transaction_task_ids={sink.task_id},
        snapshots=(sink, source),
    )
    retried_decision = store.decide_checkpoint(
        job_id="job-1",
        checkpoint_id=7,
        attempt_id=2,
        coordinator_epoch=4,
        expected_task_ids={source.task_id, sink.task_id},
        expected_transaction_task_ids={sink.task_id},
        snapshots=(source, sink),
    )
    finalization = store.finalize_checkpoint(
        decision,
        output_manifests=("s3://output/checkpoint-7.json",),
    )
    retried_finalization = store.finalize_checkpoint(
        decision,
        output_manifests=("s3://output/checkpoint-7.json",),
    )

    assert store.read_task_snapshot(source) == {"kind": "source"}
    assert retried_decision == decision
    assert retried_finalization == finalization
    assert store.read_decision("job-1", 7) == decision
    assert store.read_finalization("job-1", 7) == finalization
    assert store.unfinalized_decisions("job-1") == ()
    latest = store.latest_manifest(
        "job-1",
        expected_task_ids={source.task_id, sink.task_id},
    )
    assert latest is not None and latest.checkpoint_id == 7
    assert {
        "pystream/checkpoints/job-1/00000000000000000007/decision.json",
        "pystream/checkpoints/job-1/00000000000000000007/finalized.json",
    }.issubset(objects.objects)


def test_s3_checkpoint_immutable_snapshot_conflict_is_rejected() -> None:
    store = S3CheckpointStore(MemoryObjectStore())
    arguments = {
        "job_id": "job-1",
        "checkpoint_id": 1,
        "attempt_id": 0,
        "task_id": "job-1:map:0",
        "operator_id": "map",
    }
    store.write_task_snapshot(**arguments, state={"value": 1})

    with pytest.raises(CheckpointError, match="内容冲突"):
        store.write_task_snapshot(**arguments, state={"value": 2})


def test_s3_checkpoint_manifest_reconciles_success_after_response_loss() -> None:
    objects = ResponseLostObjectStore()
    store = S3CheckpointStore(objects)
    snapshot = store.write_task_snapshot(
        job_id="job-1",
        checkpoint_id=3,
        attempt_id=0,
        task_id="job-1:map:0",
        operator_id="map",
        state={"value": 1},
    )
    objects.lose_next_response = True

    manifest = store.complete_checkpoint(
        job_id="job-1",
        checkpoint_id=3,
        attempt_id=0,
        expected_task_ids={snapshot.task_id},
        snapshots=(snapshot,),
    )

    assert store.latest_manifest("job-1") == manifest


def test_s3_checkpoint_abort_leaves_unreachable_immutable_attempt() -> None:
    objects = MemoryObjectStore()
    store = S3CheckpointStore(objects)
    descriptor = store.write_task_snapshot(
        job_id="job-1",
        checkpoint_id=2,
        attempt_id=1,
        task_id="job-1:map:0",
        operator_id="map",
        state={"value": 1},
    )

    store.abort_checkpoint("job-1", 2, 1)

    assert store.read_task_snapshot(descriptor) == {"value": 1}
    assert store.latest_manifest("job-1") is None
