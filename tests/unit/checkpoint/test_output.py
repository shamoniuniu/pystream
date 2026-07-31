"""事务 File Sink 的 manifest-last 发布与接管恢复测试。"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pystream.api import FileSinkConfig
from pystream.checkpoint import (
    CheckpointDecision,
    CheckpointError,
    LocalFileOutputCommitter,
    TaskSnapshotDescriptor,
    TransactionDescriptor,
)
from pystream.common import RecordEnvelope
from pystream.operators import FileSinkOperator, OperatorContext


def _record(word: str = "apple") -> RecordEnvelope:
    return RecordEnvelope(
        record_id=f"words:0:{word}",
        payload={"word": word, "count": 1},
        processing_time=datetime(2026, 7, 31, 12, 0, tzinfo=UTC),
        headers={"window_end": "2026-07-31T12:00:00Z"},
    )


def _sink(tmp_path: Path, transaction_ids: tuple[str, ...]) -> FileSinkOperator:
    identifiers = iter(transaction_ids)
    return FileSinkOperator(
        OperatorContext("output"),
        job_id="job-1",
        config=FileSinkConfig(
            connector="file",
            output_path=str(tmp_path),
        ),
        transactional=True,
        attempt_id=2,
        coordinator_epoch=4,
        transaction_id_factory=lambda: next(identifiers),
    )


def _decision(transaction: TransactionDescriptor) -> CheckpointDecision:
    snapshot = TaskSnapshotDescriptor(
        job_id=transaction.job_id,
        checkpoint_id=transaction.checkpoint_id,
        attempt_id=transaction.attempt_id,
        coordinator_epoch=transaction.coordinator_epoch,
        task_id=transaction.task_id,
        operator_id=transaction.operator_id,
        relative_path="job-1/checkpoint-7/output.json",
        sha256="a" * 64,
        size=100,
        transactions=(transaction,),
    )
    return CheckpointDecision(
        job_id=transaction.job_id,
        checkpoint_id=transaction.checkpoint_id,
        attempt_id=transaction.attempt_id,
        coordinator_epoch=transaction.coordinator_epoch,
        decided_at=datetime(2026, 7, 31, 12, 1, tzinfo=UTC),
        snapshots=(snapshot,),
    )


def test_output_manifest_is_invisible_until_all_fragments_are_committed(
    tmp_path: Path,
) -> None:
    sink = _sink(tmp_path, ("prepared", "active"))
    sink.open()
    sink.process(_record())
    transaction = sink.pre_commit(7)
    decision = _decision(transaction)
    committer = LocalFileOutputCommitter()
    manifest = tmp_path / "job-1" / "output" / "manifests" / "checkpoint-00000000000000000007.json"

    with pytest.raises(CheckpointError, match="不存在"):
        committer.publish(decision, output_roots={"output": tmp_path})
    assert not manifest.exists()

    sink.commit_transaction(7)
    published = committer.publish(
        decision,
        output_roots={"output": tmp_path},
    )
    retried = committer.publish(
        decision,
        output_roots={"output": tmp_path},
    )

    assert published == retried == (manifest.resolve().as_posix(),)
    document = json.loads(manifest.read_text(encoding="utf-8"))
    assert document["checkpoint_id"] == 7
    assert document["fragments"] == [
        {
            "relative_path": (
                "job-1/output/committed/checkpoint-00000000000000000007/part-00000.csv"
            ),
            "sha256": transaction.sha256,
            "size": transaction.size,
            "task_id": "job-1:output:0",
            "transaction_id": "prepared",
        }
    ]
    sink.close()


def test_decided_pending_fragment_can_be_finalized_after_process_loss(
    tmp_path: Path,
) -> None:
    sink = _sink(tmp_path, ("decided", "discarded-active"))
    sink.open()
    sink.process(_record("recover"))
    transaction = sink.pre_commit(8)
    decision = _decision(
        TransactionDescriptor(
            **{
                **transaction.to_dict(),
                "checkpoint_id": 8,
            }
        )
    )
    sink.close()
    committer = LocalFileOutputCommitter()

    committer.finalize_transactions(
        decision,
        output_roots={"output": tmp_path},
    )
    committer.finalize_transactions(
        decision,
        output_roots={"output": tmp_path},
    )
    manifests = committer.publish(
        decision,
        output_roots={"output": tmp_path},
    )

    committed = (
        tmp_path
        / "job-1"
        / "output"
        / "committed"
        / "checkpoint-00000000000000000008"
        / "part-00000.csv"
    )
    assert committed.read_text(encoding="utf-8").endswith(",recover,1\n")
    assert len(manifests) == 1


def test_output_committer_orphan_cleanup_preserves_decision_paths(
    tmp_path: Path,
) -> None:
    pending = tmp_path / "job-1" / "output" / "pending" / "attempt-00000001"
    protected = pending / "tx-protected" / "part-00000.csv"
    orphan = pending / "tx-orphan" / "part-00000.csv"
    protected.parent.mkdir(parents=True)
    orphan.parent.mkdir(parents=True)
    protected.write_text("keep", encoding="utf-8")
    orphan.write_text("delete", encoding="utf-8")
    committer = LocalFileOutputCommitter()

    removed = committer.cleanup_orphans(
        job_id="job-1",
        output_roots={"output": tmp_path},
        protected_pending_paths={
            protected.relative_to(tmp_path).as_posix(),
        },
    )

    assert removed == 1
    assert protected.is_file()
    assert not orphan.exists()
