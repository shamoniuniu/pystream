"""高级 Checkpoint 状态机与严格持久化模型测试。"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pystream.checkpoint import (
    CHECKPOINT_SCHEMA_VERSION,
    CheckpointDecision,
    CheckpointError,
    CheckpointFinalization,
    CheckpointPhase,
    CheckpointStateMachine,
    TaskSnapshotDescriptor,
    TransactionDescriptor,
)

AT = datetime(2026, 7, 31, 12, 0, tzinfo=UTC)


def transaction() -> TransactionDescriptor:
    return TransactionDescriptor(
        job_id="job-1",
        checkpoint_id=7,
        attempt_id=2,
        coordinator_epoch=4,
        task_id="job-1:output:0",
        operator_id="output",
        transaction_id="tx-1",
        pending_path="pending/attempt-2/tx-1/part-0.csv",
        sha256="a" * 64,
        size=42,
    )


def snapshot() -> TaskSnapshotDescriptor:
    return TaskSnapshotDescriptor(
        job_id="job-1",
        checkpoint_id=7,
        attempt_id=2,
        task_id="job-1:output:0",
        operator_id="output",
        relative_path="job-1/checkpoint-7/output.json",
        sha256="b" * 64,
        size=100,
        coordinator_epoch=4,
        transactions=(transaction(),),
    )


def test_checkpoint状态机_覆盖成功_abort和finalize重试路径() -> None:
    successful = CheckpointStateMachine()
    for phase in (
        CheckpointPhase.ARMED,
        CheckpointPhase.ALIGNING,
        CheckpointPhase.PREPARED,
        CheckpointPhase.DECIDED,
        CheckpointPhase.FINALIZING,
        CheckpointPhase.FINALIZING,
        CheckpointPhase.FINALIZED,
        CheckpointPhase.IDLE,
    ):
        successful.transition(phase)
    assert successful.phase is CheckpointPhase.IDLE

    aborted = CheckpointStateMachine(CheckpointPhase.ARMED)
    aborted.transition(CheckpointPhase.ABORTED)
    aborted.transition(CheckpointPhase.RECOVERING)
    aborted.transition(CheckpointPhase.IDLE)
    assert aborted.phase is CheckpointPhase.IDLE


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (CheckpointPhase.IDLE, CheckpointPhase.DECIDED),
        (CheckpointPhase.DECIDED, CheckpointPhase.ABORTED),
        (CheckpointPhase.FINALIZING, CheckpointPhase.ABORTED),
        (CheckpointPhase.FINALIZED, CheckpointPhase.ARMED),
    ],
)
def test_checkpoint状态机_拒绝跳步和decision回退(
    current: CheckpointPhase,
    target: CheckpointPhase,
) -> None:
    state = CheckpointStateMachine(current)

    with pytest.raises(CheckpointError, match="不能从"):
        state.transition(target)

    assert state.phase is current


def test_transaction_snapshot_decision和finalization严格往返() -> None:
    descriptor = snapshot()
    assert TaskSnapshotDescriptor.from_dict(descriptor.to_dict()) == descriptor

    decision = CheckpointDecision(
        job_id="job-1",
        checkpoint_id=7,
        attempt_id=2,
        coordinator_epoch=4,
        decided_at=AT,
        snapshots=(descriptor,),
    )
    assert CheckpointDecision.from_dict(decision.to_dict()) == decision

    finalized = CheckpointFinalization(
        job_id="job-1",
        checkpoint_id=7,
        attempt_id=2,
        coordinator_epoch=4,
        finalized_at=AT,
        decision_sha256="c" * 64,
        output_manifests=("manifests/checkpoint-7.json",),
    )
    assert CheckpointFinalization.from_dict(finalized.to_dict()) == finalized
    assert finalized.schema_version == CHECKPOINT_SCHEMA_VERSION


def test_checkpoint文档_拒绝旧schema_身份错配和越界路径() -> None:
    decision = CheckpointDecision(
        job_id="job-1",
        checkpoint_id=7,
        attempt_id=2,
        coordinator_epoch=4,
        decided_at=AT,
        snapshots=(snapshot(),),
    ).to_dict()
    decision["schema_version"] = 1
    with pytest.raises(CheckpointError, match="schema 不兼容"):
        CheckpointDecision.from_dict(decision)

    descriptor = snapshot().to_dict()
    descriptor["transactions"][0]["coordinator_epoch"] = 3
    with pytest.raises(CheckpointError, match="身份不一致"):
        TaskSnapshotDescriptor.from_dict(descriptor)

    descriptor = snapshot().to_dict()
    descriptor["transactions"][0]["pending_path"] = "../escape.csv"
    with pytest.raises(CheckpointError, match="相对路径"):
        TaskSnapshotDescriptor.from_dict(descriptor)

    finalized = CheckpointFinalization(
        job_id="job-1",
        checkpoint_id=7,
        attempt_id=2,
        coordinator_epoch=4,
        finalized_at=AT,
        decision_sha256="c" * 64,
        output_manifests=("manifests/checkpoint-7.json",),
    ).to_dict()
    finalized["output_manifests"] = []
    with pytest.raises(CheckpointError, match="output_manifests"):
        CheckpointFinalization.from_dict(finalized)
