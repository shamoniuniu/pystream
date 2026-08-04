"""Checkpoint 持久仓库的 Local/S3 共享端口。"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from pystream.checkpoint.models import (
    CheckpointDecision,
    CheckpointFinalization,
    CheckpointManifest,
    TaskSnapshotDescriptor,
    TransactionDescriptor,
)
from pystream.common import JsonValue


class CheckpointStore(Protocol):
    def write_task_snapshot(
        self,
        *,
        job_id: str,
        checkpoint_id: int,
        attempt_id: int,
        coordinator_epoch: int = 0,
        task_id: str,
        operator_id: str,
        state: dict[str, JsonValue],
        transactions: tuple[TransactionDescriptor, ...] = (),
    ) -> TaskSnapshotDescriptor: ...

    def read_task_snapshot(
        self,
        descriptor: TaskSnapshotDescriptor,
    ) -> dict[str, JsonValue]: ...

    def complete_checkpoint(
        self,
        *,
        job_id: str,
        checkpoint_id: int,
        attempt_id: int,
        coordinator_epoch: int = 0,
        expected_task_ids: set[str],
        snapshots: tuple[TaskSnapshotDescriptor, ...],
        created_at: datetime | None = None,
    ) -> CheckpointManifest: ...

    def decide_checkpoint(
        self,
        *,
        job_id: str,
        checkpoint_id: int,
        attempt_id: int,
        coordinator_epoch: int,
        expected_task_ids: set[str],
        expected_transaction_task_ids: set[str],
        snapshots: tuple[TaskSnapshotDescriptor, ...],
        decided_at: datetime | None = None,
    ) -> CheckpointDecision: ...

    def read_decision(
        self,
        job_id: str,
        checkpoint_id: int,
        *,
        expected_task_ids: set[str] | None = None,
        expected_transaction_task_ids: set[str] | None = None,
    ) -> CheckpointDecision: ...

    def finalize_checkpoint(
        self,
        decision: CheckpointDecision,
        *,
        output_manifests: tuple[str, ...],
        finalized_at: datetime | None = None,
    ) -> CheckpointFinalization: ...

    def read_finalization(
        self,
        job_id: str,
        checkpoint_id: int,
    ) -> CheckpointFinalization: ...

    def unfinalized_decisions(self, job_id: str) -> tuple[CheckpointDecision, ...]: ...

    def has_decision(self, job_id: str, checkpoint_id: int) -> bool: ...

    def latest_manifest(
        self,
        job_id: str,
        *,
        expected_task_ids: set[str] | None = None,
    ) -> CheckpointManifest | None: ...

    def abort_checkpoint(
        self,
        job_id: str,
        checkpoint_id: int,
        attempt_id: int,
    ) -> None: ...


__all__ = ["CheckpointStore"]
