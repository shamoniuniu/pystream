"""Checkpoint 快照描述符与完整 manifest 契约。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Any

CHECKPOINT_SCHEMA_VERSION = 2
DEFAULT_MAX_SNAPSHOT_SIZE = 64 * 1024 * 1024


class CheckpointError(RuntimeError):
    """Checkpoint 状态、文件或 manifest 不符合一致性契约。"""


def _require_string(document: dict[str, Any], field: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise CheckpointError(f"{field} 必须是非空字符串")
    return value


def _require_integer(document: dict[str, Any], field: str) -> int:
    value = document.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CheckpointError(f"{field} 必须是非负整数")
    return value


def _require_datetime(document: dict[str, Any], field: str) -> datetime:
    raw_value = _require_string(document, field)
    try:
        value = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CheckpointError(f"{field} 不是合法 ISO-8601 时间") from exc
    if value.tzinfo is None or value.utcoffset() is None:
        raise CheckpointError(f"{field} 必须包含时区")
    return value.astimezone(UTC)


def _validate_sha256(value: str, field: str = "sha256") -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise CheckpointError(f"{field} 必须是 64 位小写十六进制")
    return value


def _strict_fields(document: object, expected: set[str], context: str) -> dict[str, Any]:
    if not isinstance(document, dict) or not all(isinstance(key, str) for key in document):
        raise CheckpointError(f"{context} 必须是 JSON object")
    missing = expected - document.keys()
    extra = document.keys() - expected
    if missing or extra:
        details: list[str] = []
        if missing:
            details.append("缺少 " + ", ".join(sorted(missing)))
        if extra:
            details.append("未知 " + ", ".join(sorted(extra)))
        raise CheckpointError(f"{context} 字段错误: {'; '.join(details)}")
    return document


class CheckpointPhase(StrEnum):
    """协调器 Checkpoint 的显式阶段。"""

    IDLE = "IDLE"
    ARMED = "ARMED"
    ALIGNING = "ALIGNING"
    PREPARED = "PREPARED"
    DECIDED = "DECIDED"
    FINALIZING = "FINALIZING"
    FINALIZED = "FINALIZED"
    ABORTED = "ABORTED"
    RECOVERING = "RECOVERING"


_CHECKPOINT_TRANSITIONS: dict[CheckpointPhase, frozenset[CheckpointPhase]] = {
    CheckpointPhase.IDLE: frozenset({CheckpointPhase.ARMED}),
    CheckpointPhase.ARMED: frozenset({CheckpointPhase.ALIGNING, CheckpointPhase.ABORTED}),
    CheckpointPhase.ALIGNING: frozenset({CheckpointPhase.PREPARED, CheckpointPhase.ABORTED}),
    CheckpointPhase.PREPARED: frozenset({CheckpointPhase.DECIDED, CheckpointPhase.ABORTED}),
    CheckpointPhase.DECIDED: frozenset({CheckpointPhase.FINALIZING}),
    CheckpointPhase.FINALIZING: frozenset({CheckpointPhase.FINALIZING, CheckpointPhase.FINALIZED}),
    CheckpointPhase.FINALIZED: frozenset({CheckpointPhase.IDLE}),
    CheckpointPhase.ABORTED: frozenset({CheckpointPhase.RECOVERING}),
    CheckpointPhase.RECOVERING: frozenset({CheckpointPhase.IDLE}),
}


@dataclass(slots=True)
class CheckpointStateMachine:
    """只允许规格声明的 Checkpoint 状态转换。"""

    phase: CheckpointPhase = CheckpointPhase.IDLE

    def transition(self, target: CheckpointPhase) -> None:
        """推进阶段；FINALIZING 自转换表示未知结果后的幂等重试。"""
        if target not in _CHECKPOINT_TRANSITIONS[self.phase]:
            raise CheckpointError(f"Checkpoint 不能从 {self.phase} 转换到 {target}")
        self.phase = target


@dataclass(frozen=True, slots=True)
class TransactionDescriptor:
    """Sink 在 Barrier 边界冻结的不可变 PREPARED transaction。"""

    job_id: str
    checkpoint_id: int
    attempt_id: int
    coordinator_epoch: int
    task_id: str
    operator_id: str
    transaction_id: str
    pending_path: str
    sha256: str
    size: int

    def to_dict(self) -> dict[str, str | int]:
        """转换为 checkpoint 文档中的严格 JSON 对象。"""
        return {
            "job_id": self.job_id,
            "checkpoint_id": self.checkpoint_id,
            "attempt_id": self.attempt_id,
            "coordinator_epoch": self.coordinator_epoch,
            "task_id": self.task_id,
            "operator_id": self.operator_id,
            "transaction_id": self.transaction_id,
            "pending_path": self.pending_path,
            "sha256": self.sha256,
            "size": self.size,
        }

    @classmethod
    def from_dict(cls, document: object) -> TransactionDescriptor:
        """从不可信快照字段恢复 transaction descriptor。"""
        data = _strict_fields(
            document,
            {
                "job_id",
                "checkpoint_id",
                "attempt_id",
                "coordinator_epoch",
                "task_id",
                "operator_id",
                "transaction_id",
                "pending_path",
                "sha256",
                "size",
            },
            "transaction descriptor",
        )
        pending_path = _require_string(data, "pending_path")
        parsed_path = PurePosixPath(pending_path)
        if parsed_path.is_absolute() or ".." in parsed_path.parts:
            raise CheckpointError("pending_path 必须是存储根目录下的相对路径")
        return cls(
            job_id=_require_string(data, "job_id"),
            checkpoint_id=_require_integer(data, "checkpoint_id"),
            attempt_id=_require_integer(data, "attempt_id"),
            coordinator_epoch=_require_integer(data, "coordinator_epoch"),
            task_id=_require_string(data, "task_id"),
            operator_id=_require_string(data, "operator_id"),
            transaction_id=_require_string(data, "transaction_id"),
            pending_path=pending_path,
            sha256=_validate_sha256(_require_string(data, "sha256")),
            size=_require_integer(data, "size"),
        )


@dataclass(frozen=True, slots=True)
class TaskSnapshotDescriptor:
    """一个不可变 Task 快照的身份与完整性信息。"""

    job_id: str
    checkpoint_id: int
    attempt_id: int
    task_id: str
    operator_id: str
    relative_path: str
    sha256: str
    size: int
    coordinator_epoch: int = 0
    transactions: tuple[TransactionDescriptor, ...] = ()

    def to_dict(self) -> dict[str, object]:
        """转换为 manifest 内的严格 JSON 对象。"""
        return {
            "job_id": self.job_id,
            "checkpoint_id": self.checkpoint_id,
            "attempt_id": self.attempt_id,
            "task_id": self.task_id,
            "operator_id": self.operator_id,
            "relative_path": self.relative_path,
            "sha256": self.sha256,
            "size": self.size,
            "coordinator_epoch": self.coordinator_epoch,
            "transactions": [item.to_dict() for item in self.transactions],
        }

    @classmethod
    def from_dict(cls, document: object) -> TaskSnapshotDescriptor:
        """从不可信 manifest 字段恢复 descriptor。"""
        data = _strict_fields(
            document,
            {
                "job_id",
                "checkpoint_id",
                "attempt_id",
                "task_id",
                "operator_id",
                "relative_path",
                "sha256",
                "size",
                "coordinator_epoch",
                "transactions",
            },
            "task snapshot descriptor",
        )
        raw_transactions = data["transactions"]
        if not isinstance(raw_transactions, list):
            raise CheckpointError("transactions 必须是数组")
        transactions = tuple(TransactionDescriptor.from_dict(item) for item in raw_transactions)
        descriptor = cls(
            job_id=_require_string(data, "job_id"),
            checkpoint_id=_require_integer(data, "checkpoint_id"),
            attempt_id=_require_integer(data, "attempt_id"),
            task_id=_require_string(data, "task_id"),
            operator_id=_require_string(data, "operator_id"),
            relative_path=_require_string(data, "relative_path"),
            sha256=_validate_sha256(_require_string(data, "sha256")),
            size=_require_integer(data, "size"),
            coordinator_epoch=_require_integer(data, "coordinator_epoch"),
            transactions=transactions,
        )
        if len({item.transaction_id for item in transactions}) != len(transactions):
            raise CheckpointError("Task snapshot 包含重复 transaction_id")
        for transaction in transactions:
            if (
                transaction.job_id != descriptor.job_id
                or transaction.checkpoint_id != descriptor.checkpoint_id
                or transaction.attempt_id != descriptor.attempt_id
                or transaction.coordinator_epoch != descriptor.coordinator_epoch
                or transaction.task_id != descriptor.task_id
                or transaction.operator_id != descriptor.operator_id
            ):
                raise CheckpointError("Task snapshot 与 transaction descriptor 身份不一致")
        return descriptor


@dataclass(frozen=True, slots=True)
class CheckpointManifest:
    """全执行图当前 attempt 的完整 Checkpoint。"""

    job_id: str
    checkpoint_id: int
    attempt_id: int
    created_at: datetime
    snapshots: tuple[TaskSnapshotDescriptor, ...]
    coordinator_epoch: int = 0
    schema_version: int = CHECKPOINT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, object]:
        """转换为规范 manifest JSON 对象。"""
        return {
            "schema_version": self.schema_version,
            "job_id": self.job_id,
            "checkpoint_id": self.checkpoint_id,
            "attempt_id": self.attempt_id,
            "coordinator_epoch": self.coordinator_epoch,
            "created_at": self.created_at.astimezone(UTC).isoformat(),
            "snapshots": [descriptor.to_dict() for descriptor in self.snapshots],
        }

    @classmethod
    def from_dict(cls, document: object) -> CheckpointManifest:
        """从不可信文件恢复并校验 manifest。"""
        data = _strict_fields(
            document,
            {
                "schema_version",
                "job_id",
                "checkpoint_id",
                "attempt_id",
                "coordinator_epoch",
                "created_at",
                "snapshots",
            },
            "checkpoint manifest",
        )
        schema_version = _require_integer(data, "schema_version")
        if schema_version != CHECKPOINT_SCHEMA_VERSION:
            raise CheckpointError(
                f"Checkpoint schema 不兼容: {schema_version} != {CHECKPOINT_SCHEMA_VERSION}"
            )
        created_at = _require_datetime(data, "created_at")
        raw_snapshots = data["snapshots"]
        if not isinstance(raw_snapshots, list) or not raw_snapshots:
            raise CheckpointError("snapshots 必须是非空数组")
        snapshots = tuple(TaskSnapshotDescriptor.from_dict(item) for item in raw_snapshots)
        if len({item.task_id for item in snapshots}) != len(snapshots):
            raise CheckpointError("manifest 包含重复 task_id")
        manifest = cls(
            schema_version=schema_version,
            job_id=_require_string(data, "job_id"),
            checkpoint_id=_require_integer(data, "checkpoint_id"),
            attempt_id=_require_integer(data, "attempt_id"),
            coordinator_epoch=_require_integer(data, "coordinator_epoch"),
            created_at=created_at,
            snapshots=snapshots,
        )
        for descriptor in manifest.snapshots:
            if (
                descriptor.job_id != manifest.job_id
                or descriptor.checkpoint_id != manifest.checkpoint_id
                or descriptor.attempt_id != manifest.attempt_id
                or descriptor.coordinator_epoch != manifest.coordinator_epoch
            ):
                raise CheckpointError("manifest 与 Task descriptor 身份不一致")
        return manifest


@dataclass(frozen=True, slots=True)
class CheckpointDecision:
    """已验证任务全集后写入的不可逆 Exactly-once 决定。"""

    job_id: str
    checkpoint_id: int
    attempt_id: int
    coordinator_epoch: int
    decided_at: datetime
    snapshots: tuple[TaskSnapshotDescriptor, ...]
    schema_version: int = CHECKPOINT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, object]:
        """转换为规范 decision.json 文档。"""
        return {
            "schema_version": self.schema_version,
            "job_id": self.job_id,
            "checkpoint_id": self.checkpoint_id,
            "attempt_id": self.attempt_id,
            "coordinator_epoch": self.coordinator_epoch,
            "decided_at": self.decided_at.astimezone(UTC).isoformat(),
            "snapshots": [descriptor.to_dict() for descriptor in self.snapshots],
        }

    @classmethod
    def from_dict(cls, document: object) -> CheckpointDecision:
        """从不可信对象存储内容恢复并验证决定。"""
        data = _strict_fields(
            document,
            {
                "schema_version",
                "job_id",
                "checkpoint_id",
                "attempt_id",
                "coordinator_epoch",
                "decided_at",
                "snapshots",
            },
            "checkpoint decision",
        )
        schema_version = _require_integer(data, "schema_version")
        if schema_version != CHECKPOINT_SCHEMA_VERSION:
            raise CheckpointError(
                f"Checkpoint schema 不兼容: {schema_version} != {CHECKPOINT_SCHEMA_VERSION}"
            )
        raw_snapshots = data["snapshots"]
        if not isinstance(raw_snapshots, list) or not raw_snapshots:
            raise CheckpointError("snapshots 必须是非空数组")
        decision = cls(
            schema_version=schema_version,
            job_id=_require_string(data, "job_id"),
            checkpoint_id=_require_integer(data, "checkpoint_id"),
            attempt_id=_require_integer(data, "attempt_id"),
            coordinator_epoch=_require_integer(data, "coordinator_epoch"),
            decided_at=_require_datetime(data, "decided_at"),
            snapshots=tuple(TaskSnapshotDescriptor.from_dict(item) for item in raw_snapshots),
        )
        if len({item.task_id for item in decision.snapshots}) != len(decision.snapshots):
            raise CheckpointError("decision 包含重复 task_id")
        for descriptor in decision.snapshots:
            if (
                descriptor.job_id != decision.job_id
                or descriptor.checkpoint_id != decision.checkpoint_id
                or descriptor.attempt_id != decision.attempt_id
                or descriptor.coordinator_epoch != decision.coordinator_epoch
            ):
                raise CheckpointError("decision 与 Task descriptor 身份不一致")
        return decision


@dataclass(frozen=True, slots=True)
class CheckpointFinalization:
    """decision 的幂等 finalize 完成凭据。"""

    job_id: str
    checkpoint_id: int
    attempt_id: int
    coordinator_epoch: int
    finalized_at: datetime
    decision_sha256: str
    output_manifests: tuple[str, ...]
    schema_version: int = CHECKPOINT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, object]:
        """转换为规范 finalized.json 文档。"""
        return {
            "schema_version": self.schema_version,
            "job_id": self.job_id,
            "checkpoint_id": self.checkpoint_id,
            "attempt_id": self.attempt_id,
            "coordinator_epoch": self.coordinator_epoch,
            "finalized_at": self.finalized_at.astimezone(UTC).isoformat(),
            "decision_sha256": self.decision_sha256,
            "output_manifests": list(self.output_manifests),
        }

    @classmethod
    def from_dict(cls, document: object) -> CheckpointFinalization:
        """从不可信对象存储内容恢复 finalize 凭据。"""
        data = _strict_fields(
            document,
            {
                "schema_version",
                "job_id",
                "checkpoint_id",
                "attempt_id",
                "coordinator_epoch",
                "finalized_at",
                "decision_sha256",
                "output_manifests",
            },
            "checkpoint finalization",
        )
        schema_version = _require_integer(data, "schema_version")
        if schema_version != CHECKPOINT_SCHEMA_VERSION:
            raise CheckpointError(
                f"Checkpoint schema 不兼容: {schema_version} != {CHECKPOINT_SCHEMA_VERSION}"
            )
        raw_manifests = data["output_manifests"]
        if (
            not isinstance(raw_manifests, list)
            or not raw_manifests
            or not all(isinstance(item, str) and item for item in raw_manifests)
            or len(set(raw_manifests)) != len(raw_manifests)
        ):
            raise CheckpointError("output_manifests 必须是唯一非空字符串数组")
        return cls(
            schema_version=schema_version,
            job_id=_require_string(data, "job_id"),
            checkpoint_id=_require_integer(data, "checkpoint_id"),
            attempt_id=_require_integer(data, "attempt_id"),
            coordinator_epoch=_require_integer(data, "coordinator_epoch"),
            finalized_at=_require_datetime(data, "finalized_at"),
            decision_sha256=_validate_sha256(
                _require_string(data, "decision_sha256"),
                "decision_sha256",
            ),
            output_manifests=tuple(raw_manifests),
        )


__all__ = [
    "CHECKPOINT_SCHEMA_VERSION",
    "DEFAULT_MAX_SNAPSHOT_SIZE",
    "CheckpointDecision",
    "CheckpointError",
    "CheckpointFinalization",
    "CheckpointManifest",
    "CheckpointPhase",
    "CheckpointStateMachine",
    "TaskSnapshotDescriptor",
    "TransactionDescriptor",
]
