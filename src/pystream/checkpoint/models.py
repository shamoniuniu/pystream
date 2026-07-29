"""Checkpoint 快照描述符与完整 manifest 契约。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

CHECKPOINT_SCHEMA_VERSION = 1
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

    def to_dict(self) -> dict[str, str | int]:
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
            },
            "task snapshot descriptor",
        )
        descriptor = cls(
            job_id=_require_string(data, "job_id"),
            checkpoint_id=_require_integer(data, "checkpoint_id"),
            attempt_id=_require_integer(data, "attempt_id"),
            task_id=_require_string(data, "task_id"),
            operator_id=_require_string(data, "operator_id"),
            relative_path=_require_string(data, "relative_path"),
            sha256=_require_string(data, "sha256"),
            size=_require_integer(data, "size"),
        )
        if len(descriptor.sha256) != 64 or any(
            character not in "0123456789abcdef" for character in descriptor.sha256
        ):
            raise CheckpointError("sha256 必须是 64 位小写十六进制")
        return descriptor


@dataclass(frozen=True, slots=True)
class CheckpointManifest:
    """全执行图当前 attempt 的完整 Checkpoint。"""

    job_id: str
    checkpoint_id: int
    attempt_id: int
    created_at: datetime
    snapshots: tuple[TaskSnapshotDescriptor, ...]
    schema_version: int = CHECKPOINT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, object]:
        """转换为规范 manifest JSON 对象。"""
        return {
            "schema_version": self.schema_version,
            "job_id": self.job_id,
            "checkpoint_id": self.checkpoint_id,
            "attempt_id": self.attempt_id,
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
        raw_created_at = _require_string(data, "created_at")
        try:
            created_at = datetime.fromisoformat(raw_created_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise CheckpointError("created_at 不是合法 ISO-8601 时间") from exc
        if created_at.tzinfo is None or created_at.utcoffset() is None:
            raise CheckpointError("created_at 必须包含时区")
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
            created_at=created_at.astimezone(UTC),
            snapshots=snapshots,
        )
        for descriptor in manifest.snapshots:
            if (
                descriptor.job_id != manifest.job_id
                or descriptor.checkpoint_id != manifest.checkpoint_id
                or descriptor.attempt_id != manifest.attempt_id
            ):
                raise CheckpointError("manifest 与 Task descriptor 身份不一致")
        return manifest


__all__ = [
    "CHECKPOINT_SCHEMA_VERSION",
    "DEFAULT_MAX_SNAPSHOT_SIZE",
    "CheckpointError",
    "CheckpointManifest",
    "TaskSnapshotDescriptor",
]
