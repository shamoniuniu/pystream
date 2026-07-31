"""共享文件系统上的原子 Task 快照与 manifest-last Checkpoint Store。"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path

from pystream.checkpoint.models import (
    CHECKPOINT_SCHEMA_VERSION,
    DEFAULT_MAX_SNAPSHOT_SIZE,
    CheckpointError,
    CheckpointManifest,
    TaskSnapshotDescriptor,
)
from pystream.common import JsonValue

_SAFE_JOB_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_CHECKPOINT_DIRECTORY = re.compile(r"^checkpoint-(?P<id>[0-9]{20})$")


def _canonical_json(document: object) -> bytes:
    try:
        return json.dumps(
            document,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise CheckpointError(f"Checkpoint 状态必须可严格 JSON 序列化: {exc}") from exc


def _load_json(content: bytes, context: str) -> object:
    try:
        return json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CheckpointError(f"{context} 不是合法 UTF-8 JSON: {exc}") from exc


class LocalCheckpointStore:
    """使用共享命名卷保存不可变 Checkpoint。"""

    def __init__(
        self,
        root: str | Path,
        *,
        max_snapshot_size: int = DEFAULT_MAX_SNAPSHOT_SIZE,
    ) -> None:
        if max_snapshot_size <= 0:
            raise ValueError("max_snapshot_size 必须大于 0")
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.max_snapshot_size = max_snapshot_size

    @staticmethod
    def _validate_job_id(job_id: str) -> str:
        if _SAFE_JOB_ID.fullmatch(job_id) is None:
            raise CheckpointError("job_id 只能包含字母、数字、下划线和连字符")
        return job_id

    def _job_root(self, job_id: str) -> Path:
        self._validate_job_id(job_id)
        path = (self.root / job_id).resolve()
        if self.root not in path.parents:
            raise CheckpointError("Checkpoint job 路径越过存储根目录")
        return path

    def _checkpoint_root(self, job_id: str, checkpoint_id: int) -> Path:
        if isinstance(checkpoint_id, bool) or not isinstance(checkpoint_id, int):
            raise CheckpointError("checkpoint_id 必须是非负整数")
        if checkpoint_id < 0:
            raise CheckpointError("checkpoint_id 必须是非负整数")
        return self._job_root(job_id) / f"checkpoint-{checkpoint_id:020d}"

    def _snapshot_path(
        self,
        job_id: str,
        checkpoint_id: int,
        attempt_id: int,
        task_id: str,
    ) -> Path:
        if isinstance(attempt_id, bool) or not isinstance(attempt_id, int) or attempt_id < 0:
            raise CheckpointError("attempt_id 必须是非负整数")
        if not isinstance(task_id, str) or not task_id:
            raise CheckpointError("task_id 必须是非空字符串")
        task_digest = hashlib.sha256(task_id.encode("utf-8")).hexdigest()
        return (
            self._checkpoint_root(job_id, checkpoint_id)
            / f"attempt-{attempt_id:08d}"
            / "tasks"
            / f"{task_digest}.json"
        )

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
    ) -> TaskSnapshotDescriptor:
        """原子写入一个当前 attempt 的 Task 状态。"""
        self._validate_job_id(job_id)
        if (
            isinstance(coordinator_epoch, bool)
            or not isinstance(coordinator_epoch, int)
            or coordinator_epoch < 0
        ):
            raise CheckpointError("coordinator_epoch 必须是非负整数")
        if not isinstance(operator_id, str) or not operator_id:
            raise CheckpointError("operator_id 必须是非空字符串")
        if not isinstance(state, dict) or not all(isinstance(key, str) for key in state):
            raise CheckpointError("Task state 必须是字符串键的 JSON object")
        document = {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "job_id": job_id,
            "checkpoint_id": checkpoint_id,
            "attempt_id": attempt_id,
            "coordinator_epoch": coordinator_epoch,
            "task_id": task_id,
            "operator_id": operator_id,
            "state": state,
        }
        content = _canonical_json(document)
        if len(content) > self.max_snapshot_size:
            raise CheckpointError(
                f"Task snapshot {len(content)} bytes 超过上限 {self.max_snapshot_size}"
            )
        target = self._snapshot_path(job_id, checkpoint_id, attempt_id, task_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            existing = target.read_bytes()
            if existing != content:
                raise CheckpointError("同一 Task/Checkpoint/attempt 快照不可覆盖")
        else:
            self._atomic_write(target, content)
        descriptor = TaskSnapshotDescriptor(
            job_id=job_id,
            checkpoint_id=checkpoint_id,
            attempt_id=attempt_id,
            task_id=task_id,
            operator_id=operator_id,
            relative_path=target.relative_to(self.root).as_posix(),
            sha256=hashlib.sha256(content).hexdigest(),
            size=len(content),
            coordinator_epoch=coordinator_epoch,
        )
        self.read_task_snapshot(descriptor)
        return descriptor

    def read_task_snapshot(
        self,
        descriptor: TaskSnapshotDescriptor,
    ) -> dict[str, JsonValue]:
        """复验 descriptor 并返回 Task state。"""
        expected = self._snapshot_path(
            descriptor.job_id,
            descriptor.checkpoint_id,
            descriptor.attempt_id,
            descriptor.task_id,
        )
        if descriptor.relative_path != expected.relative_to(self.root).as_posix():
            raise CheckpointError("Task snapshot relative_path 与身份不匹配")
        if expected.is_symlink() or not expected.is_file():
            raise CheckpointError(f"Task snapshot 不存在或不是普通文件: {expected}")
        content = expected.read_bytes()
        if len(content) != descriptor.size:
            raise CheckpointError("Task snapshot 大小不匹配")
        if len(content) > self.max_snapshot_size:
            raise CheckpointError("Task snapshot 超过读取上限")
        if hashlib.sha256(content).hexdigest() != descriptor.sha256:
            raise CheckpointError("Task snapshot SHA-256 不匹配")
        document = _load_json(content, "Task snapshot")
        if not isinstance(document, dict):
            raise CheckpointError("Task snapshot 必须是 JSON object")
        expected_fields = {
            "schema_version",
            "job_id",
            "checkpoint_id",
            "attempt_id",
            "coordinator_epoch",
            "task_id",
            "operator_id",
            "state",
        }
        if set(document) != expected_fields:
            raise CheckpointError("Task snapshot 字段集合不匹配")
        if document["schema_version"] != CHECKPOINT_SCHEMA_VERSION:
            raise CheckpointError("Task snapshot schema 不兼容")
        identity = (
            document["job_id"],
            document["checkpoint_id"],
            document["attempt_id"],
            document["coordinator_epoch"],
            document["task_id"],
            document["operator_id"],
        )
        expected_identity = (
            descriptor.job_id,
            descriptor.checkpoint_id,
            descriptor.attempt_id,
            descriptor.coordinator_epoch,
            descriptor.task_id,
            descriptor.operator_id,
        )
        if identity != expected_identity:
            raise CheckpointError("Task snapshot 内容身份与 descriptor 不一致")
        state = document["state"]
        if not isinstance(state, dict) or not all(isinstance(key, str) for key in state):
            raise CheckpointError("Task snapshot state 必须是 JSON object")
        return state

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
    ) -> CheckpointManifest:
        """验证任务全集后最后写入不可变 manifest。"""
        if (
            isinstance(coordinator_epoch, bool)
            or not isinstance(coordinator_epoch, int)
            or coordinator_epoch < 0
        ):
            raise CheckpointError("coordinator_epoch 必须是非负整数")
        if not expected_task_ids:
            raise CheckpointError("expected_task_ids 不能为空")
        if {item.task_id for item in snapshots} != expected_task_ids:
            raise CheckpointError("Task snapshot 集合与执行图不一致")
        for descriptor in snapshots:
            if (
                descriptor.job_id != job_id
                or descriptor.checkpoint_id != checkpoint_id
                or descriptor.attempt_id != attempt_id
                or descriptor.coordinator_epoch != coordinator_epoch
            ):
                raise CheckpointError("Task snapshot descriptor 不属于当前 Checkpoint")
            self.read_task_snapshot(descriptor)
        sorted_snapshots = tuple(sorted(snapshots, key=lambda item: item.task_id))
        target = self._checkpoint_root(job_id, checkpoint_id) / "manifest.json"
        if target.exists():
            existing = self.read_manifest(
                job_id,
                checkpoint_id,
                expected_task_ids=expected_task_ids,
            )
            if existing.attempt_id != attempt_id or existing.snapshots != sorted_snapshots:
                raise CheckpointError("已完成 Checkpoint manifest 不可覆盖")
            if created_at is not None and existing.created_at != created_at.astimezone(UTC):
                raise CheckpointError("已完成 Checkpoint manifest 不可覆盖")
            return existing
        manifest = CheckpointManifest(
            job_id=job_id,
            checkpoint_id=checkpoint_id,
            attempt_id=attempt_id,
            coordinator_epoch=coordinator_epoch,
            created_at=(created_at or datetime.now(UTC)).astimezone(UTC),
            snapshots=sorted_snapshots,
        )
        content = _canonical_json(manifest.to_dict())
        target.parent.mkdir(parents=True, exist_ok=True)
        self._atomic_write(target, content)
        return self.read_manifest(
            job_id,
            checkpoint_id,
            expected_task_ids=expected_task_ids,
        )

    def read_manifest(
        self,
        job_id: str,
        checkpoint_id: int,
        *,
        expected_task_ids: set[str] | None = None,
    ) -> CheckpointManifest:
        """读取 manifest 并复验其全部 Task snapshot。"""
        path = self._checkpoint_root(job_id, checkpoint_id) / "manifest.json"
        if path.is_symlink() or not path.is_file():
            raise CheckpointError("Checkpoint manifest 不存在或不是普通文件")
        manifest = CheckpointManifest.from_dict(
            _load_json(path.read_bytes(), "Checkpoint manifest")
        )
        if manifest.job_id != job_id or manifest.checkpoint_id != checkpoint_id:
            raise CheckpointError("Checkpoint manifest 路径与内容身份不一致")
        task_ids = {item.task_id for item in manifest.snapshots}
        if expected_task_ids is not None and task_ids != expected_task_ids:
            raise CheckpointError("Checkpoint manifest 任务集合与执行图不一致")
        for descriptor in manifest.snapshots:
            self.read_task_snapshot(descriptor)
        return manifest

    def latest_manifest(
        self,
        job_id: str,
        *,
        expected_task_ids: set[str] | None = None,
    ) -> CheckpointManifest | None:
        """返回最高合法完整 Checkpoint，跳过损坏或不完整目录。"""
        root = self._job_root(job_id)
        if not root.is_dir():
            return None
        candidates: list[int] = []
        for path in root.iterdir():
            match = _CHECKPOINT_DIRECTORY.fullmatch(path.name)
            if path.is_dir() and not path.is_symlink() and match is not None:
                candidates.append(int(match.group("id")))
        for checkpoint_id in sorted(candidates, reverse=True):
            try:
                return self.read_manifest(
                    job_id,
                    checkpoint_id,
                    expected_task_ids=expected_task_ids,
                )
            except CheckpointError:
                continue
        return None

    def abort_checkpoint(
        self,
        job_id: str,
        checkpoint_id: int,
        attempt_id: int,
    ) -> None:
        """删除未完成 attempt；已完成 manifest 保持不可变。"""
        checkpoint_root = self._checkpoint_root(job_id, checkpoint_id)
        if (checkpoint_root / "manifest.json").exists():
            return
        attempt_root = checkpoint_root / f"attempt-{attempt_id:08d}"
        if attempt_root.is_symlink():
            raise CheckpointError("拒绝删除符号链接 Checkpoint attempt")
        shutil.rmtree(attempt_root, ignore_errors=True)
        if checkpoint_root.is_dir() and not any(checkpoint_root.iterdir()):
            checkpoint_root.rmdir()

    @staticmethod
    def _atomic_write(target: Path, content: bytes) -> None:
        temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
        try:
            with temporary.open("xb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)


__all__ = ["LocalCheckpointStore"]
