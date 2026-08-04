"""不可变 Job revision 与 ETag CAS current pointer。"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from pystream.api import DeliveryGuarantee
from pystream.control.models import JobStatus
from pystream.storage import (
    ObjectConflict,
    ObjectNotFound,
    ObjectStore,
    ObjectStoreError,
)

JOB_METADATA_SCHEMA_VERSION = 1
_SAFE_JOB_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class JobMetadataError(RuntimeError):
    """Job metadata 内容或存储操作不符合契约。"""


class JobMetadataConflict(JobMetadataError):
    """current pointer 已被其他写入者推进。"""


@dataclass(frozen=True, slots=True)
class JobMetadataRevision:
    """接管所需的不可变 JobManager 聚合状态。"""

    job_id: str
    revision: int
    definition: dict[str, object]
    artifact_sha256: str
    artifact_size: int
    delivery_guarantee: DeliveryGuarantee
    status: JobStatus
    attempt_id: int
    next_checkpoint_id: int
    last_decided_checkpoint_id: int | None
    last_finalized_checkpoint_id: int | None
    recovery_attempts: int
    last_failure: str | None
    coordinator_epoch: int
    finalize_backlog: tuple[int, ...]
    updated_at: datetime
    schema_version: int = JOB_METADATA_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != JOB_METADATA_SCHEMA_VERSION:
            raise JobMetadataError("Job metadata schema 不兼容")
        if _SAFE_JOB_ID.fullmatch(self.job_id) is None:
            raise JobMetadataError("job_id 只能包含字母、数字、下划线和连字符")
        for field_name, value in (
            ("revision", self.revision),
            ("attempt_id", self.attempt_id),
            ("recovery_attempts", self.recovery_attempts),
            ("coordinator_epoch", self.coordinator_epoch),
        ):
            _require_non_negative(value, field_name)
        if (
            isinstance(self.next_checkpoint_id, bool)
            or not isinstance(self.next_checkpoint_id, int)
            or self.next_checkpoint_id < 1
        ):
            raise JobMetadataError("next_checkpoint_id 必须是正整数")
        if _SHA256.fullmatch(self.artifact_sha256) is None:
            raise JobMetadataError("artifact_sha256 必须是 64 位小写十六进制")
        _require_non_negative(self.artifact_size, "artifact_size")
        if not isinstance(self.definition, dict) or not all(
            isinstance(key, str) for key in self.definition
        ):
            raise JobMetadataError("definition 必须是字符串键 JSON object")
        _canonical_json(self.definition)
        decided = _optional_non_negative(
            self.last_decided_checkpoint_id,
            "last_decided_checkpoint_id",
        )
        finalized = _optional_non_negative(
            self.last_finalized_checkpoint_id,
            "last_finalized_checkpoint_id",
        )
        if finalized is not None and (decided is None or finalized > decided):
            raise JobMetadataError("last_finalized_checkpoint_id 不能超过 last_decided")
        if (
            not isinstance(self.finalize_backlog, tuple)
            or len(set(self.finalize_backlog)) != len(self.finalize_backlog)
            or tuple(sorted(self.finalize_backlog)) != self.finalize_backlog
        ):
            raise JobMetadataError("finalize_backlog 必须是唯一升序 tuple")
        for checkpoint_id in self.finalize_backlog:
            _require_non_negative(checkpoint_id, "finalize_backlog checkpoint")
        if self.updated_at.tzinfo is None or self.updated_at.utcoffset() is None:
            raise JobMetadataError("updated_at 必须包含时区")
        if self.last_failure is not None and not isinstance(self.last_failure, str):
            raise JobMetadataError("last_failure 必须是字符串或 null")

    def to_dict(self) -> dict[str, object]:
        """转换为规范 revision JSON。"""
        return {
            "schema_version": self.schema_version,
            "job_id": self.job_id,
            "revision": self.revision,
            "definition": self.definition,
            "artifact_sha256": self.artifact_sha256,
            "artifact_size": self.artifact_size,
            "delivery_guarantee": self.delivery_guarantee.value,
            "status": self.status.value,
            "attempt_id": self.attempt_id,
            "next_checkpoint_id": self.next_checkpoint_id,
            "last_decided_checkpoint_id": self.last_decided_checkpoint_id,
            "last_finalized_checkpoint_id": self.last_finalized_checkpoint_id,
            "recovery_attempts": self.recovery_attempts,
            "last_failure": self.last_failure,
            "coordinator_epoch": self.coordinator_epoch,
            "finalize_backlog": list(self.finalize_backlog),
            "updated_at": self.updated_at.astimezone(UTC).isoformat(),
        }

    @classmethod
    def from_dict(cls, document: object) -> JobMetadataRevision:
        """从不可信对象恢复严格 revision。"""
        expected = {
            "schema_version",
            "job_id",
            "revision",
            "definition",
            "artifact_sha256",
            "artifact_size",
            "delivery_guarantee",
            "status",
            "attempt_id",
            "next_checkpoint_id",
            "last_decided_checkpoint_id",
            "last_finalized_checkpoint_id",
            "recovery_attempts",
            "last_failure",
            "coordinator_epoch",
            "finalize_backlog",
            "updated_at",
        }
        if not isinstance(document, dict) or set(document) != expected:
            raise JobMetadataError("Job metadata revision 字段集合不匹配")
        try:
            updated_at = datetime.fromisoformat(str(document["updated_at"]).replace("Z", "+00:00"))
            guarantee = DeliveryGuarantee(document["delivery_guarantee"])
            status = JobStatus(document["status"])
        except (TypeError, ValueError) as exc:
            raise JobMetadataError(f"Job metadata enum/time 非法: {exc}") from exc
        backlog = document["finalize_backlog"]
        if not isinstance(backlog, list):
            raise JobMetadataError("finalize_backlog 必须是 array")
        definition = document["definition"]
        if not isinstance(definition, dict):
            raise JobMetadataError("definition 必须是 object")
        return cls(
            schema_version=document["schema_version"],
            job_id=document["job_id"],
            revision=document["revision"],
            definition=definition,
            artifact_sha256=document["artifact_sha256"],
            artifact_size=document["artifact_size"],
            delivery_guarantee=guarantee,
            status=status,
            attempt_id=document["attempt_id"],
            next_checkpoint_id=document["next_checkpoint_id"],
            last_decided_checkpoint_id=document["last_decided_checkpoint_id"],
            last_finalized_checkpoint_id=document["last_finalized_checkpoint_id"],
            recovery_attempts=document["recovery_attempts"],
            last_failure=document["last_failure"],
            coordinator_epoch=document["coordinator_epoch"],
            finalize_backlog=tuple(backlog),
            updated_at=updated_at,
        )


@dataclass(frozen=True, slots=True)
class StoredJobMetadata:
    revision: JobMetadataRevision
    current_etag: str


class JobMetadataRepository(Protocol):
    """JobManager 持久化聚合状态所需端口。"""

    def publish(
        self,
        revision: JobMetadataRevision,
        *,
        expected_current_etag: str | None,
    ) -> StoredJobMetadata: ...

    def read_current(self, job_id: str) -> StoredJobMetadata: ...

    def list_jobs(self) -> tuple[str, ...]: ...


class S3JobMetadataRepository:
    """以 immutable revision + CAS current pointer 保存 Job 状态。"""

    def __init__(self, store: ObjectStore, *, prefix: str = "pystream") -> None:
        self.store = store
        self.prefix = prefix.strip("/")
        if not self.prefix:
            raise ValueError("metadata prefix 不能为空")

    def publish(
        self,
        revision: JobMetadataRevision,
        *,
        expected_current_etag: str | None,
    ) -> StoredJobMetadata:
        """先持久化 revision，再条件推进 current pointer。"""
        content = _canonical_json(revision.to_dict())
        revision_sha256 = hashlib.sha256(content).hexdigest()
        revision_key = self._revision_key(revision.job_id, revision.revision)
        try:
            self.store.put_if_absent(revision_key, content)
        except ObjectStoreError as exc:
            try:
                existing = self.store.get(revision_key)
            except ObjectStoreError:
                raise JobMetadataError(f"写入 Job revision 失败: {exc}") from exc
            if existing.content != content:
                raise JobMetadataConflict("同一 Job revision 已存在不同内容") from exc
        pointer = _canonical_json(
            {
                "schema_version": JOB_METADATA_SCHEMA_VERSION,
                "job_id": revision.job_id,
                "revision": revision.revision,
                "revision_sha256": revision_sha256,
            }
        )
        current_key = self._current_key(revision.job_id)
        try:
            if expected_current_etag is None:
                current_etag = self.store.put_if_absent(current_key, pointer)
            else:
                current_etag = self.store.put_if_match(
                    current_key,
                    pointer,
                    expected_current_etag,
                )
        except ObjectStoreError as exc:
            try:
                observed = self.read_current(revision.job_id)
            except JobMetadataError:
                observed = None
            if observed is not None and observed.revision == revision:
                return observed
            if isinstance(exc, ObjectConflict):
                raise JobMetadataConflict("Job current pointer CAS 冲突") from exc
            raise JobMetadataError(f"推进 Job current pointer 失败: {exc}") from exc
        return StoredJobMetadata(revision=revision, current_etag=current_etag)

    def read_current(self, job_id: str) -> StoredJobMetadata:
        """读取 current 及其 immutable revision，并复验摘要和身份。"""
        current = self._get(self._current_key(job_id))
        pointer = _load_json(current.content, "Job current pointer")
        if not isinstance(pointer, dict) or set(pointer) != {
            "schema_version",
            "job_id",
            "revision",
            "revision_sha256",
        }:
            raise JobMetadataError("Job current pointer 字段集合不匹配")
        if pointer["schema_version"] != JOB_METADATA_SCHEMA_VERSION or pointer["job_id"] != job_id:
            raise JobMetadataError("Job current pointer identity/schema 不匹配")
        revision_number = pointer["revision"]
        _require_non_negative(revision_number, "revision")
        revision_object = self._get(self._revision_key(job_id, revision_number))
        expected_sha256 = pointer["revision_sha256"]
        if (
            not isinstance(expected_sha256, str)
            or hashlib.sha256(revision_object.content).hexdigest() != expected_sha256
        ):
            raise JobMetadataError("Job revision SHA-256 不匹配")
        revision = JobMetadataRevision.from_dict(
            _load_json(revision_object.content, "Job metadata revision")
        )
        if revision.job_id != job_id or revision.revision != revision_number:
            raise JobMetadataError("Job revision identity 不匹配")
        return StoredJobMetadata(revision=revision, current_etag=current.etag)

    def list_jobs(self) -> tuple[str, ...]:
        """列出具有 current pointer 的合法 job_id。"""
        prefix = f"{self.prefix}/jobs/"
        suffix = "/current.json"
        jobs = []
        try:
            keys = self.store.list_keys(prefix)
        except ObjectStoreError as exc:
            raise JobMetadataError(f"列出 Job metadata 失败: {exc}") from exc
        for key in keys:
            if not key.endswith(suffix):
                continue
            job_id = key.removeprefix(prefix).removesuffix(suffix)
            if _SAFE_JOB_ID.fullmatch(job_id) is not None:
                jobs.append(job_id)
        return tuple(sorted(set(jobs)))

    def _revision_key(self, job_id: str, revision: int) -> str:
        if _SAFE_JOB_ID.fullmatch(job_id) is None:
            raise JobMetadataError("job_id 非法")
        _require_non_negative(revision, "revision")
        return f"{self.prefix}/jobs/{job_id}/revisions/{revision:020d}.json"

    def _current_key(self, job_id: str) -> str:
        if _SAFE_JOB_ID.fullmatch(job_id) is None:
            raise JobMetadataError("job_id 非法")
        return f"{self.prefix}/jobs/{job_id}/current.json"

    def _get(self, key: str):
        try:
            return self.store.get(key)
        except ObjectNotFound as exc:
            raise JobMetadataError(f"Job metadata 对象不存在: {key}") from exc
        except ObjectStoreError as exc:
            raise JobMetadataError(f"读取 Job metadata 失败: {exc}") from exc


def _require_non_negative(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise JobMetadataError(f"{field} 必须是非负整数")
    return value


def _optional_non_negative(value: object, field: str) -> int | None:
    if value is None:
        return None
    return _require_non_negative(value, field)


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
        raise JobMetadataError(f"Job metadata 必须可严格 JSON 序列化: {exc}") from exc


def _load_json(content: bytes, context: str) -> Any:
    try:
        return json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JobMetadataError(f"{context} 不是合法 UTF-8 JSON") from exc


__all__ = [
    "JOB_METADATA_SCHEMA_VERSION",
    "JobMetadataConflict",
    "JobMetadataError",
    "JobMetadataRepository",
    "JobMetadataRevision",
    "S3JobMetadataRepository",
    "StoredJobMetadata",
]
