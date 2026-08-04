"""S3 immutable 对象上的 Checkpoint snapshot/decision/finalized 仓库。"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime

from pystream.checkpoint.models import (
    CHECKPOINT_SCHEMA_VERSION,
    DEFAULT_MAX_SNAPSHOT_SIZE,
    CheckpointDecision,
    CheckpointError,
    CheckpointFinalization,
    CheckpointManifest,
    TaskSnapshotDescriptor,
    TransactionDescriptor,
)
from pystream.common import JsonValue
from pystream.storage import (
    ObjectConflict,
    ObjectNotFound,
    ObjectStore,
    ObjectStoreError,
)

_SAFE_JOB_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


class S3CheckpointStore:
    """使用条件创建和内容复验保存不可变 Checkpoint 对象。"""

    def __init__(
        self,
        store: ObjectStore,
        *,
        prefix: str = "pystream",
        max_snapshot_size: int = DEFAULT_MAX_SNAPSHOT_SIZE,
    ) -> None:
        if max_snapshot_size <= 0:
            raise ValueError("max_snapshot_size 必须大于 0")
        self.store = store
        self.prefix = prefix.strip("/")
        if not self.prefix:
            raise ValueError("checkpoint prefix 不能为空")
        self.max_snapshot_size = max_snapshot_size

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
    ) -> TaskSnapshotDescriptor:
        """条件创建 Task snapshot，并把 transaction 纳入内容摘要。"""
        key = self._task_key(job_id, checkpoint_id, attempt_id, task_id)
        if not isinstance(operator_id, str) or not operator_id:
            raise CheckpointError("operator_id 必须是非空字符串")
        if not isinstance(state, dict) or not all(isinstance(name, str) for name in state):
            raise CheckpointError("Task state 必须是字符串键 JSON object")
        document = {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "job_id": job_id,
            "checkpoint_id": checkpoint_id,
            "attempt_id": attempt_id,
            "coordinator_epoch": coordinator_epoch,
            "task_id": task_id,
            "operator_id": operator_id,
            "state": state,
            "transactions": [item.to_dict() for item in transactions],
        }
        content = _canonical_json(document)
        if len(content) > self.max_snapshot_size:
            raise CheckpointError("Task snapshot 超过大小上限")
        descriptor = TaskSnapshotDescriptor(
            job_id=job_id,
            checkpoint_id=checkpoint_id,
            attempt_id=attempt_id,
            coordinator_epoch=coordinator_epoch,
            task_id=task_id,
            operator_id=operator_id,
            relative_path=key,
            sha256=hashlib.sha256(content).hexdigest(),
            size=len(content),
            transactions=transactions,
        )
        descriptor = TaskSnapshotDescriptor.from_dict(descriptor.to_dict())
        self._put_immutable(key, content, "Task snapshot")
        self.read_task_snapshot(descriptor)
        return descriptor

    def read_task_snapshot(
        self,
        descriptor: TaskSnapshotDescriptor,
    ) -> dict[str, JsonValue]:
        """读取并复验 Task snapshot identity、摘要和 transaction。"""
        descriptor = TaskSnapshotDescriptor.from_dict(descriptor.to_dict())
        expected_key = self._task_key(
            descriptor.job_id,
            descriptor.checkpoint_id,
            descriptor.attempt_id,
            descriptor.task_id,
        )
        if descriptor.relative_path != expected_key:
            raise CheckpointError("Task snapshot key 与 descriptor identity 不匹配")
        content = self._get(expected_key, "Task snapshot")
        if (
            len(content) != descriptor.size
            or len(content) > self.max_snapshot_size
            or hashlib.sha256(content).hexdigest() != descriptor.sha256
        ):
            raise CheckpointError("Task snapshot size/SHA-256 不匹配")
        document = _load_json(content, "Task snapshot")
        expected_fields = {
            "schema_version",
            "job_id",
            "checkpoint_id",
            "attempt_id",
            "coordinator_epoch",
            "task_id",
            "operator_id",
            "state",
            "transactions",
        }
        if not isinstance(document, dict) or set(document) != expected_fields:
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
        if identity != (
            descriptor.job_id,
            descriptor.checkpoint_id,
            descriptor.attempt_id,
            descriptor.coordinator_epoch,
            descriptor.task_id,
            descriptor.operator_id,
        ):
            raise CheckpointError("Task snapshot 内容 identity 不匹配")
        raw_transactions = document["transactions"]
        if (
            not isinstance(raw_transactions, list)
            or tuple(TransactionDescriptor.from_dict(item) for item in raw_transactions)
            != descriptor.transactions
        ):
            raise CheckpointError("Task snapshot transaction 不匹配")
        state = document["state"]
        if not isinstance(state, dict) or not all(isinstance(name, str) for name in state):
            raise CheckpointError("Task snapshot state 非法")
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
        """为 At-least-once 路径条件创建 manifest。"""
        sorted_snapshots = self._validate_snapshot_set(
            job_id,
            checkpoint_id,
            attempt_id,
            coordinator_epoch,
            expected_task_ids,
            snapshots,
        )
        manifest = CheckpointManifest(
            job_id=job_id,
            checkpoint_id=checkpoint_id,
            attempt_id=attempt_id,
            coordinator_epoch=coordinator_epoch,
            created_at=(created_at or datetime.now(UTC)).astimezone(UTC),
            snapshots=sorted_snapshots,
        )
        key = self._checkpoint_key(job_id, checkpoint_id, "manifest.json")
        if not self._try_create(
            key,
            _canonical_json(manifest.to_dict()),
            "manifest",
        ):
            existing = self.read_manifest(
                job_id,
                checkpoint_id,
                expected_task_ids=expected_task_ids,
            )
            if (
                existing.attempt_id != attempt_id
                or existing.coordinator_epoch != coordinator_epoch
                or existing.snapshots != sorted_snapshots
                or (created_at is not None and existing.created_at != manifest.created_at)
            ):
                raise CheckpointError("Checkpoint manifest 不可覆盖")
            return existing
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
        manifest = CheckpointManifest.from_dict(
            _load_json(
                self._get(
                    self._checkpoint_key(job_id, checkpoint_id, "manifest.json"),
                    "Checkpoint manifest",
                ),
                "Checkpoint manifest",
            )
        )
        self._validate_read_identity(
            manifest.job_id,
            manifest.checkpoint_id,
            job_id,
            checkpoint_id,
        )
        if (
            expected_task_ids is not None
            and {item.task_id for item in manifest.snapshots} != expected_task_ids
        ):
            raise CheckpointError("Checkpoint manifest Task 集合不匹配")
        for descriptor in manifest.snapshots:
            self.read_task_snapshot(descriptor)
        return manifest

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
    ) -> CheckpointDecision:
        """验证 Task/transaction 全集后条件创建 decision。"""
        sorted_snapshots = self._validate_snapshot_set(
            job_id,
            checkpoint_id,
            attempt_id,
            coordinator_epoch,
            expected_task_ids,
            snapshots,
        )
        transactions = tuple(
            transaction for snapshot in sorted_snapshots for transaction in snapshot.transactions
        )
        task_ids = [item.task_id for item in transactions]
        if set(task_ids) != expected_transaction_task_ids or len(task_ids) != len(
            expected_transaction_task_ids
        ):
            raise CheckpointError("decision transaction 集合与 Sink Task 不匹配")
        decision = CheckpointDecision(
            job_id=job_id,
            checkpoint_id=checkpoint_id,
            attempt_id=attempt_id,
            coordinator_epoch=coordinator_epoch,
            decided_at=(decided_at or datetime.now(UTC)).astimezone(UTC),
            snapshots=sorted_snapshots,
        )
        key = self._checkpoint_key(job_id, checkpoint_id, "decision.json")
        if not self._try_create(
            key,
            _canonical_json(decision.to_dict()),
            "decision",
        ):
            existing = self.read_decision(
                job_id,
                checkpoint_id,
                expected_task_ids=expected_task_ids,
                expected_transaction_task_ids=expected_transaction_task_ids,
            )
            if (
                existing.attempt_id != attempt_id
                or existing.coordinator_epoch != coordinator_epoch
                or existing.snapshots != sorted_snapshots
                or (decided_at is not None and existing.decided_at != decision.decided_at)
            ):
                raise CheckpointError("Checkpoint decision 不可覆盖")
            return existing
        return self.read_decision(
            job_id,
            checkpoint_id,
            expected_task_ids=expected_task_ids,
            expected_transaction_task_ids=expected_transaction_task_ids,
        )

    def read_decision(
        self,
        job_id: str,
        checkpoint_id: int,
        *,
        expected_task_ids: set[str] | None = None,
        expected_transaction_task_ids: set[str] | None = None,
    ) -> CheckpointDecision:
        decision = CheckpointDecision.from_dict(
            _load_json(
                self._get(
                    self._checkpoint_key(job_id, checkpoint_id, "decision.json"),
                    "Checkpoint decision",
                ),
                "Checkpoint decision",
            )
        )
        self._validate_read_identity(
            decision.job_id,
            decision.checkpoint_id,
            job_id,
            checkpoint_id,
        )
        task_ids = {item.task_id for item in decision.snapshots}
        if expected_task_ids is not None and task_ids != expected_task_ids:
            raise CheckpointError("Checkpoint decision Task 集合不匹配")
        transaction_task_ids = [
            transaction.task_id
            for snapshot in decision.snapshots
            for transaction in snapshot.transactions
        ]
        if expected_transaction_task_ids is not None and (
            set(transaction_task_ids) != expected_transaction_task_ids
            or len(transaction_task_ids) != len(expected_transaction_task_ids)
        ):
            raise CheckpointError("Checkpoint decision transaction 集合不匹配")
        for descriptor in decision.snapshots:
            self.read_task_snapshot(descriptor)
        return decision

    def finalize_checkpoint(
        self,
        decision: CheckpointDecision,
        *,
        output_manifests: tuple[str, ...],
        finalized_at: datetime | None = None,
    ) -> CheckpointFinalization:
        persisted = self.read_decision(decision.job_id, decision.checkpoint_id)
        if persisted != decision:
            raise CheckpointError("finalize decision 与持久化内容不一致")
        decision_sha256 = hashlib.sha256(_canonical_json(decision.to_dict())).hexdigest()
        finalization = CheckpointFinalization(
            job_id=decision.job_id,
            checkpoint_id=decision.checkpoint_id,
            attempt_id=decision.attempt_id,
            coordinator_epoch=decision.coordinator_epoch,
            finalized_at=(finalized_at or datetime.now(UTC)).astimezone(UTC),
            decision_sha256=decision_sha256,
            output_manifests=output_manifests,
        )
        key = self._checkpoint_key(
            decision.job_id,
            decision.checkpoint_id,
            "finalized.json",
        )
        if not self._try_create(
            key,
            _canonical_json(finalization.to_dict()),
            "finalization",
        ):
            existing = self.read_finalization(
                decision.job_id,
                decision.checkpoint_id,
            )
            if (
                existing.decision_sha256 != decision_sha256
                or existing.output_manifests != output_manifests
                or (finalized_at is not None and existing.finalized_at != finalization.finalized_at)
            ):
                raise CheckpointError("Checkpoint finalization 不可覆盖")
            return existing
        return self.read_finalization(decision.job_id, decision.checkpoint_id)

    def read_finalization(
        self,
        job_id: str,
        checkpoint_id: int,
    ) -> CheckpointFinalization:
        finalization = CheckpointFinalization.from_dict(
            _load_json(
                self._get(
                    self._checkpoint_key(job_id, checkpoint_id, "finalized.json"),
                    "Checkpoint finalization",
                ),
                "Checkpoint finalization",
            )
        )
        decision = self.read_decision(job_id, checkpoint_id)
        expected_sha256 = hashlib.sha256(_canonical_json(decision.to_dict())).hexdigest()
        if (
            finalization.job_id != job_id
            or finalization.checkpoint_id != checkpoint_id
            or finalization.attempt_id != decision.attempt_id
            or finalization.coordinator_epoch != decision.coordinator_epoch
            or finalization.decision_sha256 != expected_sha256
        ):
            raise CheckpointError("Checkpoint finalization identity/SHA 不匹配")
        return finalization

    def unfinalized_decisions(self, job_id: str) -> tuple[CheckpointDecision, ...]:
        prefix = self._job_checkpoint_prefix(job_id)
        decisions: list[CheckpointDecision] = []
        for key in self._list(prefix):
            if not key.endswith("/decision.json"):
                continue
            checkpoint_id = _checkpoint_id_from_key(prefix, key)
            if not self._exists(self._checkpoint_key(job_id, checkpoint_id, "finalized.json")):
                decisions.append(self.read_decision(job_id, checkpoint_id))
        return tuple(sorted(decisions, key=lambda item: item.checkpoint_id))

    def has_decision(self, job_id: str, checkpoint_id: int) -> bool:
        return self._exists(self._checkpoint_key(job_id, checkpoint_id, "decision.json"))

    def latest_manifest(
        self,
        job_id: str,
        *,
        expected_task_ids: set[str] | None = None,
    ) -> CheckpointManifest | None:
        prefix = self._job_checkpoint_prefix(job_id)
        checkpoint_ids = {
            _checkpoint_id_from_key(prefix, key)
            for key in self._list(prefix)
            if key.endswith("/decision.json") or key.endswith("/manifest.json")
        }
        for checkpoint_id in sorted(checkpoint_ids, reverse=True):
            try:
                decision = self.read_decision(
                    job_id,
                    checkpoint_id,
                    expected_task_ids=expected_task_ids,
                )
                return CheckpointManifest(
                    job_id=decision.job_id,
                    checkpoint_id=decision.checkpoint_id,
                    attempt_id=decision.attempt_id,
                    coordinator_epoch=decision.coordinator_epoch,
                    created_at=decision.decided_at,
                    snapshots=decision.snapshots,
                )
            except CheckpointError:
                pass
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
        """对象快照不可变；无 decision/manifest 的 attempt 保持不可达。"""
        self._task_prefix(job_id, checkpoint_id, attempt_id)

    def _validate_snapshot_set(
        self,
        job_id: str,
        checkpoint_id: int,
        attempt_id: int,
        coordinator_epoch: int,
        expected_task_ids: set[str],
        snapshots: tuple[TaskSnapshotDescriptor, ...],
    ) -> tuple[TaskSnapshotDescriptor, ...]:
        if not expected_task_ids or {item.task_id for item in snapshots} != expected_task_ids:
            raise CheckpointError("Task snapshot 集合与执行图不一致")
        for descriptor in snapshots:
            if (
                descriptor.job_id != job_id
                or descriptor.checkpoint_id != checkpoint_id
                or descriptor.attempt_id != attempt_id
                or descriptor.coordinator_epoch != coordinator_epoch
            ):
                raise CheckpointError("Task snapshot identity 不属于当前 Checkpoint")
            self.read_task_snapshot(descriptor)
        return tuple(sorted(snapshots, key=lambda item: item.task_id))

    def _task_key(
        self,
        job_id: str,
        checkpoint_id: int,
        attempt_id: int,
        task_id: str,
    ) -> str:
        if not isinstance(task_id, str) or not task_id:
            raise CheckpointError("task_id 必须是非空字符串")
        digest = hashlib.sha256(task_id.encode()).hexdigest()
        return f"{self._task_prefix(job_id, checkpoint_id, attempt_id)}/{digest}.json"

    def _task_prefix(self, job_id: str, checkpoint_id: int, attempt_id: int) -> str:
        _require_non_negative(attempt_id, "attempt_id")
        return (
            f"{self._job_checkpoint_prefix(job_id)}{_checkpoint(checkpoint_id)}/"
            f"attempts/{attempt_id:08d}/tasks"
        )

    def _checkpoint_key(self, job_id: str, checkpoint_id: int, name: str) -> str:
        return f"{self._job_checkpoint_prefix(job_id)}{_checkpoint(checkpoint_id)}/{name}"

    def _job_checkpoint_prefix(self, job_id: str) -> str:
        if _SAFE_JOB_ID.fullmatch(job_id) is None:
            raise CheckpointError("job_id 非法")
        return f"{self.prefix}/checkpoints/{job_id}/"

    def _put_immutable(self, key: str, content: bytes, context: str) -> None:
        try:
            self.store.put_if_absent(key, content)
        except ObjectStoreError as exc:
            try:
                existing = self.store.get(key).content
            except ObjectStoreError:
                raise CheckpointError(f"{context} 写入失败: {exc}") from exc
            if existing != content:
                raise CheckpointError(f"{context} immutable object 内容冲突") from exc

    def _try_create(self, key: str, content: bytes, context: str) -> bool:
        try:
            self.store.put_if_absent(key, content)
            return True
        except ObjectConflict:
            return False
        except ObjectStoreError as exc:
            try:
                self.store.get(key)
            except ObjectStoreError:
                raise CheckpointError(f"{context} 写入失败: {exc}") from exc
            return False

    def _get(self, key: str, context: str) -> bytes:
        try:
            return self.store.get(key).content
        except ObjectNotFound as exc:
            raise CheckpointError(f"{context} 不存在") from exc
        except ObjectStoreError as exc:
            raise CheckpointError(f"{context} 读取失败: {exc}") from exc

    def _exists(self, key: str) -> bool:
        try:
            self.store.get(key)
            return True
        except ObjectNotFound:
            return False
        except ObjectStoreError as exc:
            raise CheckpointError(f"检查对象存在性失败: {exc}") from exc

    def _list(self, prefix: str) -> tuple[str, ...]:
        try:
            return self.store.list_keys(prefix)
        except ObjectStoreError as exc:
            raise CheckpointError(f"列出 Checkpoint 对象失败: {exc}") from exc

    @staticmethod
    def _validate_read_identity(
        actual_job_id: str,
        actual_checkpoint_id: int,
        expected_job_id: str,
        expected_checkpoint_id: int,
    ) -> None:
        if actual_job_id != expected_job_id or actual_checkpoint_id != expected_checkpoint_id:
            raise CheckpointError("Checkpoint key 与内容 identity 不匹配")


def _checkpoint(checkpoint_id: int) -> str:
    _require_non_negative(checkpoint_id, "checkpoint_id")
    return f"{checkpoint_id:020d}"


def _require_non_negative(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CheckpointError(f"{field} 必须是非负整数")
    return value


def _checkpoint_id_from_key(prefix: str, key: str) -> int:
    relative = key.removeprefix(prefix)
    raw = relative.split("/", 1)[0]
    if len(raw) != 20 or not raw.isdigit():
        raise CheckpointError(f"Checkpoint object key 非法: {key}")
    return int(raw)


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
        raise CheckpointError(f"Checkpoint 必须可严格 JSON 序列化: {exc}") from exc


def _load_json(content: bytes, context: str) -> object:
    try:
        return json.loads(content.decode())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CheckpointError(f"{context} 不是合法 UTF-8 JSON") from exc


__all__ = ["S3CheckpointStore"]
