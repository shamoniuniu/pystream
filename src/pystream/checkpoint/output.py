"""事务文件 Sink 的 committed fragment 校验与 manifest-last 发布。"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from pystream.checkpoint.models import (
    CHECKPOINT_SCHEMA_VERSION,
    CheckpointDecision,
    CheckpointError,
    TransactionDescriptor,
)


@dataclass(frozen=True, slots=True)
class _CommittedFragment:
    descriptor: TransactionDescriptor
    root: Path
    path: Path
    relative_path: str


class LocalFileOutputCommitter:
    """复验全部事务分片后，按 Sink 原子发布 checkpoint manifest。"""

    def publish(
        self,
        decision: CheckpointDecision,
        *,
        output_roots: dict[str, str | Path],
    ) -> tuple[str, ...]:
        """发布 manifest；目标已存在且内容相同时视为幂等成功。"""
        transactions = tuple(
            transaction for snapshot in decision.snapshots for transaction in snapshot.transactions
        )
        if not transactions:
            raise CheckpointError("Exactly-once decision 不包含 transaction")
        fragments = tuple(
            self._resolve_committed(transaction, output_roots) for transaction in transactions
        )
        for fragment in fragments:
            _verify_file(fragment.path, fragment.descriptor)

        grouped: dict[str, list[_CommittedFragment]] = defaultdict(list)
        for fragment in fragments:
            grouped[fragment.descriptor.operator_id].append(fragment)
        manifests: list[str] = []
        for operator_id in sorted(grouped):
            operator_fragments = sorted(
                grouped[operator_id],
                key=lambda item: item.descriptor.task_id,
            )
            root = operator_fragments[0].root
            target = (
                root
                / decision.job_id
                / operator_id
                / "manifests"
                / f"checkpoint-{decision.checkpoint_id:020d}.json"
            )
            document = {
                "schema_version": CHECKPOINT_SCHEMA_VERSION,
                "job_id": decision.job_id,
                "checkpoint_id": decision.checkpoint_id,
                "attempt_id": decision.attempt_id,
                "coordinator_epoch": decision.coordinator_epoch,
                "operator_id": operator_id,
                "fragments": [
                    {
                        "task_id": fragment.descriptor.task_id,
                        "transaction_id": fragment.descriptor.transaction_id,
                        "relative_path": fragment.relative_path,
                        "sha256": fragment.descriptor.sha256,
                        "size": fragment.descriptor.size,
                    }
                    for fragment in operator_fragments
                ],
            }
            content = _canonical_json(document)
            target.parent.mkdir(parents=True, exist_ok=True)
            _atomic_write_idempotent(target, content)
            manifests.append(target.resolve().as_posix())
        return tuple(manifests)

    def finalize_transactions(
        self,
        decision: CheckpointDecision,
        *,
        output_roots: dict[str, str | Path],
    ) -> None:
        """接管恢复时按 descriptor 幂等完成 DECIDED fragments。"""
        for snapshot in decision.snapshots:
            for descriptor in snapshot.transactions:
                committed = self._resolve_committed(descriptor, output_roots)
                pending = self._resolve_pending(descriptor, committed.root)
                committed.path.parent.mkdir(parents=True, exist_ok=True)
                if committed.path.exists():
                    _verify_file(committed.path, descriptor)
                    if pending.exists():
                        _verify_file(pending, descriptor)
                        pending.unlink()
                    _cleanup_empty_transaction_directories(pending)
                    continue
                _verify_file(pending, descriptor)
                try:
                    os.replace(pending, committed.path)
                except OSError as exc:
                    raise CheckpointError(
                        f"恢复提交 transaction {descriptor.transaction_id} 失败: {exc}"
                    ) from exc
                _verify_file(committed.path, descriptor)
                _cleanup_empty_transaction_directories(pending)

    def cleanup_orphans(
        self,
        *,
        job_id: str,
        output_roots: dict[str, str | Path],
        protected_pending_paths: set[str],
    ) -> int:
        """在 Task 停止期间清理未被任何 durable decision 引用的 pending 目录。"""
        removed = 0
        for operator_id, raw_root in sorted(output_roots.items()):
            root = Path(raw_root).resolve()
            pending_root = root / job_id / operator_id / "pending"
            if not pending_root.exists():
                continue
            if pending_root.is_symlink() or not pending_root.is_dir():
                raise CheckpointError(f"Sink pending 路径不是普通目录: {pending_root}")
            for transaction_root in sorted(pending_root.glob("attempt-*/tx-*")):
                if transaction_root.is_symlink() or not transaction_root.is_dir():
                    raise CheckpointError(f"拒绝清理异常 transaction 路径: {transaction_root}")
                fragment_paths = tuple(transaction_root.glob("part-*.csv"))
                protected = any(
                    path.relative_to(root).as_posix() in protected_pending_paths
                    for path in fragment_paths
                )
                if protected:
                    continue
                shutil.rmtree(transaction_root)
                removed += 1
            for attempt_root in sorted(pending_root.glob("attempt-*"), reverse=True):
                if (
                    attempt_root.is_dir()
                    and not attempt_root.is_symlink()
                    and not any(attempt_root.iterdir())
                ):
                    attempt_root.rmdir()
            if not any(pending_root.iterdir()):
                pending_root.rmdir()
        return removed

    @staticmethod
    def _resolve_committed(
        descriptor: TransactionDescriptor,
        output_roots: dict[str, str | Path],
    ) -> _CommittedFragment:
        try:
            root = Path(output_roots[descriptor.operator_id]).resolve()
        except KeyError as exc:
            raise CheckpointError(f"Sink {descriptor.operator_id!r} 缺少 output root") from exc
        subtask_index = _subtask_index(descriptor)
        relative = (
            PurePosixPath(descriptor.job_id)
            / descriptor.operator_id
            / "committed"
            / f"checkpoint-{descriptor.checkpoint_id:020d}"
            / f"part-{subtask_index:05d}.csv"
        )
        path = (root / Path(*relative.parts)).resolve()
        if not path.is_relative_to(root):
            raise CheckpointError("committed fragment 越过 output root")
        return _CommittedFragment(
            descriptor=descriptor,
            root=root,
            path=path,
            relative_path=relative.as_posix(),
        )

    @staticmethod
    def _resolve_pending(descriptor: TransactionDescriptor, root: Path) -> Path:
        subtask_index = _subtask_index(descriptor)
        expected = (
            PurePosixPath(descriptor.job_id)
            / descriptor.operator_id
            / "pending"
            / f"attempt-{descriptor.attempt_id:08d}"
            / f"tx-{descriptor.transaction_id}"
            / f"part-{subtask_index:05d}.csv"
        )
        if PurePosixPath(descriptor.pending_path) != expected:
            raise CheckpointError("transaction pending_path 与 descriptor 身份不匹配")
        path = (root / Path(*expected.parts)).resolve()
        if not path.is_relative_to(root):
            raise CheckpointError("pending fragment 越过 output root")
        return path


def _subtask_index(descriptor: TransactionDescriptor) -> int:
    prefix = f"{descriptor.job_id}:{descriptor.operator_id}:"
    if not descriptor.task_id.startswith(prefix):
        raise CheckpointError("transaction task_id 与 job/operator 身份不匹配")
    raw_subtask = descriptor.task_id.removeprefix(prefix)
    if not raw_subtask.isdigit():
        raise CheckpointError("transaction task_id 缺少合法 subtask")
    return int(raw_subtask)


def _verify_file(path: Path, descriptor: TransactionDescriptor) -> None:
    if path.is_symlink() or not path.is_file():
        raise CheckpointError(f"transaction fragment 不存在或不是普通文件: {path}")
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                size += len(chunk)
                digest.update(chunk)
    except OSError as exc:
        raise CheckpointError(f"读取 transaction fragment {path} 失败: {exc}") from exc
    actual_sha256 = digest.hexdigest()
    if size != descriptor.size or actual_sha256 != descriptor.sha256:
        raise CheckpointError(
            f"transaction fragment 完整性不匹配: {path}; "
            f"expected={descriptor.size}/{descriptor.sha256}, "
            f"actual={size}/{actual_sha256}"
        )


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
        raise CheckpointError(f"output manifest 必须可严格 JSON 序列化: {exc}") from exc


def _atomic_write_idempotent(target: Path, content: bytes) -> None:
    if target.exists():
        if target.is_symlink() or not target.is_file() or target.read_bytes() != content:
            raise CheckpointError(f"output manifest 已存在且内容不匹配: {target}")
        return
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def _cleanup_empty_transaction_directories(path: Path) -> None:
    for directory in (path.parent, path.parent.parent):
        if directory.is_dir() and not directory.is_symlink() and not any(directory.iterdir()):
            directory.rmdir()


__all__ = ["LocalFileOutputCommitter"]
