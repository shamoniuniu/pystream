"""JobManager 侧的本地不可变作业制品仓库。

文件以 ``job_id/sha256.zip`` 保存。写入使用同目录临时文件和原子替换，
读取时重新计算摘要，确保 Worker 下载接口不会返回损坏或错配的制品。
"""

from __future__ import annotations

import hashlib
import os
import re
import uuid
from pathlib import Path

from pystream.control.errors import ArtifactError
from pystream.control.models import ArtifactDescriptor

_SAFE_JOB_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class LocalArtifactRepository:
    """基于本地文件系统的不可变制品仓库。"""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _digest(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    def _path(self, job_id: str, sha256: str) -> Path:
        if _SAFE_JOB_ID.fullmatch(job_id) is None:
            raise ArtifactError("job_id 只能包含字母、数字、下划线和连字符")
        if _SHA256.fullmatch(sha256) is None:
            raise ArtifactError("sha256 必须是 64 位小写十六进制")
        path = (self.root / job_id / f"{sha256}.zip").resolve()
        if self.root not in path.parents:
            raise ArtifactError("制品路径越过仓库根目录")
        return path

    def put(
        self,
        job_id: str,
        content: bytes,
        expected_sha256: str | None = None,
    ) -> ArtifactDescriptor:
        """按内容摘要保存制品，相同 job/digest 的重复写入保持幂等。"""
        sha256 = self._digest(content)
        if expected_sha256 is not None and expected_sha256 != sha256:
            raise ArtifactError(f"制品摘要不匹配: 期望 {expected_sha256}, 实际 {sha256}")
        target = self._path(job_id, sha256)
        target.parent.mkdir(parents=True, exist_ok=True)

        if not target.exists():
            temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
            try:
                with temporary.open("xb") as file:
                    file.write(content)
                    file.flush()
                    os.fsync(file.fileno())
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)

        descriptor = ArtifactDescriptor(job_id=job_id, sha256=sha256, size=len(content))
        self.read(descriptor)
        return descriptor

    def read(self, descriptor: ArtifactDescriptor) -> bytes:
        """读取并校验制品大小和 SHA-256。"""
        path = self._path(descriptor.job_id, descriptor.sha256)
        try:
            content = path.read_bytes()
        except OSError as exc:
            raise ArtifactError(f"无法读取制品 {path}: {exc}") from exc
        if len(content) != descriptor.size:
            raise ArtifactError(f"制品大小不匹配: 期望 {descriptor.size}, 实际 {len(content)}")
        actual = self._digest(content)
        if actual != descriptor.sha256:
            raise ArtifactError(f"制品摘要不匹配: 期望 {descriptor.sha256}, 实际 {actual}")
        return content


__all__ = ["LocalArtifactRepository"]
