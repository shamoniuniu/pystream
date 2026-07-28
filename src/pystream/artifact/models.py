"""不可变作业制品的清单与构建结果模型。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from pystream.artifact.errors import ArtifactValidationError

MANIFEST_NAME = "MANIFEST.json"
MANIFEST_VERSION = 1
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def validate_sha256(value: str, *, field: str = "sha256") -> str:
    """校验并返回小写 SHA-256 十六进制摘要。"""
    normalized = value.lower()
    if _SHA256_PATTERN.fullmatch(normalized) is None:
        raise ArtifactValidationError(f"{field} 必须是 64 位 SHA-256 十六进制字符串")
    return normalized


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    """清单中的单个普通文件。"""

    path: str
    size: int
    sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.path, str) or not self.path:
            raise ArtifactValidationError("清单文件 path 不能为空")
        if isinstance(self.size, bool) or not isinstance(self.size, int) or self.size < 0:
            raise ArtifactValidationError(f"清单文件 {self.path!r} 的 size 必须是非负整数")
        object.__setattr__(
            self,
            "sha256",
            validate_sha256(self.sha256, field=f"清单文件 {self.path!r} 的 sha256"),
        )

    def to_dict(self) -> dict[str, Any]:
        """返回用于稳定 JSON 编码的字典。"""
        return {"path": self.path, "size": self.size, "sha256": self.sha256}

    @classmethod
    def from_dict(cls, value: Any, *, index: int) -> ManifestEntry:
        """从严格字典恢复清单项，拒绝未知或缺失字段。"""
        if not isinstance(value, dict):
            raise ArtifactValidationError(f"manifest.files[{index}] 必须是 object")
        expected = {"path", "size", "sha256"}
        if set(value) != expected:
            raise ArtifactValidationError(
                f"manifest.files[{index}] 字段必须恰好为 {sorted(expected)}"
            )
        return cls(path=value["path"], size=value["size"], sha256=value["sha256"])


@dataclass(frozen=True, slots=True)
class ArtifactManifest:
    """ZIP 内嵌的版本化文件清单。"""

    version: int
    entrypoint: str
    total_size: int
    files: tuple[ManifestEntry, ...]

    def __post_init__(self) -> None:
        if isinstance(self.version, bool) or not isinstance(self.version, int):
            raise ArtifactValidationError("manifest.version 必须是整数")
        if self.version != MANIFEST_VERSION:
            raise ArtifactValidationError(
                f"不支持的 manifest version {self.version!r}, 仅支持 {MANIFEST_VERSION}"
            )
        if not isinstance(self.entrypoint, str) or self.entrypoint != "job.yaml":
            raise ArtifactValidationError("manifest.entrypoint 必须是 job.yaml")
        if (
            isinstance(self.total_size, bool)
            or not isinstance(self.total_size, int)
            or self.total_size < 0
        ):
            raise ArtifactValidationError("manifest.total_size 必须是非负整数")
        paths = [entry.path for entry in self.files]
        if len(paths) != len(set(paths)):
            raise ArtifactValidationError("manifest.files 包含重复 path")
        if self.entrypoint not in paths:
            raise ArtifactValidationError("作业包必须包含根目录 job.yaml")
        actual_size = sum(entry.size for entry in self.files)
        if self.total_size != actual_size:
            raise ArtifactValidationError(
                f"manifest.total_size={self.total_size} 与文件总大小 {actual_size} 不一致"
            )

    def to_dict(self) -> dict[str, Any]:
        """返回用于稳定 JSON 编码的字典。"""
        return {
            "version": self.version,
            "entrypoint": self.entrypoint,
            "total_size": self.total_size,
            "files": [entry.to_dict() for entry in self.files],
        }

    @classmethod
    def from_dict(cls, value: Any) -> ArtifactManifest:
        """从严格字典恢复清单。"""
        if not isinstance(value, dict):
            raise ArtifactValidationError("manifest 根节点必须是 object")
        expected = {"version", "entrypoint", "total_size", "files"}
        if set(value) != expected:
            raise ArtifactValidationError(f"manifest 字段必须恰好为 {sorted(expected)}")
        files_value = value["files"]
        if not isinstance(files_value, list):
            raise ArtifactValidationError("manifest.files 必须是 array")
        files = tuple(
            ManifestEntry.from_dict(entry, index=index) for index, entry in enumerate(files_value)
        )
        return cls(
            version=value["version"],
            entrypoint=value["entrypoint"],
            total_size=value["total_size"],
            files=files,
        )


@dataclass(frozen=True, slots=True)
class JobBundle:
    """一次作业包构建的不可变结果。"""

    path: str
    sha256: str
    size: int
    manifest: ArtifactManifest

    def __post_init__(self) -> None:
        object.__setattr__(self, "sha256", validate_sha256(self.sha256))


__all__ = [
    "MANIFEST_NAME",
    "MANIFEST_VERSION",
    "ArtifactManifest",
    "JobBundle",
    "ManifestEntry",
    "validate_sha256",
]
