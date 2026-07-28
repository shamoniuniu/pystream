"""确定性 ZIP 作业包构建、完整性验证和安全解压。"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
import zipfile
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from pystream.artifact.errors import ArtifactValidationError
from pystream.artifact.models import (
    MANIFEST_NAME,
    MANIFEST_VERSION,
    ArtifactManifest,
    JobBundle,
    ManifestEntry,
    validate_sha256,
)

DEFAULT_MAX_FILES = 1_000
DEFAULT_MAX_FILE_SIZE = 16 * 1024 * 1024
DEFAULT_MAX_TOTAL_SIZE = 64 * 1024 * 1024
MAX_MANIFEST_SIZE = 1024 * 1024
_COPY_CHUNK_SIZE = 64 * 1024
_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:")
_WINDOWS_RESERVED_NAMES = {
    "AUX",
    "CON",
    "NUL",
    "PRN",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}


def _sha256_stream(stream: BinaryIO) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    while chunk := stream.read(_COPY_CHUNK_SIZE):
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def sha256_file(path: str | Path) -> str:
    """计算文件 SHA-256。"""
    with Path(path).open("rb") as stream:
        return _sha256_stream(stream)[0]


def artifact_filename(sha256: str) -> str:
    """返回由完整摘要决定的不可变制品文件名。"""
    return f"artifact_{validate_sha256(sha256)}.zip"


def _validated_member_parts(name: str) -> tuple[str, ...]:
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ArtifactValidationError("ZIP 条目名必须是非空 UTF-8 路径")
    if "\\" in name:
        raise ArtifactValidationError(f"ZIP 条目禁止反斜杠路径: {name!r}")
    raw_parts = name.split("/")
    if any(part in {"", ".", ".."} for part in raw_parts):
        raise ArtifactValidationError(f"ZIP 条目包含非法路径分段: {name!r}")
    for part in raw_parts:
        device_name = part.split(".", 1)[0].upper()
        if ":" in part or part.rstrip(" .") != part or device_name in _WINDOWS_RESERVED_NAMES:
            raise ArtifactValidationError(f"ZIP 条目包含 Windows 非法路径分段: {name!r}")
    path = PurePosixPath(name)
    if path.is_absolute() or _WINDOWS_DRIVE.match(raw_parts[0]):
        raise ArtifactValidationError(f"ZIP 条目禁止绝对路径: {name!r}")
    if name == MANIFEST_NAME:
        return (MANIFEST_NAME,)
    return path.parts


def _is_link_or_special(info: zipfile.ZipInfo) -> bool:
    mode = (info.external_attr >> 16) & 0xFFFF
    if mode == 0:
        return False
    file_type = stat.S_IFMT(mode)
    return file_type not in {0, stat.S_IFREG}


def _zip_info(name: str) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.create_system = 3
    info.external_attr = (stat.S_IFREG | 0o644) << 16
    return info


def _source_files(
    source_root: Path,
    *,
    max_files: int,
    max_file_size: int,
    max_total_size: int,
) -> tuple[Path, ...]:
    files: list[Path] = []
    total_size = 0
    for current, directory_names, file_names in os.walk(source_root, followlinks=False):
        current_path = Path(current)
        directory_names[:] = [name for name in directory_names if name != "__pycache__"]
        for directory_name in directory_names:
            directory = current_path / directory_name
            if directory.is_symlink():
                raise ArtifactValidationError(f"作业目录禁止符号链接: {directory}")
        for file_name in file_names:
            if file_name.endswith((".pyc", ".pyo")):
                continue
            source = current_path / file_name
            if source.is_symlink() or not source.is_file():
                raise ArtifactValidationError(f"作业包仅允许普通文件: {source}")
            size = source.stat().st_size
            if size > max_file_size:
                raise ArtifactValidationError(
                    f"文件 {source.relative_to(source_root).as_posix()!r} "
                    f"大小 {size} 超过上限 {max_file_size}"
                )
            total_size += size
            if total_size > max_total_size:
                raise ArtifactValidationError(
                    f"作业文件总大小 {total_size} 超过上限 {max_total_size}"
                )
            files.append(source)
            if len(files) > max_files:
                raise ArtifactValidationError(f"作业文件数量超过上限 {max_files}")
    files.sort(key=lambda item: item.relative_to(source_root).as_posix())
    return tuple(files)


def _build_manifest(source_root: Path, files: Iterable[Path]) -> ArtifactManifest:
    entries: list[ManifestEntry] = []
    for source in files:
        relative = source.relative_to(source_root).as_posix()
        _validated_member_parts(relative)
        with source.open("rb") as stream:
            digest, size = _sha256_stream(stream)
        entries.append(ManifestEntry(path=relative, size=size, sha256=digest))
    return ArtifactManifest(
        version=MANIFEST_VERSION,
        entrypoint="job.yaml",
        total_size=sum(entry.size for entry in entries),
        files=tuple(entries),
    )


def _manifest_bytes(manifest: ArtifactManifest) -> bytes:
    return json.dumps(
        manifest.to_dict(),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def build_job_bundle(
    source_dir: str | Path,
    output_dir: str | Path,
    *,
    max_files: int = DEFAULT_MAX_FILES,
    max_file_size: int = DEFAULT_MAX_FILE_SIZE,
    max_total_size: int = DEFAULT_MAX_TOTAL_SIZE,
) -> JobBundle:
    """把作业目录构建为内容寻址、可重复生成的 ZIP 制品。"""
    source = Path(source_dir)
    if source.is_symlink() or not source.is_dir():
        raise ArtifactValidationError(f"作业源目录不存在或不是普通目录: {source}")
    source = source.resolve()
    output = Path(output_dir).resolve()
    if output == source or source in output.parents:
        raise ArtifactValidationError("制品输出目录不能位于作业源目录内")
    output.mkdir(parents=True, exist_ok=True)

    files = _source_files(
        source,
        max_files=max_files,
        max_file_size=max_file_size,
        max_total_size=max_total_size,
    )
    manifest = _build_manifest(source, files)
    manifest_content = _manifest_bytes(manifest)
    if len(manifest_content) > MAX_MANIFEST_SIZE:
        raise ArtifactValidationError("manifest 大小超过安全上限")

    descriptor, temporary_name = tempfile.mkstemp(
        dir=output,
        prefix=".pystream-artifact-",
        suffix=".tmp",
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        with zipfile.ZipFile(
            temporary,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=6,
        ) as archive:
            archive.writestr(_zip_info(MANIFEST_NAME), manifest_content)
            for entry, source_file in zip(manifest.files, files, strict=True):
                archive.writestr(_zip_info(entry.path), source_file.read_bytes())

        digest = sha256_file(temporary)
        validated_manifest, _ = _validate_archive(
            temporary,
            expected_sha256=digest,
            max_files=max_files,
            max_file_size=max_file_size,
            max_total_size=max_total_size,
        )
        if validated_manifest != manifest:  # pragma: no cover - 并发修改文件的纵深防御
            raise ArtifactValidationError("构建后的 ZIP 内容与生成清单不一致")
        destination = output / artifact_filename(digest)
        if destination.exists():
            if not destination.is_file() or sha256_file(destination) != digest:
                raise ArtifactValidationError(f"不可变制品目标已存在但内容不匹配: {destination}")
            temporary.unlink()
        else:
            os.replace(temporary, destination)
        return JobBundle(
            path=str(destination),
            sha256=digest,
            size=destination.stat().st_size,
            manifest=manifest,
        )
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _load_manifest(archive: zipfile.ZipFile, info: zipfile.ZipInfo) -> ArtifactManifest:
    if info.file_size > MAX_MANIFEST_SIZE:
        raise ArtifactValidationError("manifest 大小超过安全上限")
    try:
        with archive.open(info, "r") as stream:
            content = stream.read(MAX_MANIFEST_SIZE + 1)
        if len(content) > MAX_MANIFEST_SIZE:
            raise ArtifactValidationError("manifest 解压后大小超过安全上限")
        document = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArtifactValidationError(f"manifest 不是合法 UTF-8 JSON: {exc}") from exc
    return ArtifactManifest.from_dict(document)


def _validate_archive(
    archive_path: Path,
    *,
    expected_sha256: str,
    max_files: int,
    max_file_size: int,
    max_total_size: int,
) -> tuple[ArtifactManifest, dict[str, zipfile.ZipInfo]]:
    expected = validate_sha256(expected_sha256, field="expected_sha256")
    actual = sha256_file(archive_path)
    if actual != expected:
        raise ArtifactValidationError(f"制品 SHA-256 不匹配: expected={expected}, actual={actual}")

    try:
        archive = zipfile.ZipFile(archive_path, "r")
    except (OSError, zipfile.BadZipFile) as exc:
        raise ArtifactValidationError(f"无法打开 ZIP 制品: {exc}") from exc

    with archive:
        infos: dict[str, zipfile.ZipInfo] = {}
        for info in archive.infolist():
            _validated_member_parts(info.filename)
            if info.filename in infos:
                raise ArtifactValidationError(f"ZIP 包含重复条目: {info.filename!r}")
            if info.is_dir() or _is_link_or_special(info):
                raise ArtifactValidationError(f"ZIP 仅允许普通文件: {info.filename!r}")
            if info.flag_bits & 0x1:
                raise ArtifactValidationError(f"ZIP 不允许加密条目: {info.filename!r}")
            if info.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
                raise ArtifactValidationError(f"ZIP 条目使用不支持的压缩算法: {info.filename!r}")
            infos[info.filename] = info

        manifest_info = infos.get(MANIFEST_NAME)
        if manifest_info is None:
            raise ArtifactValidationError(f"ZIP 缺少 {MANIFEST_NAME}")
        manifest = _load_manifest(archive, manifest_info)
        payload_infos = {name: info for name, info in infos.items() if name != MANIFEST_NAME}
        manifest_entries = {entry.path: entry for entry in manifest.files}
        if set(payload_infos) != set(manifest_entries):
            raise ArtifactValidationError("ZIP 文件集合与 manifest.files 不一致")
        if len(payload_infos) > max_files:
            raise ArtifactValidationError(f"作业文件数量超过上限 {max_files}")
        if manifest.total_size > max_total_size:
            raise ArtifactValidationError(
                f"作业文件总大小 {manifest.total_size} 超过上限 {max_total_size}"
            )
        for name, info in payload_infos.items():
            _validated_member_parts(name)
            entry = manifest_entries[name]
            if entry.size > max_file_size:
                raise ArtifactValidationError(
                    f"文件 {name!r} 大小 {entry.size} 超过上限 {max_file_size}"
                )
            if info.file_size != entry.size:
                raise ArtifactValidationError(f"文件 {name!r} 大小与 manifest 不一致")
        return manifest, payload_infos


def verify_job_bundle(
    archive_path: str | Path,
    *,
    expected_sha256: str,
    max_files: int = DEFAULT_MAX_FILES,
    max_file_size: int = DEFAULT_MAX_FILE_SIZE,
    max_total_size: int = DEFAULT_MAX_TOTAL_SIZE,
) -> ArtifactManifest:
    """验证摘要、ZIP 结构和清单元数据，不向磁盘解压。"""
    source = Path(archive_path)
    if not source.is_file():
        raise ArtifactValidationError(f"制品不存在或不是普通文件: {source}")
    manifest, _ = _validate_archive(
        source,
        expected_sha256=expected_sha256,
        max_files=max_files,
        max_file_size=max_file_size,
        max_total_size=max_total_size,
    )
    return manifest


def extract_job_bundle(
    archive_path: str | Path,
    destination_dir: str | Path,
    *,
    expected_sha256: str,
    max_files: int = DEFAULT_MAX_FILES,
    max_file_size: int = DEFAULT_MAX_FILE_SIZE,
    max_total_size: int = DEFAULT_MAX_TOTAL_SIZE,
) -> ArtifactManifest:
    """校验制品并在临时目录完整验证后原子发布到作业目录。"""
    source = Path(archive_path)
    if not source.is_file():
        raise ArtifactValidationError(f"制品不存在或不是普通文件: {source}")
    destination = Path(destination_dir)
    if destination.exists() or destination.is_symlink():
        raise ArtifactValidationError(f"作业目标目录必须不存在: {destination}")

    manifest, payload_infos = _validate_archive(
        source,
        expected_sha256=expected_sha256,
        max_files=max_files,
        max_file_size=max_file_size,
        max_total_size=max_total_size,
    )
    destination_parent = destination.parent.resolve()
    destination_parent.mkdir(parents=True, exist_ok=True)
    published_destination = destination_parent / destination.name
    staging = Path(
        tempfile.mkdtemp(
            dir=destination_parent,
            prefix=f".{destination.name}-staging-",
        )
    )
    total_written = 0
    try:
        entries = {entry.path: entry for entry in manifest.files}
        with zipfile.ZipFile(source, "r") as archive:
            for name in sorted(payload_infos):
                entry = entries[name]
                target = staging.joinpath(*_validated_member_parts(name))
                target.parent.mkdir(parents=True, exist_ok=True)
                digest = hashlib.sha256()
                file_written = 0
                with (
                    archive.open(payload_infos[name], "r") as input_stream,
                    target.open("xb") as output_stream,
                ):
                    while chunk := input_stream.read(_COPY_CHUNK_SIZE):
                        file_written += len(chunk)
                        total_written += len(chunk)
                        if file_written > entry.size or file_written > max_file_size:
                            raise ArtifactValidationError(
                                f"文件 {name!r} 解压大小超过声明或安全上限"
                            )
                        if total_written > max_total_size:
                            raise ArtifactValidationError("作业解压总大小超过安全上限")
                        digest.update(chunk)
                        output_stream.write(chunk)
                if file_written != entry.size or digest.hexdigest() != entry.sha256:
                    raise ArtifactValidationError(f"文件 {name!r} 内容与 manifest 不一致")
        if published_destination.exists():
            raise ArtifactValidationError(f"作业目标目录已被并发创建: {published_destination}")
        os.replace(staging, published_destination)
        return manifest
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


__all__ = [
    "DEFAULT_MAX_FILES",
    "DEFAULT_MAX_FILE_SIZE",
    "DEFAULT_MAX_TOTAL_SIZE",
    "artifact_filename",
    "build_job_bundle",
    "extract_job_bundle",
    "sha256_file",
    "verify_job_bundle",
]
