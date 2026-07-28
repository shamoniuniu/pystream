"""ZIP 作业制品安全边界和 UDF 隔离加载测试。"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
import zipfile
from pathlib import Path

import pytest

from pystream.artifact import (
    MANIFEST_NAME,
    ArtifactValidationError,
    UDFContractError,
    UDFKind,
    UDFLoader,
    UDFLoadError,
    artifact_filename,
    build_job_bundle,
    extract_job_bundle,
    sha256_file,
    verify_job_bundle,
)


def make_job(root: Path, *, udf_source: str = "def transform(value):\n    return value\n") -> Path:
    """创建用于制品测试的最小作业目录。"""
    root.mkdir()
    (root / "job.yaml").write_text(
        "api_version: pystream/v1\njob:\n  name: artifact-test\n",
        encoding="utf-8",
    )
    (root / "udfs.py").write_text(udf_source, encoding="utf-8")
    return root


def raw_manifest(entries: dict[str, bytes]) -> bytes:
    """为手工恶意 ZIP 生成内嵌清单。"""
    files = [
        {
            "path": name,
            "size": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
        }
        for name, content in entries.items()
    ]
    return json.dumps(
        {
            "version": 1,
            "entrypoint": "job.yaml",
            "total_size": sum(len(content) for content in entries.values()),
            "files": files,
        },
        sort_keys=True,
    ).encode()


def regular_info(name: str) -> zipfile.ZipInfo:
    """创建带普通文件模式的 ZIP 条目。"""
    info = zipfile.ZipInfo(name)
    info.create_system = 3
    info.external_attr = (stat.S_IFREG | 0o644) << 16
    return info


def write_raw_bundle(
    path: Path,
    entries: dict[str, bytes],
    *,
    manifest_entries: dict[str, bytes] | None = None,
    special_infos: dict[str, zipfile.ZipInfo] | None = None,
) -> str:
    """构造可包含非法名称或类型的 ZIP，并返回摘要。"""
    special_infos = special_infos or {}
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(regular_info(MANIFEST_NAME), raw_manifest(manifest_entries or entries))
        for name, content in entries.items():
            archive.writestr(special_infos.get(name, regular_info(name)), content)
    return sha256_file(path)


def test_构建内容寻址制品并完成安全解压(tmp_path):
    source = make_job(tmp_path / "job")
    nested = source / "pkg"
    nested.mkdir()
    (nested / "helper.py").write_text("VALUE = 1\n", encoding="utf-8")
    output = tmp_path / "artifacts"

    first = build_job_bundle(source, output)
    second = build_job_bundle(source, output)

    bundle_path = Path(first.path)
    assert first == second
    assert bundle_path.name == artifact_filename(first.sha256)
    assert sha256_file(bundle_path) == first.sha256
    assert [entry.path for entry in first.manifest.files] == [
        "job.yaml",
        "pkg/helper.py",
        "udfs.py",
    ]
    assert first.manifest.total_size == sum(entry.size for entry in first.manifest.files)
    assert verify_job_bundle(bundle_path, expected_sha256=first.sha256) == first.manifest

    destination = tmp_path / "jobs" / "job-1"
    extracted_manifest = extract_job_bundle(
        bundle_path,
        destination,
        expected_sha256=first.sha256,
    )

    assert extracted_manifest == first.manifest
    assert (destination / "job.yaml").read_bytes() == (source / "job.yaml").read_bytes()
    assert (destination / "pkg" / "helper.py").read_text(encoding="utf-8") == "VALUE = 1\n"


def test_构建拒绝缺失_job_yaml_和源目录内输出(tmp_path):
    missing_entrypoint = tmp_path / "missing"
    missing_entrypoint.mkdir()
    (missing_entrypoint / "udfs.py").write_text("pass\n", encoding="utf-8")

    with pytest.raises(ArtifactValidationError, match=r"job\.yaml"):
        build_job_bundle(missing_entrypoint, tmp_path / "output")

    source = make_job(tmp_path / "job")
    with pytest.raises(ArtifactValidationError, match="不能位于作业源目录内"):
        build_job_bundle(source, source / "artifacts")


def test_构建拒绝文件数量和大小上限(tmp_path):
    source = make_job(tmp_path / "job")
    with pytest.raises(ArtifactValidationError, match="大小"):
        build_job_bundle(source, tmp_path / "large-output", max_file_size=1)
    with pytest.raises(ArtifactValidationError, match="数量"):
        build_job_bundle(source, tmp_path / "many-output", max_files=1)


def test_构建拒绝源目录符号链接(tmp_path):
    source = make_job(tmp_path / "job")
    target = tmp_path / "outside.py"
    target.write_text("SECRET = True\n", encoding="utf-8")
    link = source / "link.py"
    try:
        os.symlink(target, link)
    except OSError:
        pytest.skip("当前 Windows 环境未授权创建符号链接")
    with pytest.raises(ArtifactValidationError, match=r"符号链接|普通文件"):
        build_job_bundle(source, tmp_path / "link-output")


def test_摘要不匹配时不创建目标目录(tmp_path):
    bundle = build_job_bundle(make_job(tmp_path / "job"), tmp_path / "output")
    archive = Path(bundle.path)
    content = bytearray(archive.read_bytes())
    content[-1] ^= 0x01
    archive.write_bytes(content)
    destination = tmp_path / "extracted"

    with pytest.raises(ArtifactValidationError, match="SHA-256 不匹配"):
        extract_job_bundle(
            archive,
            destination,
            expected_sha256=bundle.sha256,
        )

    assert not destination.exists()


@pytest.mark.parametrize(
    "malicious_name",
    [
        "../outside.py",
        "/absolute.py",
        r"..\outside.py",
        "C:/outside.py",
        "pkg//file.py",
        "CON.py",
        "pkg/name:stream.py",
        "pkg/trailing. ",
    ],
)
def test_解压拒绝路径穿越和非规范路径(tmp_path, malicious_name):
    archive = tmp_path / "malicious.zip"
    entries = {
        "job.yaml": b"api_version: pystream/v1\n",
        malicious_name: b"escaped = True\n",
    }
    digest = write_raw_bundle(archive, entries)
    destination = tmp_path / "job"

    with pytest.raises(ArtifactValidationError, match=r"路径|绝对"):
        extract_job_bundle(archive, destination, expected_sha256=digest)

    assert not destination.exists()
    assert not (tmp_path / "outside.py").exists()


def test_解压拒绝符号链接条目(tmp_path):
    archive = tmp_path / "symlink.zip"
    entries = {
        "job.yaml": b"api_version: pystream/v1\n",
        "link.py": b"../outside.py",
    }
    link_info = zipfile.ZipInfo("link.py")
    link_info.create_system = 3
    link_info.external_attr = (stat.S_IFLNK | 0o777) << 16
    digest = write_raw_bundle(
        archive,
        entries,
        special_infos={"link.py": link_info},
    )

    with pytest.raises(ArtifactValidationError, match="仅允许普通文件"):
        extract_job_bundle(archive, tmp_path / "job", expected_sha256=digest)


def test_解压拒绝清单与_zip_文件集合不一致(tmp_path):
    archive = tmp_path / "mismatch.zip"
    actual = {
        "job.yaml": b"api_version: pystream/v1\n",
        "extra.py": b"pass\n",
    }
    declared = {"job.yaml": actual["job.yaml"]}
    digest = write_raw_bundle(archive, actual, manifest_entries=declared)

    with pytest.raises(ArtifactValidationError, match="文件集合"):
        verify_job_bundle(archive, expected_sha256=digest)


def test_解压拒绝超限文件且清理暂存目录(tmp_path):
    archive = tmp_path / "large.zip"
    entries = {
        "job.yaml": b"api_version: pystream/v1\n",
        "large.bin": b"x" * 128,
    }
    digest = write_raw_bundle(archive, entries)
    destination = tmp_path / "job"

    with pytest.raises(ArtifactValidationError, match="大小"):
        extract_job_bundle(
            archive,
            destination,
            expected_sha256=digest,
            max_file_size=64,
        )

    assert not destination.exists()
    assert not list(tmp_path.glob(".job-staging-*"))


def test_目标目录存在时拒绝覆盖(tmp_path):
    bundle = build_job_bundle(make_job(tmp_path / "source"), tmp_path / "output")
    destination = tmp_path / "job"
    destination.mkdir()
    marker = destination / "keep.txt"
    marker.write_text("keep", encoding="utf-8")

    with pytest.raises(ArtifactValidationError, match="必须不存在"):
        extract_job_bundle(
            bundle.path,
            destination,
            expected_sha256=bundle.sha256,
        )

    assert marker.read_text(encoding="utf-8") == "keep"


def test_同名_udf_在不同作业命名空间中隔离并可清理(tmp_path):
    first_root = make_job(
        tmp_path / "job-one",
        udf_source="def transform(value):\n    return {'job': 'one', 'value': value}\n",
    )
    second_root = make_job(
        tmp_path / "job-two",
        udf_source="def transform(value):\n    return {'job': 'two', 'value': value}\n",
    )
    first_loader = UDFLoader(first_root, job_id="first")
    second_loader = UDFLoader(second_root, job_id="second")
    first = first_loader.load("udfs:transform", UDFKind.MAP)
    second = second_loader.load("udfs:transform", "map")

    assert first({"n": 1}) == {"job": "one", "value": {"n": 1}}
    assert second({"n": 1}) == {"job": "two", "value": {"n": 1}}
    assert first.function.__module__ != second.function.__module__
    first_namespace = first_loader.namespace
    second_namespace = second_loader.namespace
    assert f"{first_namespace}.udfs" in sys.modules
    assert f"{second_namespace}.udfs" in sys.modules

    first_loader.close()
    assert not any(
        name == first_namespace or name.startswith(f"{first_namespace}.") for name in sys.modules
    )
    assert f"{second_namespace}.udfs" in sys.modules
    second_loader.close()


def test_udf_包内相对导入保持在作业命名空间(tmp_path):
    root = make_job(tmp_path / "job")
    package = root / "wordcount"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "helper.py").write_text("PREFIX = 'job:'\n", encoding="utf-8")
    (package / "functions.py").write_text(
        "from .helper import PREFIX\ndef key(value):\n    return PREFIX + value['word']\n",
        encoding="utf-8",
    )

    with UDFLoader(root, job_id="relative-import") as loader:
        key_selector = loader.load("wordcount.functions:key", UDFKind.KEY_SELECTOR)
        assert key_selector({"word": "apple"}) == "job:apple"
        assert f"{loader.namespace}.wordcount.helper" in sys.modules


@pytest.mark.parametrize(
    ("reference", "message"),
    [
        ("missing_separator", "module:function"),
        ("../outside:run", "module:function"),
        ("missing:run", "不在当前作业目录"),
        ("udfs:missing", "不存在或不可调用"),
    ],
)
def test_udf_拒绝非法引用和缺失对象(tmp_path, reference, message):
    root = make_job(tmp_path / "job")
    with (
        UDFLoader(root, job_id="invalid") as loader,
        pytest.raises(UDFLoadError, match=message),
    ):
        loader.load(reference, UDFKind.MAP)


@pytest.mark.parametrize(
    ("source", "kind", "message"),
    [
        ("def value(a, b):\n    return a\n", UDFKind.MAP, "1 个位置参数"),
        ("def value(a):\n    return a\n", UDFKind.REDUCE, "2 个位置参数"),
        ("async def value(a):\n    return a\n", UDFKind.MAP, "同步函数"),
    ],
)
def test_udf_签名必须满足算子契约(tmp_path, source, kind, message):
    root = make_job(tmp_path / "job", udf_source=source)
    with (
        UDFLoader(root, job_id="signature") as loader,
        pytest.raises(UDFContractError, match=message),
    ):
        loader.load("udfs:value", kind)


def test_udf_返回值必须是严格_json_且调用参数数目明确(tmp_path):
    root = make_job(
        tmp_path / "job",
        udf_source=(
            "def object_value(value):\n"
            "    return object()\n"
            "def nan_value(value):\n"
            "    return float('nan')\n"
        ),
    )
    with UDFLoader(root, job_id="return-contract") as loader:
        object_udf = loader.load("udfs:object_value", UDFKind.MAP)
        nan_udf = loader.load("udfs:nan_value", UDFKind.MAP)

        with pytest.raises(UDFContractError, match="JSON"):
            object_udf({})
        with pytest.raises(UDFContractError, match="JSON"):
            nan_udf({})
        with pytest.raises(UDFContractError, match="参数数量"):
            object_udf()


def test_udf_loader_支持单参数_payload_validator(tmp_path):
    root = make_job(
        tmp_path / "job",
        udf_source=(
            "def validate(value):\n"
            "    if not isinstance(value, dict) or 'word' not in value:\n"
            "        raise ValueError('word required')\n"
        ),
    )
    with UDFLoader(root, job_id="validator") as loader:
        validator = loader.load("udfs:validate", UDFKind.VALIDATOR)

        assert validator({"word": "apple"}) is None
        with pytest.raises(ValueError, match="word required"):
            validator({})


def test_udf_loader_关闭后禁止继续加载(tmp_path):
    loader = UDFLoader(make_job(tmp_path / "job"), job_id="closed")
    loader.close()

    with pytest.raises(UDFLoadError, match="已关闭"):
        loader.load("udfs:transform", UDFKind.MAP)


def test_udf_模块首次导入失败后_loader_仍可加载其他模块(tmp_path):
    root = make_job(
        tmp_path / "job",
        udf_source="raise RuntimeError('broken')\n",
    )
    (root / "healthy.py").write_text(
        "def transform(value):\n    return value\n",
        encoding="utf-8",
    )

    with UDFLoader(root, job_id="retry") as loader:
        with pytest.raises(UDFLoadError, match="加载 UDF 模块"):
            loader.load("udfs:transform", UDFKind.MAP)
        healthy = loader.load("healthy:transform", UDFKind.MAP)
        assert healthy({"ok": True}) == {"ok": True}
