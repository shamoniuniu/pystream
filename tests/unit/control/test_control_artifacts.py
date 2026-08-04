"""JobManager 本地制品仓库测试。"""

from __future__ import annotations

import hashlib

import pytest

from pystream.control import (
    ArtifactDescriptor,
    ArtifactError,
    LocalArtifactRepository,
    S3ArtifactRepository,
)
from pystream.storage import (
    ObjectConflict,
    ObjectNotFound,
    ObjectStoreError,
    ObjectValue,
)


def test_repository_按job和摘要不可变保存并幂等读取(tmp_path):
    repository = LocalArtifactRepository(tmp_path / "artifacts")
    content = b"PK\x03\x04fake-job-bundle"
    expected = hashlib.sha256(content).hexdigest()

    first = repository.put("job-1", content, expected)
    second = repository.put("job-1", content, expected)

    assert first == second
    assert first.sha256 == expected
    assert first.size == len(content)
    assert repository.read(first) == content
    assert (tmp_path / "artifacts" / "job-1" / f"{expected}.zip").read_bytes() == content
    assert list((tmp_path / "artifacts" / "job-1").glob("*.tmp")) == []


def test_repository_拒绝期望摘要不匹配且不写文件(tmp_path):
    repository = LocalArtifactRepository(tmp_path / "artifacts")

    with pytest.raises(ArtifactError, match="摘要不匹配"):
        repository.put("job-1", b"content", "0" * 64)

    assert list((tmp_path / "artifacts").rglob("*.zip")) == []


@pytest.mark.parametrize("job_id", ["../escape", "/absolute", "", "has space"])
def test_repository_拒绝不安全job_id(tmp_path, job_id):
    repository = LocalArtifactRepository(tmp_path / "artifacts")

    with pytest.raises(ArtifactError, match="job_id"):
        repository.put(job_id, b"content")


def test_repository_read_拒绝缺失_大小变化和摘要篡改(tmp_path):
    repository = LocalArtifactRepository(tmp_path / "artifacts")
    content = b"original"
    descriptor = repository.put("job-1", content)
    path = tmp_path / "artifacts" / "job-1" / f"{descriptor.sha256}.zip"

    with pytest.raises(ArtifactError, match="无法读取"):
        repository.read(ArtifactDescriptor("missing", descriptor.sha256, len(content)))

    with pytest.raises(ArtifactError, match="大小不匹配"):
        repository.read(ArtifactDescriptor("job-1", descriptor.sha256, len(content) + 1))

    path.write_bytes(b"tampered")
    with pytest.raises(ArtifactError, match="摘要不匹配"):
        repository.read(descriptor)


def test_repository_read_拒绝非法摘要格式(tmp_path):
    repository = LocalArtifactRepository(tmp_path / "artifacts")

    with pytest.raises(ArtifactError, match="sha256"):
        repository.read(ArtifactDescriptor("job-1", "NOT-A-DIGEST", 0))


class MemoryObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, ObjectValue] = {}

    def get(self, key: str) -> ObjectValue:
        try:
            return self.objects[key]
        except KeyError as exc:
            raise ObjectNotFound(key) from exc

    def put_if_absent(self, key: str, content: bytes) -> str:
        if key in self.objects:
            raise ObjectConflict(key)
        etag = hashlib.sha256(content).hexdigest()
        self.objects[key] = ObjectValue(content, etag)
        return etag

    def put_if_match(self, key: str, content: bytes, etag: str) -> str:
        raise NotImplementedError

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        return tuple(sorted(key for key in self.objects if key.startswith(prefix)))


class ResponseLostObjectStore(MemoryObjectStore):
    def put_if_absent(self, key: str, content: bytes) -> str:
        super().put_if_absent(key, content)
        raise ObjectStoreError("simulated response loss")


def test_s3_artifact_repository_uses_content_address_and_reuses_across_jobs() -> None:
    store = MemoryObjectStore()
    repository = S3ArtifactRepository(store)
    content = b"same-bundle"
    digest = hashlib.sha256(content).hexdigest()

    first = repository.put("job-1", content)
    second = repository.put("job-2", content)

    assert first.job_id == "job-1"
    assert second.job_id == "job-2"
    assert repository.read(second) == content
    assert set(store.objects) == {f"pystream/artifacts/{digest}.zip"}


def test_s3_artifact_repository_reconciles_success_after_response_loss() -> None:
    repository = S3ArtifactRepository(ResponseLostObjectStore())

    descriptor = repository.put("job-1", b"bundle")

    assert repository.read(descriptor) == b"bundle"


def test_s3_artifact_repository_rejects_conflicting_content_address() -> None:
    store = MemoryObjectStore()
    repository = S3ArtifactRepository(store)
    content = b"bundle"
    digest = hashlib.sha256(content).hexdigest()
    store.objects[f"pystream/artifacts/{digest}.zip"] = ObjectValue(
        b"tampered",
        "etag",
    )

    with pytest.raises(ArtifactError, match="内容不匹配"):
        repository.put("job-1", content)
