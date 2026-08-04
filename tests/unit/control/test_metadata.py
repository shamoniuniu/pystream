"""Job metadata immutable revision 与 current CAS 测试。"""

from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from pystream.api import DeliveryGuarantee
from pystream.control.metadata import (
    JobMetadataConflict,
    JobMetadataError,
    JobMetadataRevision,
    S3JobMetadataRepository,
)
from pystream.control.models import JobStatus
from pystream.storage import (
    ObjectConflict,
    ObjectNotFound,
    ObjectStoreError,
    ObjectValue,
)


class MemoryObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, ObjectValue] = {}
        self.generation = 0

    def get(self, key: str) -> ObjectValue:
        try:
            return self.objects[key]
        except KeyError as exc:
            raise ObjectNotFound(key) from exc

    def put_if_absent(self, key: str, content: bytes) -> str:
        if key in self.objects:
            raise ObjectConflict(key)
        return self._put(key, content)

    def put_if_match(self, key: str, content: bytes, etag: str) -> str:
        current = self.objects.get(key)
        if current is None or current.etag != etag:
            raise ObjectConflict(key)
        return self._put(key, content)

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        return tuple(sorted(key for key in self.objects if key.startswith(prefix)))

    def _put(self, key: str, content: bytes) -> str:
        self.generation += 1
        etag = f"etag-{self.generation}"
        self.objects[key] = ObjectValue(content, etag)
        return etag


class ResponseLostObjectStore(MemoryObjectStore):
    def __init__(self) -> None:
        super().__init__()
        self.lose_next_match_response = False

    def put_if_match(self, key: str, content: bytes, etag: str) -> str:
        updated_etag = super().put_if_match(key, content, etag)
        if self.lose_next_match_response:
            self.lose_next_match_response = False
            raise ObjectStoreError("simulated response loss")
        return updated_etag


def revision(number: int) -> JobMetadataRevision:
    return JobMetadataRevision(
        job_id="job-1",
        revision=number,
        definition={"api_version": "pystream/v1", "job": {"name": "demo"}},
        artifact_sha256="a" * 64,
        artifact_size=1024,
        delivery_guarantee=DeliveryGuarantee.EXACTLY_ONCE,
        status=JobStatus.RUNNING,
        attempt_id=2,
        next_checkpoint_id=8,
        last_decided_checkpoint_id=7,
        last_finalized_checkpoint_id=6,
        recovery_attempts=1,
        last_failure=None,
        coordinator_epoch=4,
        finalize_backlog=(7,),
        updated_at=datetime(2026, 7, 31, 12, 0, tzinfo=UTC),
    )


def test_job_metadata_revision_and_current_pointer_roundtrip() -> None:
    store = MemoryObjectStore()
    repository = S3JobMetadataRepository(store)

    first = repository.publish(revision(0), expected_current_etag=None)
    second_revision = replace(
        revision(1),
        last_finalized_checkpoint_id=7,
        finalize_backlog=(),
    )
    second = repository.publish(
        second_revision,
        expected_current_etag=first.current_etag,
    )

    assert repository.read_current("job-1") == second
    assert repository.list_jobs() == ("job-1",)
    assert set(store.objects) == {
        "pystream/jobs/job-1/current.json",
        "pystream/jobs/job-1/revisions/00000000000000000000.json",
        "pystream/jobs/job-1/revisions/00000000000000000001.json",
    }


def test_job_metadata_stale_current_etag_is_explicit_conflict() -> None:
    store = MemoryObjectStore()
    repository = S3JobMetadataRepository(store)
    first = repository.publish(revision(0), expected_current_etag=None)
    repository.publish(revision(1), expected_current_etag=first.current_etag)

    with pytest.raises(JobMetadataConflict, match="CAS"):
        repository.publish(revision(2), expected_current_etag=first.current_etag)

    assert repository.read_current("job-1").revision.revision == 1


def test_job_metadata_retry_reconciles_already_applied_current_pointer() -> None:
    repository = S3JobMetadataRepository(MemoryObjectStore())

    first = repository.publish(revision(0), expected_current_etag=None)
    retried = repository.publish(revision(0), expected_current_etag=None)

    assert retried == first


def test_job_metadata_reconciles_cas_success_after_response_loss() -> None:
    store = ResponseLostObjectStore()
    repository = S3JobMetadataRepository(store)
    first = repository.publish(revision(0), expected_current_etag=None)
    store.lose_next_match_response = True

    second = repository.publish(
        replace(revision(1), last_finalized_checkpoint_id=7, finalize_backlog=()),
        expected_current_etag=first.current_etag,
    )

    assert second.revision.revision == 1
    assert repository.read_current("job-1") == second


def test_job_metadata_immutable_revision_rejects_different_content() -> None:
    store = MemoryObjectStore()
    repository = S3JobMetadataRepository(store)
    first = repository.publish(revision(0), expected_current_etag=None)

    with pytest.raises(JobMetadataConflict, match="不同内容"):
        repository.publish(
            replace(revision(0), attempt_id=99),
            expected_current_etag=first.current_etag,
        )


def test_job_metadata_read_rejects_tampered_revision() -> None:
    store = MemoryObjectStore()
    repository = S3JobMetadataRepository(store)
    repository.publish(revision(0), expected_current_etag=None)
    key = "pystream/jobs/job-1/revisions/00000000000000000000.json"
    store.objects[key] = ObjectValue(
        b'{"tampered":true}',
        hashlib.sha256(b"tampered").hexdigest(),
    )

    with pytest.raises(JobMetadataError, match="SHA-256"):
        repository.read_current("job-1")


@pytest.mark.parametrize(
    "changes",
    [
        {"last_finalized_checkpoint_id": 8},
        {"finalize_backlog": (7, 7)},
        {"updated_at": datetime(2026, 7, 31)},
    ],
)
def test_job_metadata_revision_rejects_invalid_invariants(changes) -> None:
    with pytest.raises(JobMetadataError):
        replace(revision(0), **changes)
