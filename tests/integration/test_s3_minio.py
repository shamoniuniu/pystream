"""由显式环境变量启用的真实 S3/MinIO 条件写集成测试。"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from pystream.api import DeliveryGuarantee
from pystream.checkpoint import S3CheckpointStore
from pystream.control import S3ArtifactRepository
from pystream.control.metadata import (
    JobMetadataRevision,
    S3JobMetadataRepository,
)
from pystream.control.models import JobStatus
from pystream.storage import S3ObjectStore

_REQUIRED_ENVIRONMENT = (
    "PYSTREAM_TEST_S3_ENDPOINT",
    "PYSTREAM_TEST_S3_BUCKET",
    "PYSTREAM_TEST_S3_ACCESS_KEY",
    "PYSTREAM_TEST_S3_SECRET_KEY",
)

pytestmark = pytest.mark.skipif(
    any(not os.environ.get(name) for name in _REQUIRED_ENVIRONMENT),
    reason="需要 PYSTREAM_TEST_S3_* MinIO 配置",
)


def test_minio_conditional_writes_and_repositories_roundtrip() -> None:
    import boto3
    from botocore.exceptions import ClientError

    endpoint = os.environ["PYSTREAM_TEST_S3_ENDPOINT"]
    bucket = os.environ["PYSTREAM_TEST_S3_BUCKET"]
    access_key = os.environ["PYSTREAM_TEST_S3_ACCESS_KEY"]
    secret_key = os.environ["PYSTREAM_TEST_S3_SECRET_KEY"]
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    try:
        client.create_bucket(Bucket=bucket)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code"))
        if code not in {"BucketAlreadyExists", "BucketAlreadyOwnedByYou"}:
            raise

    prefix = f"pystream-integration/{uuid4().hex}"
    store = S3ObjectStore(bucket, client=client)
    store.probe_conditional_writes(prefix=f"{prefix}/probes")

    artifact_repository = S3ArtifactRepository(store, prefix=prefix)
    descriptor = artifact_repository.put("integration-job", b"integration-artifact")
    assert artifact_repository.read(descriptor) == b"integration-artifact"

    checkpoint_store = S3CheckpointStore(store, prefix=prefix)
    snapshot = checkpoint_store.write_task_snapshot(
        job_id="integration-job",
        checkpoint_id=1,
        attempt_id=0,
        task_id="integration-job:source:0",
        operator_id="source",
        state={"offset": 7},
    )
    manifest = checkpoint_store.complete_checkpoint(
        job_id="integration-job",
        checkpoint_id=1,
        attempt_id=0,
        expected_task_ids={snapshot.task_id},
        snapshots=(snapshot,),
    )
    assert checkpoint_store.latest_manifest("integration-job") == manifest

    metadata_repository = S3JobMetadataRepository(store, prefix=prefix)
    revision = JobMetadataRevision(
        job_id="integration-job",
        revision=0,
        definition={"api_version": "pystream/v1", "job": {"name": "integration"}},
        artifact_sha256=descriptor.sha256,
        artifact_size=descriptor.size,
        delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
        status=JobStatus.RUNNING,
        attempt_id=0,
        next_checkpoint_id=2,
        last_decided_checkpoint_id=1,
        last_finalized_checkpoint_id=1,
        recovery_attempts=0,
        last_failure=None,
        coordinator_epoch=0,
        finalize_backlog=(),
        updated_at=datetime.now(UTC),
    )
    stored = metadata_repository.publish(revision, expected_current_etag=None)

    assert metadata_repository.read_current("integration-job") == stored
