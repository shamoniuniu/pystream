"""S3 条件请求适配器的响应与错误映射测试。"""

from __future__ import annotations

import io

import pytest

from pystream.storage import (
    ObjectConflict,
    ObjectNotFound,
    ObjectStoreError,
    S3ObjectStore,
)


class S3Failure(RuntimeError):
    def __init__(self, code: str, status: int | None = None) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}
        if status is not None:
            self.response["ResponseMetadata"] = {"HTTPStatusCode": status}


class FakeS3Client:
    def __init__(self) -> None:
        self.put_calls: list[dict[str, object]] = []
        self.delete_calls: list[dict[str, object]] = []
        self.get_response: object = {
            "Body": io.BytesIO(b"value"),
            "ETag": '"read-etag"',
        }
        self.list_responses = [
            {
                "Contents": [{"Key": "pystream/b"}, {"Key": "pystream/a"}],
                "IsTruncated": True,
                "NextContinuationToken": "next",
            },
            {
                "Contents": [{"Key": "pystream/c"}],
                "IsTruncated": False,
            },
        ]

    def get_object(self, **kwargs):
        del kwargs
        if isinstance(self.get_response, Exception):
            raise self.get_response
        return self.get_response

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)
        body = kwargs["Body"]
        return {"ETag": f'"etag-{len(body)}"'}

    def list_objects_v2(self, **kwargs):
        if len(self.list_responses) == 1:
            assert kwargs["ContinuationToken"] == "next"
        return self.list_responses.pop(0)

    def delete_object(self, **kwargs):
        self.delete_calls.append(kwargs)


def test_s3_object_store_uses_conditional_headers_and_normalizes_etags() -> None:
    client = FakeS3Client()
    store = S3ObjectStore("bucket", client=client)

    assert store.put_if_absent("pystream/immutable", b"abc") == "etag-3"
    assert store.put_if_match("pystream/current", b"next", "old-etag") == "etag-4"
    value = store.get("pystream/immutable")

    assert client.put_calls == [
        {
            "Bucket": "bucket",
            "Key": "pystream/immutable",
            "Body": b"abc",
            "IfNoneMatch": "*",
        },
        {
            "Bucket": "bucket",
            "Key": "pystream/current",
            "Body": b"next",
            "IfMatch": '"old-etag"',
        },
    ]
    assert value.content == b"value"
    assert value.etag == "read-etag"
    assert store.list_keys("pystream/") == (
        "pystream/a",
        "pystream/b",
        "pystream/c",
    )


@pytest.mark.parametrize(
    ("code", "status", "error_type"),
    [
        ("NoSuchKey", None, ObjectNotFound),
        ("ProviderMissing", 404, ObjectNotFound),
        ("PreconditionFailed", None, ObjectConflict),
        ("ProviderConflict", 409, ObjectConflict),
        ("ProviderPrecondition", 412, ObjectConflict),
        ("AccessDenied", 403, ObjectStoreError),
    ],
)
def test_s3_object_store_maps_service_errors(
    code: str,
    status: int | None,
    error_type: type[Exception],
) -> None:
    client = FakeS3Client()
    client.get_response = S3Failure(code, status)
    store = S3ObjectStore("bucket", client=client)

    with pytest.raises(error_type):
        store.get("pystream/key")


@pytest.mark.parametrize("key", ["", "/absolute"])
def test_s3_object_store_rejects_invalid_keys(key: str) -> None:
    store = S3ObjectStore("bucket", client=FakeS3Client())

    with pytest.raises(ValueError):
        store.put_if_absent(key, b"value")


class ConditionalS3Client:
    def __init__(self) -> None:
        self.content: bytes | None = None
        self.etag: str | None = None
        self.generation = 0
        self.deleted = False

    def put_object(self, **kwargs):
        if kwargs.get("IfNoneMatch") == "*" and self.content is not None:
            raise S3Failure("PreconditionFailed")
        if "IfMatch" in kwargs and kwargs["IfMatch"].strip('"') != self.etag:
            raise S3Failure("PreconditionFailed")
        self.generation += 1
        self.content = kwargs["Body"]
        self.etag = f"etag-{self.generation}"
        return {"ETag": f'"{self.etag}"'}

    def get_object(self, **kwargs):
        del kwargs
        return {
            "Body": io.BytesIO(self.content),
            "ETag": f'"{self.etag}"',
        }

    def list_objects_v2(self, **kwargs):
        return {
            "Contents": [{"Key": kwargs["Prefix"] + "probe.json"}],
            "IsTruncated": False,
        }

    def delete_object(self, **kwargs):
        del kwargs
        self.deleted = True


def test_s3_object_store_capability_probe_validates_conditional_writes(
    monkeypatch,
) -> None:
    client = ConditionalS3Client()
    store = S3ObjectStore("bucket", client=client)
    monkeypatch.setattr(
        "pystream.storage.s3.uuid4",
        lambda: type("ProbeId", (), {"hex": "probe"})(),
    )

    store.probe_conditional_writes()

    assert client.content == b'{"generation":2}'
    assert client.deleted is True
