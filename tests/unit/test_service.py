"""Service 对象存储依赖注入与 Secret 文件测试。"""

from __future__ import annotations

from pathlib import Path

import pytest

import pystream.service as service


def test_object_store_configuration_reads_credentials_only_from_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    access_key = tmp_path / "access-key"
    secret_key = tmp_path / "secret-key"
    access_key.write_text("access-from-file\n", encoding="utf-8")
    secret_key.write_text("secret-from-file\n", encoding="utf-8")
    captured: dict[str, object] = {}

    class FakeObjectStore:
        def __init__(self, bucket: str, **kwargs) -> None:
            captured["bucket"] = bucket
            captured.update(kwargs)

        def probe_conditional_writes(self) -> None:
            captured["probed"] = True

    monkeypatch.setattr(service, "S3ObjectStore", FakeObjectStore)
    args = service.build_parser().parse_args(
        [
            "jobmanager",
            "--object-store-endpoint",
            "http://object-store:9000",
            "--object-store-bucket",
            "jobs",
            "--object-store-access-key-file",
            str(access_key),
            "--object-store-secret-key-file",
            str(secret_key),
        ]
    )

    result = service._object_store(args)

    assert isinstance(result, FakeObjectStore)
    assert captured == {
        "bucket": "jobs",
        "endpoint_url": "http://object-store:9000",
        "region_name": "us-east-1",
        "access_key_id": "access-from-file",
        "secret_access_key": "secret-from-file",
        "verify": True,
        "probed": True,
    }


def test_object_store_configuration_uses_environment_file_paths(
    tmp_path: Path,
    monkeypatch,
) -> None:
    access_key = tmp_path / "access-key"
    secret_key = tmp_path / "secret-key"
    ca_file = tmp_path / "ca.pem"
    access_key.write_text("environment-access\n", encoding="utf-8")
    secret_key.write_text("environment-secret\n", encoding="utf-8")
    ca_file.write_text("test-ca\n", encoding="utf-8")
    monkeypatch.setenv("PYSTREAM_OBJECT_STORE_ENDPOINT", "https://object-store:9000")
    monkeypatch.setenv("PYSTREAM_OBJECT_STORE_BUCKET", "environment-bucket")
    monkeypatch.setenv("PYSTREAM_OBJECT_STORE_REGION", "test-region-1")
    monkeypatch.setenv("PYSTREAM_OBJECT_STORE_ACCESS_KEY_FILE", str(access_key))
    monkeypatch.setenv("PYSTREAM_OBJECT_STORE_SECRET_KEY_FILE", str(secret_key))
    monkeypatch.setenv("PYSTREAM_OBJECT_STORE_CA_FILE", str(ca_file))
    captured: dict[str, object] = {}

    class FakeObjectStore:
        def __init__(self, bucket: str, **kwargs) -> None:
            captured["bucket"] = bucket
            captured.update(kwargs)

        def probe_conditional_writes(self) -> None:
            captured["probed"] = True

    monkeypatch.setattr(service, "S3ObjectStore", FakeObjectStore)
    args = service.build_parser().parse_args(
        [
            "worker",
            "--worker-id",
            "worker-1",
            "--control-address",
            "http://worker-1:8081",
            "--data-host",
            "worker-1",
        ]
    )

    result = service._object_store(args)

    assert isinstance(result, FakeObjectStore)
    assert captured == {
        "bucket": "environment-bucket",
        "endpoint_url": "https://object-store:9000",
        "region_name": "test-region-1",
        "access_key_id": "environment-access",
        "secret_access_key": "environment-secret",
        "verify": str(ca_file),
        "probed": True,
    }


def test_object_store_configuration_rejects_missing_or_empty_secret(
    tmp_path: Path,
) -> None:
    parser = service.build_parser()
    missing = parser.parse_args(
        [
            "jobmanager",
            "--object-store-endpoint",
            "http://object-store:9000",
        ]
    )
    with pytest.raises(ValueError, match="要求"):
        service._object_store(missing)

    empty = tmp_path / "empty"
    empty.write_text("", encoding="utf-8")
    configured = parser.parse_args(
        [
            "jobmanager",
            "--object-store-endpoint",
            "http://object-store:9000",
            "--object-store-access-key-file",
            str(empty),
            "--object-store-secret-key-file",
            str(empty),
        ]
    )
    with pytest.raises(ValueError, match="不能为空"):
        service._object_store(configured)
