"""Service 对象存储依赖注入与 Secret 文件测试。"""

from __future__ import annotations

from pathlib import Path

import pytest
from aiohttp import web

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


def test_ha_jobmanager_requires_object_store() -> None:
    args = service.build_parser().parse_args(
        [
            "jobmanager",
            "--jobmanager-id",
            "jobmanager-1",
        ]
    )

    with pytest.raises(ValueError, match="要求配置对象存储"):
        service._create_jobmanager_app(args)


def test_ha_jobmanager_injects_standby_leadership(monkeypatch) -> None:
    object_store = object()
    captured: dict[str, object] = {}

    class RecordingHttpService:
        def __init__(
            self,
            manager,
            *,
            reconcile_interval: float,
            leadership,
        ) -> None:
            captured["manager"] = manager
            captured["reconcile_interval"] = reconcile_interval
            captured["leadership"] = leadership

        def create_app(self) -> web.Application:
            return web.Application()

    monkeypatch.setattr(service, "_object_store", lambda args: object_store)
    monkeypatch.setattr(service, "JobManagerHttpService", RecordingHttpService)
    args = service.build_parser().parse_args(
        [
            "jobmanager",
            "--jobmanager-id",
            "jobmanager-1",
            "--object-store-endpoint",
            "http://object-store:9000",
            "--leader-lease-ttl",
            "12",
            "--leader-renew-interval",
            "4",
            "--leader-poll-interval",
            "2",
        ]
    )

    app = service._create_jobmanager_app(args)

    manager = captured["manager"]
    leadership = captured["leadership"]
    assert isinstance(app, web.Application)
    assert manager.role.value == "STANDBY"
    assert manager.leader_ready is False
    assert leadership.holder_id == "jobmanager-1"
    assert leadership.ttl.total_seconds() == 12
    assert leadership.renew_interval.total_seconds() == 4
    assert leadership.poll_interval.total_seconds() == 2
