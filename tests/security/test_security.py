"""TLS identity, Bearer Token, Secret and PKI negative-path tests."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestClient, TestServer
from cryptography import x509
from cryptography.hazmat.primitives import serialization

import pystream.security.pki as pki
from pystream.control import JobManager, JobManagerHttpService, LocalArtifactRepository
from pystream.observability import PyStreamMetrics
from pystream.runtime import DataPlaneServer, FrameType, read_frame
from pystream.security import (
    BearerTokenAuthenticator,
    PeerIdentityError,
    TlsFiles,
    create_client_ssl_context,
    create_server_ssl_context,
    read_secret_file,
    require_peer_identity,
)


class NullGateway:
    async def deploy_task(self, worker, deployment) -> None:
        del worker, deployment

    async def stop_task(
        self,
        worker,
        task_id: str,
        attempt_id: int,
        coordinator_epoch: int = 0,
    ) -> None:
        del worker, task_id, attempt_id, coordinator_epoch


class FakeSslObject:
    def __init__(self, certificate: dict[str, object]) -> None:
        self.certificate = certificate

    def getpeercert(self) -> dict[str, object]:
        return self.certificate


class FakeTransport:
    def __init__(self, certificate: dict[str, object] | None) -> None:
        self.certificate = certificate

    def get_extra_info(self, name: str, default=None):
        if name == "ssl_object" and self.certificate is not None:
            return FakeSslObject(self.certificate)
        return default


def _tls_files(root: Path, identity: str) -> TlsFiles:
    return TlsFiles(
        root / "ca.crt",
        root / "services" / f"{identity}.crt",
        root / "services" / f"{identity}.key",
    )


async def _open_tls(
    server_files: TlsFiles,
    client_files: TlsFiles,
    *,
    server_hostname: str,
) -> None:
    connections: list[asyncio.StreamWriter] = []

    async def handle(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        del reader
        connections.append(writer)
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(
        handle,
        "127.0.0.1",
        0,
        ssl=create_server_ssl_context(server_files),
    )
    try:
        port = server.sockets[0].getsockname()[1]
        _, writer = await asyncio.open_connection(
            "127.0.0.1",
            port,
            ssl=create_client_ssl_context(client_files),
            server_hostname=server_hostname,
        )
        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()
        for writer in connections:
            if not writer.is_closing():
                writer.close()


@pytest.fixture
def generated_pki(tmp_path: Path) -> Path:
    return pki.generate_development_pki(tmp_path / "pki", valid_days=30)


@pytest.mark.asyncio
async def test_mtls_accepts_trusted_identity_and_rejects_wrong_ca(
    generated_pki: Path,
    tmp_path: Path,
) -> None:
    await _open_tls(
        _tls_files(generated_pki, "worker-1"),
        _tls_files(generated_pki, "jobmanager"),
        server_hostname="worker-1",
    )
    other = pki.generate_development_pki(tmp_path / "other", valid_days=30)
    with pytest.raises((ConnectionError, OSError)):
        await _open_tls(
            _tls_files(generated_pki, "worker-1"),
            _tls_files(other, "jobmanager"),
            server_hostname="worker-1",
        )


@pytest.mark.asyncio
async def test_mtls_rejects_expired_server_certificate(generated_pki: Path) -> None:
    ca_key = serialization.load_pem_private_key(
        (generated_pki / "ca.key").read_bytes(),
        password=None,
    )
    ca_certificate = x509.load_pem_x509_certificate((generated_pki / "ca.crt").read_bytes())
    now = datetime.now(UTC)
    key, certificate = pki._issue_certificate(
        "expired-worker",
        ("expired-worker",),
        ca_key=ca_key,
        ca_certificate=ca_certificate,
        not_before=now - timedelta(days=2),
        not_after=now - timedelta(days=1),
    )
    key_path = generated_pki / "services" / "expired-worker.key"
    cert_path = generated_pki / "services" / "expired-worker.crt"
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))

    with pytest.raises((ConnectionError, OSError)):
        await _open_tls(
            TlsFiles(generated_pki / "ca.crt", cert_path, key_path),
            _tls_files(generated_pki, "jobmanager"),
            server_hostname="expired-worker",
        )


def test_peer_identity_prefers_san_and_rejects_wrong_service() -> None:
    worker = FakeTransport(
        {
            "subjectAltName": (("DNS", "worker-1"),),
            "subject": ((("commonName", "forged-cn"),),),
        }
    )
    assert require_peer_identity(worker, prefixes=("worker-",)) == "worker-1"
    with pytest.raises(PeerIdentityError, match="不被允许"):
        require_peer_identity(worker, exact=frozenset({"jobmanager"}))


@pytest.mark.asyncio
async def test_data_plane_rejects_non_worker_certificate_identity(
    generated_pki: Path,
) -> None:
    metrics = PyStreamMetrics()
    server = DataPlaneServer(
        "127.0.0.1",
        0,
        ssl_context=create_server_ssl_context(_tls_files(generated_pki, "worker-2")),
        metrics=metrics,
    )
    await server.start()
    try:
        reader, writer = await asyncio.open_connection(
            "127.0.0.1",
            server.bound_port,
            ssl=create_client_ssl_context(_tls_files(generated_pki, "jobmanager")),
            server_hostname="worker-2",
        )
        frame = await asyncio.wait_for(read_frame(reader), timeout=2)
        assert frame.frame_type is FrameType.ERROR
        assert frame.payload["code"] == "HANDSHAKE_FAILED"
        writer.close()
        await writer.wait_closed()
    finally:
        await server.close()

    rendered = metrics.render().decode("utf-8")
    assert 'surface="worker_data_plane"' in rendered


@pytest.mark.asyncio
async def test_jobmanager_internal_api_binds_certificate_to_worker_id(
    generated_pki: Path,
    tmp_path: Path,
) -> None:
    service = JobManagerHttpService(
        JobManager(
            LocalArtifactRepository(tmp_path / "store"),
            NullGateway(),
            coordinator_epoch=1,
        ),
        require_internal_tls=True,
    )
    runner = web.AppRunner(service.create_app())
    await runner.setup()
    site = web.TCPSite(
        runner,
        "127.0.0.1",
        0,
        ssl_context=create_server_ssl_context(_tls_files(generated_pki, "jobmanager")),
    )
    await site.start()
    try:
        assert site._server is not None
        port = site._server.sockets[0].getsockname()[1]
        base_url = f"https://localhost:{port}"
        worker = create_client_ssl_context(_tls_files(generated_pki, "worker-1"))
        payload = {
            "worker_id": "worker-2",
            "incarnation_id": "process-1",
            "control_address": "https://worker-1:8081",
            "data_host": "worker-1",
            "data_port": 9000,
            "total_slots": 1,
        }
        async with ClientSession() as client:
            mismatch = await client.post(
                f"{base_url}/workers/register",
                json=payload,
                ssl=worker,
            )
            assert mismatch.status == 403

            payload["worker_id"] = "worker-1"
            accepted = await client.post(
                f"{base_url}/workers/register",
                json=payload,
                ssl=worker,
            )
            assert accepted.status == 201

            wrong_service = await client.post(
                f"{base_url}/workers/worker-1/heartbeat",
                json={},
                ssl=create_client_ssl_context(_tls_files(generated_pki, "jobmanager-1")),
            )
            assert wrong_service.status == 403

            metrics = await client.get(
                f"{base_url}/metrics",
                ssl=create_client_ssl_context(_tls_files(generated_pki, "prometheus")),
            )
            assert metrics.status == 200
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_external_api_and_metrics_require_independent_bearer_tokens(
    tmp_path: Path,
) -> None:
    metrics = PyStreamMetrics()
    service = JobManagerHttpService(
        JobManager(LocalArtifactRepository(tmp_path / "store"), NullGateway()),
        external_auth=BearerTokenAuthenticator("management-secret"),
        metrics_auth=BearerTokenAuthenticator("metrics-secret"),
        metrics=metrics,
    )
    client = TestClient(TestServer(service.create_app()))
    await client.start_server()
    try:
        assert (await client.get("/v1/workers")).status == 401
        assert (
            await client.get(
                "/v1/workers",
                headers={"Authorization": "Bearer wrong"},
            )
        ).status == 401
        assert (
            await client.get(
                "/v1/workers",
                headers={"Authorization": "Bearer management-secret"},
            )
        ).status == 200

        assert (await client.get("/metrics")).status == 401
        scraped = await client.get(
            "/metrics",
            headers={"Authorization": "Bearer metrics-secret"},
        )
        assert scraped.status == 200
        body = await scraped.text()
        assert "pystream_auth_rejections_total" in body
        assert "management-secret" not in body
        assert "metrics-secret" not in body
    finally:
        await client.close()


def test_secret_errors_and_pki_manifest_do_not_expose_secret_values(
    generated_pki: Path,
    tmp_path: Path,
) -> None:
    secret = tmp_path / "empty-secret"
    secret.write_text("", encoding="utf-8")
    with pytest.raises(ValueError) as captured:
        read_secret_file(secret, "test token")
    assert "token-value" not in str(captured.value)

    manifest_text = (generated_pki / "manifest.json").read_text(encoding="utf-8")
    manifest = json.loads(manifest_text)
    external_token = read_secret_file(
        generated_pki / "secrets" / "external-token",
        "external token",
    )
    assert manifest["schema_version"] == 1
    assert external_token not in manifest_text
    assert "PRIVATE KEY" not in manifest_text
