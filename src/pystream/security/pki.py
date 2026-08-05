"""生成开发环境 PKI 与 Secret，且不把凭据写入日志。"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import shutil
import struct
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

SERVICE_IDENTITIES: dict[str, tuple[str, ...]] = {
    "haproxy": ("haproxy", "jobmanager-router", "jobmanager-router-core", "localhost"),
    "jobmanager": ("jobmanager", "jobmanager-router-core", "localhost"),
    "jobmanager-1": ("jobmanager-1", "jobmanager-router", "localhost"),
    "jobmanager-2": ("jobmanager-2", "jobmanager-router", "localhost"),
    "worker-1": ("worker-1", "worker-ha-1", "localhost"),
    "worker-2": ("worker-2", "worker-ha-2", "localhost"),
    "worker-3": ("worker-3", "worker-ha-3", "localhost"),
    "prometheus": ("prometheus", "localhost"),
    "kafka": ("kafka", "localhost"),
    "kafka-init": ("kafka-init",),
    "minio-core": ("minio-core", "localhost"),
    "minio-1": ("minio-1",),
    "minio-2": ("minio-2",),
    "minio-3": ("minio-3",),
    "minio-4": ("minio-4",),
    "object-store": ("object-store", "localhost"),
    "tools": ("tools", "localhost"),
}


def generate_development_pki(
    output: str | Path,
    *,
    valid_days: int = 30,
    force: bool = False,
    now: datetime | None = None,
) -> Path:
    """Generate an isolated development CA, service certs and file Secrets."""
    if isinstance(valid_days, bool) or not isinstance(valid_days, int) or valid_days < 1:
        raise ValueError("valid_days 必须是正整数")
    root = Path(output).resolve()
    if root.exists():
        if not force:
            raise FileExistsError(f"PKI 输出目录已存在: {root}")
        shutil.rmtree(root)
    services = root / "services"
    secret_root = root / "secrets"
    services.mkdir(parents=True)
    secret_root.mkdir(parents=True)
    observed_at = (now or datetime.now(UTC)).astimezone(UTC)
    not_before = observed_at - timedelta(minutes=5)
    not_after = observed_at + timedelta(days=valid_days)

    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "PyStream Development CA")])
    ca_certificate = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )
    ca_path = root / "ca.crt"
    _write_private_key(root / "ca.key", ca_key)
    _write_public(ca_path, ca_certificate.public_bytes(serialization.Encoding.PEM))

    issued: dict[str, tuple[rsa.RSAPrivateKey, x509.Certificate]] = {}
    manifest_services: dict[str, object] = {}
    for identity, dns_names in SERVICE_IDENTITIES.items():
        key, certificate = _issue_certificate(
            identity,
            dns_names,
            ca_key=ca_key,
            ca_certificate=ca_certificate,
            not_before=not_before,
            not_after=not_after,
        )
        issued[identity] = (key, certificate)
        key_bytes = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        cert_bytes = certificate.public_bytes(serialization.Encoding.PEM)
        _write_secret(services / f"{identity}.key", key_bytes)
        _write_public(services / f"{identity}.crt", cert_bytes)
        _write_secret(services / f"{identity}.pem", key_bytes + cert_bytes)
        manifest_services[identity] = {
            "dns_names": list(dns_names),
            "not_after": certificate.not_valid_after_utc.isoformat(),
            "sha256": certificate.fingerprint(hashes.SHA256()).hex(),
        }

    password = secrets.token_urlsafe(32)
    _write_secret(secret_root / "kafka-keystore-password", f"{password}\n".encode())
    _write_kafka_store(
        secret_root / "kafka-server.p12",
        "kafka",
        issued["kafka"],
        ca_certificate,
        password,
    )
    _write_kafka_store(
        secret_root / "kafka-client.p12",
        "kafka-init",
        issued["kafka-init"],
        ca_certificate,
        password,
    )
    _write_secret(
        secret_root / "kafka-truststore.jks",
        _serialize_jks_truststore(
            ca_certificate,
            password,
            created_at=observed_at,
        ),
    )

    for name, value in (
        ("external-token", secrets.token_urlsafe(32)),
        ("metrics-token", secrets.token_urlsafe(32)),
        ("object-store-access-key", f"pystream-{secrets.token_hex(8)}"),
        ("object-store-secret-key", secrets.token_urlsafe(36)),
    ):
        _write_secret(secret_root / name, f"{value}\n".encode())

    manifest = {
        "schema_version": 1,
        "generated_at": observed_at.isoformat(),
        "ca_sha256": hashlib.sha256(ca_path.read_bytes()).hexdigest(),
        "valid_days": valid_days,
        "services": manifest_services,
    }
    _write_public(
        root / "manifest.json",
        json.dumps(
            manifest,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            indent=2,
        ).encode("utf-8")
        + b"\n",
    )
    return root


def _issue_certificate(
    identity: str,
    dns_names: tuple[str, ...],
    *,
    ca_key: rsa.RSAPrivateKey,
    ca_certificate: x509.Certificate,
    not_before: datetime,
    not_after: datetime,
) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, identity)]))
        .issuer_name(ca_certificate.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName(name) for name in dns_names]),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.ExtendedKeyUsage(
                [
                    ExtendedKeyUsageOID.SERVER_AUTH,
                    ExtendedKeyUsageOID.CLIENT_AUTH,
                ]
            ),
            critical=False,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )
    return key, certificate


def _write_kafka_store(
    path: Path,
    name: str,
    material: tuple[rsa.RSAPrivateKey, x509.Certificate],
    ca_certificate: x509.Certificate,
    password: str,
) -> None:
    key, certificate = material
    content = pkcs12.serialize_key_and_certificates(
        name=name.encode(),
        key=key,
        cert=certificate,
        cas=[ca_certificate],
        encryption_algorithm=serialization.BestAvailableEncryption(password.encode()),
    )
    _write_secret(path, content)


def _serialize_jks_truststore(
    certificate: x509.Certificate,
    password: str,
    *,
    created_at: datetime,
) -> bytes:
    """Serialize one trusted certificate using the Java KeyStore v2 format."""
    certificate_bytes = certificate.public_bytes(serialization.Encoding.DER)
    entry = (
        struct.pack(">I", 2)
        + _jks_utf("pystream-ca")
        + struct.pack(">q", int(created_at.timestamp() * 1_000))
        + _jks_utf("X.509")
        + struct.pack(">I", len(certificate_bytes))
        + certificate_bytes
    )
    body = struct.pack(">III", 0xFEEDFEED, 2, 1) + entry
    # SHA-1 is mandated by the legacy JKS integrity format, not used as a
    # certificate signature or credential hash.
    digest = hashlib.sha1(
        password.encode("utf-16-be") + b"Mighty Aphrodite" + body,
        usedforsecurity=False,
    ).digest()
    return body + digest


def _jks_utf(value: str) -> bytes:
    encoded = value.encode("ascii")
    if len(encoded) > 65_535:
        raise ValueError("JKS UTF field 过长")
    return struct.pack(">H", len(encoded)) + encoded


def _write_private_key(path: Path, key: rsa.RSAPrivateKey) -> None:
    _write_secret(
        path,
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
    )


def _write_secret(path: Path, content: bytes) -> None:
    _atomic_write(path, content, mode=0o600)


def _write_public(path: Path, content: bytes) -> None:
    _atomic_write(path, content, mode=0o644)


def _atomic_write(path: Path, content: bytes, *, mode: int) -> None:
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(6)}.tmp")
    try:
        temporary.write_bytes(content)
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="生成或轮换 PyStream 开发 PKI 和 Secret")
    parser.add_argument("--output", type=Path, default=Path("run") / "pki")
    parser.add_argument("--valid-days", type=int, default=30)
    parser.add_argument(
        "--force",
        action="store_true",
        help="删除目标目录并轮换 CA、证书和全部 Secret",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = generate_development_pki(
        args.output,
        valid_days=args.valid_days,
        force=args.force,
    )
    print(f"PyStream development PKI written to {root}")
    return 0


__all__ = ["SERVICE_IDENTITIES", "generate_development_pki", "main"]
