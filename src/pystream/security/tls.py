"""TLS context 构造与对端证书服务身份校验。"""

from __future__ import annotations

import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from cryptography import x509


class TlsConfigurationError(ValueError):
    """TLS files or certificate policy are invalid."""


class PeerIdentityError(PermissionError):
    """The authenticated certificate does not identify an allowed service."""


@dataclass(frozen=True, slots=True)
class TlsFiles:
    """CA, certificate and private key file paths for one service identity."""

    ca_file: Path
    cert_file: Path
    key_file: Path

    def validate(self) -> None:
        for name, path in (
            ("CA", self.ca_file),
            ("certificate", self.cert_file),
            ("private key", self.key_file),
        ):
            if not path.is_file():
                raise TlsConfigurationError(f"{name} 文件不存在或不是普通文件: {path}")


class PeerTransport(Protocol):
    def get_extra_info(self, name: str, default=None): ...


def create_server_ssl_context(files: TlsFiles) -> ssl.SSLContext:
    """Create a TLS 1.2+ server context that requires a trusted client cert."""
    files.validate()
    context = ssl.create_default_context(
        ssl.Purpose.CLIENT_AUTH,
        cafile=str(files.ca_file),
    )
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = False
    try:
        context.load_cert_chain(str(files.cert_file), str(files.key_file))
    except ssl.SSLError as exc:
        raise TlsConfigurationError(f"无法加载服务证书或私钥: {type(exc).__name__}") from exc
    return context


def create_client_ssl_context(files: TlsFiles) -> ssl.SSLContext:
    """Create a TLS 1.2+ client context with hostname verification and mTLS."""
    files.validate()
    context = ssl.create_default_context(
        ssl.Purpose.SERVER_AUTH,
        cafile=str(files.ca_file),
    )
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    try:
        context.load_cert_chain(str(files.cert_file), str(files.key_file))
    except ssl.SSLError as exc:
        raise TlsConfigurationError(f"无法加载客户端证书或私钥: {type(exc).__name__}") from exc
    return context


def peer_identities(transport: PeerTransport | None) -> frozenset[str]:
    """Return DNS SAN identities, falling back to CN for legacy certificates."""
    if transport is None:
        return frozenset()
    ssl_object = transport.get_extra_info("ssl_object")
    if ssl_object is None:
        return frozenset()
    certificate = ssl_object.getpeercert()
    if not isinstance(certificate, dict):
        return frozenset()
    dns_names = {
        str(value)
        for kind, value in certificate.get("subjectAltName", ())
        if kind == "DNS" and isinstance(value, str) and value
    }
    if dns_names:
        return frozenset(dns_names)
    common_names = {
        str(value)
        for relative_name in certificate.get("subject", ())
        for kind, value in relative_name
        if kind == "commonName" and isinstance(value, str) and value
    }
    return frozenset(common_names)


def require_peer_identity(
    transport: PeerTransport | None,
    *,
    exact: frozenset[str] = frozenset(),
    prefixes: tuple[str, ...] = (),
) -> str:
    """Require at least one authenticated SAN/CN matching the service policy."""
    identities = peer_identities(transport)
    for identity in sorted(identities):
        if identity in exact or identity.startswith(prefixes):
            return identity
    raise PeerIdentityError("客户端证书身份不被允许")


def certificate_expiry_timestamp(path: str | Path) -> float:
    """Read the certificate not-valid-after time as a UTC Unix timestamp."""
    try:
        certificate = x509.load_pem_x509_certificate(Path(path).read_bytes())
    except (OSError, ValueError) as exc:
        raise TlsConfigurationError(
            f"无法解析 certificate 文件 {path}: {type(exc).__name__}"
        ) from exc
    return certificate.not_valid_after_utc.timestamp()


__all__ = [
    "PeerIdentityError",
    "TlsConfigurationError",
    "TlsFiles",
    "certificate_expiry_timestamp",
    "create_client_ssl_context",
    "create_server_ssl_context",
    "peer_identities",
    "require_peer_identity",
]
