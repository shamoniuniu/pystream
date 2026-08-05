"""PyStream 传输认证与文件 Secret 契约。"""

from pystream.security.auth import AuthenticationError, BearerTokenAuthenticator
from pystream.security.secrets import SecretFileError, read_secret_file
from pystream.security.tls import (
    PeerIdentityError,
    TlsConfigurationError,
    TlsFiles,
    certificate_expiry_timestamp,
    create_client_ssl_context,
    create_server_ssl_context,
    peer_identities,
    require_peer_identity,
)

__all__ = [
    "AuthenticationError",
    "BearerTokenAuthenticator",
    "PeerIdentityError",
    "SecretFileError",
    "TlsConfigurationError",
    "TlsFiles",
    "certificate_expiry_timestamp",
    "create_client_ssl_context",
    "create_server_ssl_context",
    "peer_identities",
    "read_secret_file",
    "require_peer_identity",
]
