"""使用恒定时间比较实现 Bearer Token 认证。"""

from __future__ import annotations

import hmac


class AuthenticationError(PermissionError):
    """A request did not provide valid authentication."""


class BearerTokenAuthenticator:
    """Validate an HTTP Bearer token without timing-sensitive equality."""

    __slots__ = ("_token",)

    def __init__(self, token: str) -> None:
        if not isinstance(token, str) or not token:
            raise ValueError("Bearer Token 不能为空")
        self._token = token

    def authenticate(self, authorization: str | None) -> None:
        prefix = "Bearer "
        if (
            not isinstance(authorization, str)
            or not authorization.startswith(prefix)
            or not hmac.compare_digest(
                authorization[len(prefix) :].encode("utf-8"),
                self._token.encode("utf-8"),
            )
        ):
            raise AuthenticationError("Bearer Token 缺失或无效")


__all__ = ["AuthenticationError", "BearerTokenAuthenticator"]
