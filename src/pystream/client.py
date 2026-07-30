"""PyStream CLI 使用的 JobManager HTTP 客户端。

本模块只负责低频控制面请求：上传作业制品、查询状态和取消作业。传输层可注入，
因此 CLI 测试不需要启动真实 JobManager；默认实现使用标准库 HTTP 客户端。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


class ClientError(RuntimeError):
    """JobManager 客户端可呈现给 CLI 用户的错误。"""


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """传输层返回的最小 HTTP 响应。"""

    status: int
    body: bytes


class HttpTransport(Protocol):
    """可替换 HTTP 传输端口。"""

    def request(
        self,
        method: str,
        url: str,
        *,
        body: bytes | None,
        headers: dict[str, str],
        timeout: float,
    ) -> HttpResponse:
        """发送一次请求并返回状态码和响应体。"""


class UrllibTransport:
    """基于 Python 标准库的同步 HTTP 传输。"""

    def request(
        self,
        method: str,
        url: str,
        *,
        body: bytes | None,
        headers: dict[str, str],
        timeout: float,
    ) -> HttpResponse:
        """发送 HTTP 请求，并保留 HTTP 错误响应供上层统一解析。"""
        request = Request(url, data=body, headers=headers, method=method)
        try:
            with urlopen(request, timeout=timeout) as response:
                return HttpResponse(status=response.status, body=response.read())
        except HTTPError as exc:
            return HttpResponse(status=exc.code, body=exc.read())
        except (URLError, TimeoutError, OSError) as exc:
            reason = getattr(exc, "reason", exc)
            raise ClientError(f"无法连接 JobManager {url}: {reason}") from exc


class JobManagerClient:
    """面向 CLI 的 JobManager v1 API 客户端。"""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 10.0,
        transport: HttpTransport | None = None,
    ) -> None:
        normalized = base_url.rstrip("/")
        if not normalized.startswith(("http://", "https://")):
            raise ClientError("JobManager URL 必须以 http:// 或 https:// 开头")
        if timeout <= 0:
            raise ClientError("HTTP timeout 必须大于 0")
        self.base_url = normalized
        self.timeout = timeout
        self.transport = transport or UrllibTransport()

    def submit(self, artifact: bytes, sha256: str, filename: str) -> dict[str, object]:
        """上传不可变 ZIP 制品并返回 job_id 与初始状态。"""
        return self._json_request(
            "POST",
            "/v1/jobs",
            body=artifact,
            headers={
                "Content-Type": "application/zip",
                "X-PyStream-SHA256": sha256,
                "X-PyStream-Filename": filename,
            },
            expected_statuses={200, 201, 202},
        )

    def status(self, job_id: str) -> dict[str, object]:
        """查询作业、任务位置和最后错误。"""
        return self._json_request(
            "GET",
            f"/v1/jobs/{quote(job_id, safe='')}",
            expected_statuses={200},
        )

    def trigger_checkpoint(self, job_id: str) -> dict[str, object]:
        """立即触发一次完整 Checkpoint 并返回 manifest。"""
        return self._json_request(
            "POST",
            f"/v1/jobs/{quote(job_id, safe='')}/checkpoint",
            body=b"",
            expected_statuses={200},
        )

    def cancel(self, job_id: str) -> dict[str, object]:
        """请求取消作业并返回当前状态。"""
        return self._json_request(
            "POST",
            f"/v1/jobs/{quote(job_id, safe='')}/cancel",
            body=b"",
            expected_statuses={200, 202},
        )

    def _json_request(
        self,
        method: str,
        path: str,
        *,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        expected_statuses: set[int],
    ) -> dict[str, object]:
        request_headers = {
            "Accept": "application/json",
            **(headers or {}),
        }
        response = self.transport.request(
            method,
            f"{self.base_url}{path}",
            body=body,
            headers=request_headers,
            timeout=self.timeout,
        )
        if response.status not in expected_statuses:
            detail = _error_detail(response.body)
            suffix = f": {detail}" if detail else ""
            raise ClientError(f"JobManager HTTP {response.status}{suffix}")
        return _decode_json(response.body)


def _decode_json(body: bytes) -> dict[str, object]:
    if not body:
        return {}
    try:
        document = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ClientError(f"JobManager 返回了非法 JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise ClientError("JobManager JSON 响应必须是 object")
    return document


def _error_detail(body: bytes) -> object | None:
    if not body:
        return None
    try:
        payload = _decode_json(body)
    except ClientError:
        text = body.decode("utf-8", errors="replace").strip()
        return text[:500] or None
    return payload.get("error") or payload.get("detail") or payload.get("message")


__all__ = [
    "ClientError",
    "HttpResponse",
    "HttpTransport",
    "JobManagerClient",
    "UrllibTransport",
]
