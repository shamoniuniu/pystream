"""JobManager HTTP 客户端的请求契约与错误边界测试。"""

from __future__ import annotations

import json

import pytest

from pystream.client import ClientError, HttpResponse, JobManagerClient


class FakeTransport:
    """记录请求并返回预设响应的同步传输。"""

    def __init__(self, *responses: HttpResponse) -> None:
        self.responses = list(responses)
        self.requests: list[dict[str, object]] = []

    def request(self, method, url, *, body, headers, timeout):
        self.requests.append(
            {
                "method": method,
                "url": url,
                "body": body,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return self.responses.pop(0)


def json_response(status: int, payload: dict[str, object]) -> HttpResponse:
    """构造 UTF-8 JSON 响应。"""
    return HttpResponse(status, json.dumps(payload, ensure_ascii=False).encode())


def test_submit_上传_zip_摘要和文件名():
    transport = FakeTransport(json_response(201, {"job_id": "job-1", "status": "SUBMITTED"}))
    client = JobManagerClient("http://manager:8080/", timeout=3, transport=transport)

    result = client.submit(b"zip-content", "a" * 64, "artifact.zip")

    assert result == {"job_id": "job-1", "status": "SUBMITTED"}
    assert transport.requests == [
        {
            "method": "POST",
            "url": "http://manager:8080/v1/jobs",
            "body": b"zip-content",
            "headers": {
                "Accept": "application/json",
                "Content-Type": "application/zip",
                "X-PyStream-SHA256": "a" * 64,
                "X-PyStream-Filename": "artifact.zip",
            },
            "timeout": 3,
        }
    ]


def test_status_对_job_id_进行_url_编码():
    transport = FakeTransport(json_response(200, {"job_id": "a/b", "status": "RUNNING"}))
    client = JobManagerClient("http://manager", transport=transport)

    client.status("a/b")

    assert transport.requests[0]["url"] == "http://manager/v1/jobs/a%2Fb"


def test_cancel_发送空_post_请求():
    transport = FakeTransport(json_response(202, {"job_id": "job-1", "status": "CANCELLING"}))
    client = JobManagerClient("http://manager", transport=transport)

    client.cancel("job-1")

    assert transport.requests[0]["method"] == "POST"
    assert transport.requests[0]["url"].endswith("/v1/jobs/job-1/cancel")
    assert transport.requests[0]["body"] == b""


def test_http_json_错误包含状态码和服务端详情():
    transport = FakeTransport(json_response(409, {"error": "状态冲突"}))
    client = JobManagerClient("http://manager", transport=transport)

    with pytest.raises(ClientError, match="HTTP 409: 状态冲突"):
        client.cancel("job-1")


def test_defect_probing_http_文本错误仍保留状态码():
    transport = FakeTransport(HttpResponse(500, b"internal failure"))
    client = JobManagerClient("http://manager", transport=transport)

    with pytest.raises(ClientError, match="HTTP 500"):
        client.status("job-1")


@pytest.mark.parametrize(
    "response",
    [
        HttpResponse(200, b"not-json"),
        HttpResponse(200, b"[]"),
    ],
)
def test_成功响应必须是_json_object(response):
    client = JobManagerClient("http://manager", transport=FakeTransport(response))

    with pytest.raises(ClientError, match=r"非法 JSON|必须是 object"):
        client.status("job-1")


@pytest.mark.parametrize(
    ("base_url", "timeout", "message"),
    [
        ("manager:8080", 1, "必须以 http"),
        ("http://manager", 0, "必须大于 0"),
        ("http://manager", -1, "必须大于 0"),
    ],
)
def test_客户端拒绝非法_url_和超时(base_url, timeout, message):
    with pytest.raises(ClientError, match=message):
        JobManagerClient(base_url, timeout=timeout)
