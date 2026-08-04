"""基于 S3 条件请求的对象存储适配器。"""

from __future__ import annotations

from contextlib import suppress
from typing import Any
from uuid import uuid4

from pystream.storage.errors import (
    ObjectConflict,
    ObjectNotFound,
    ObjectStoreError,
)
from pystream.storage.ports import ObjectValue

_NOT_FOUND_CODES = frozenset({"404", "NoSuchKey", "NotFound"})
_CONFLICT_CODES = frozenset(
    {
        "409",
        "412",
        "ConditionalRequestConflict",
        "PreconditionFailed",
    }
)


class S3ObjectStore:
    """把 S3 client 响应收敛为稳定的条件对象语义。"""

    def __init__(
        self,
        bucket: str,
        *,
        client: Any | None = None,
        endpoint_url: str | None = None,
        region_name: str = "us-east-1",
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        verify: bool | str = True,
    ) -> None:
        if not bucket:
            raise ValueError("bucket 不能为空")
        self.bucket = bucket
        if client is None:
            try:
                import boto3
                from botocore.config import Config
            except ImportError as exc:  # pragma: no cover - 生产依赖安装保证
                raise ObjectStoreError("缺少 boto3 依赖") from exc
            client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                region_name=region_name,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                verify=verify,
                config=Config(
                    signature_version="s3v4",
                    s3={"addressing_style": "path"},
                    connect_timeout=5,
                    read_timeout=30,
                    retries={"max_attempts": 3, "mode": "standard"},
                ),
            )
        self._client = client

    def get(self, key: str) -> ObjectValue:
        """读取完整对象，并规范化服务端 ETag。"""
        _validate_key(key)
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
            content = response["Body"].read()
            etag = _normalize_etag(response["ETag"])
        except Exception as exc:
            raise _translate_error("get", key, exc) from exc
        if not isinstance(content, bytes):
            raise ObjectStoreError(f"S3 get {key!r} 返回非 bytes Body")
        return ObjectValue(content=content, etag=etag)

    def put_if_absent(self, key: str, content: bytes) -> str:
        """使用 ``If-None-Match: *`` 创建 immutable 对象。"""
        _validate_write(key, content)
        try:
            response = self._client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=content,
                IfNoneMatch="*",
            )
            return _normalize_etag(response["ETag"])
        except Exception as exc:
            raise _translate_error("put_if_absent", key, exc) from exc

    def put_if_match(self, key: str, content: bytes, etag: str) -> str:
        """使用 ETag 前置条件更新单个 mutable pointer。"""
        _validate_write(key, content)
        if not isinstance(etag, str) or not etag:
            raise ValueError("etag 必须是非空字符串")
        try:
            response = self._client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=content,
                IfMatch=f'"{etag}"',
            )
            return _normalize_etag(response["ETag"])
        except Exception as exc:
            raise _translate_error("put_if_match", key, exc) from exc

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        """遍历 continuation token，返回稳定排序结果。"""
        if not isinstance(prefix, str):
            raise TypeError("prefix 必须是字符串")
        keys: list[str] = []
        continuation_token: str | None = None
        while True:
            arguments: dict[str, object] = {
                "Bucket": self.bucket,
                "Prefix": prefix,
            }
            if continuation_token is not None:
                arguments["ContinuationToken"] = continuation_token
            try:
                response = self._client.list_objects_v2(**arguments)
            except Exception as exc:
                raise _translate_error("list", prefix, exc) from exc
            contents = response.get("Contents", [])
            if not isinstance(contents, list):
                raise ObjectStoreError("S3 list Contents 不是数组")
            for item in contents:
                key = item.get("Key") if isinstance(item, dict) else None
                if not isinstance(key, str):
                    raise ObjectStoreError("S3 list 返回非法 Key")
                keys.append(key)
            if not response.get("IsTruncated", False):
                break
            raw_token = response.get("NextContinuationToken")
            if not isinstance(raw_token, str) or not raw_token:
                raise ObjectStoreError("S3 list 分页缺少 continuation token")
            continuation_token = raw_token
        return tuple(sorted(keys))

    def probe_conditional_writes(
        self,
        *,
        prefix: str = "pystream/capability-probes",
    ) -> None:
        """验证部署所依赖的 create-if-absent、CAS、读取和列表语义。"""
        normalized_prefix = prefix.strip("/")
        if not normalized_prefix:
            raise ValueError("capability probe prefix 不能为空")
        key = f"{normalized_prefix}/{uuid4().hex}.json"
        initial = b'{"generation":1}'
        updated = b'{"generation":2}'
        try:
            initial_etag = self.put_if_absent(key, initial)
            try:
                self.put_if_absent(key, initial)
            except ObjectConflict:
                pass
            else:
                raise ObjectStoreError("S3 capability probe: If-None-Match 未拒绝覆盖")

            updated_etag = self.put_if_match(key, updated, initial_etag)
            try:
                self.put_if_match(key, initial, initial_etag)
            except ObjectConflict:
                pass
            else:
                raise ObjectStoreError("S3 capability probe: stale If-Match 未被拒绝")

            observed = self.get(key)
            if observed.content != updated or observed.etag != updated_etag:
                raise ObjectStoreError("S3 capability probe: 条件写后读取结果不一致")
            if key not in self.list_keys(f"{normalized_prefix}/"):
                raise ObjectStoreError("S3 capability probe: list 未返回刚写入的对象")
        finally:
            self._cleanup_probe(key)

    def _cleanup_probe(self, key: str) -> None:
        """Probe 清理是 best-effort；生产端口本身不依赖 delete 能力。"""
        with suppress(Exception):
            self._client.delete_object(Bucket=self.bucket, Key=key)


def _validate_key(key: str) -> None:
    if not isinstance(key, str) or not key or key.startswith("/"):
        raise ValueError("object key 必须是非空相对字符串")


def _validate_write(key: str, content: bytes) -> None:
    _validate_key(key)
    if not isinstance(content, bytes):
        raise TypeError("object content 必须是 bytes")


def _normalize_etag(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ObjectStoreError("S3 响应缺少 ETag")
    normalized = value.strip('"')
    if not normalized:
        raise ObjectStoreError("S3 ETag 不能为空")
    return normalized


def _translate_error(operation: str, key: str, error: Exception) -> ObjectStoreError:
    response = getattr(error, "response", None)
    code: str | None = None
    status: str | None = None
    if isinstance(response, dict):
        details = response.get("Error")
        if isinstance(details, dict):
            raw_code = details.get("Code")
            if raw_code is not None:
                code = str(raw_code)
        metadata = response.get("ResponseMetadata")
        if isinstance(metadata, dict):
            raw_status = metadata.get("HTTPStatusCode")
            if raw_status is not None:
                status = str(raw_status)
    message = f"S3 {operation} {key!r} 失败"
    if code in _NOT_FOUND_CODES or status in _NOT_FOUND_CODES:
        return ObjectNotFound(message)
    if code in _CONFLICT_CODES or status in _CONFLICT_CODES:
        return ObjectConflict(message)
    return ObjectStoreError(f"{message}: {type(error).__name__}: {error}")


__all__ = ["S3ObjectStore"]
