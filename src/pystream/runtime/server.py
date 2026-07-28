"""Worker 共享数据端口上的入站连接分派器。

服务器读取每条 TCP 连接的首个 HELLO 帧，并按 job、上游 task、下游 task
三元组分派给已注册 TaskRuntime。异常断连不会被当作正常流结束，而是显式
通知目标任务失败，防止第一阶段静默产生数据缺口。
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Protocol, cast

from pystream.common import JsonValue, RecordEnvelope
from pystream.observability import log_event
from pystream.runtime.errors import RuntimeConnectionError, RuntimeLifecycleError
from pystream.runtime.protocol import (
    ChannelIdentity,
    Frame,
    FrameType,
    HandshakeError,
    error_frame,
    read_frame,
    records_from_data_batch,
    validate_hello,
    write_frame,
)


class InputConsumer(Protocol):
    """数据服务器向目标 TaskRuntime 投递输入所需的接口。"""

    async def accept_records(
        self,
        identity: ChannelIdentity,
        records: tuple[RecordEnvelope, ...],
    ) -> None:
        """接收同一通道内保持顺序的一批记录。"""

    async def input_closed(self, identity: ChannelIdentity) -> None:
        """接收正常 END_OF_STREAM。"""

    async def input_failed(self, identity: ChannelIdentity, error: BaseException) -> None:
        """接收异常断连或协议错误。"""


@dataclass(slots=True)
class _Registration:
    consumer: InputConsumer
    active_writers: set[asyncio.StreamWriter] = field(default_factory=set)


class DataPlaneServer:
    """在一个 Worker 端口上承载多个下游 Task 的入站连接。"""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 0,
        *,
        start_server: Callable[..., Awaitable[asyncio.AbstractServer]] = asyncio.start_server,
    ) -> None:
        if not host:
            raise ValueError("data host 不能为空")
        if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
            raise ValueError("data port 必须位于 0..65535")
        self.host = host
        self.port = port
        self._start_server = start_server
        self._server: asyncio.AbstractServer | None = None
        self._registrations: dict[ChannelIdentity, _Registration] = {}
        self._lock = asyncio.Lock()
        self._connections_opened = 0
        self._connections_closed = 0
        self._connection_errors = 0
        self._batches_received = 0
        self._records_received = 0

    @property
    def running(self) -> bool:
        """返回监听器是否已经启动。"""
        return self._server is not None

    @property
    def bound_port(self) -> int:
        """返回实际监听端口，支持测试中的端口 0。"""
        server = self._server
        if server is None or not server.sockets:
            return self.port
        return cast(int, server.sockets[0].getsockname()[1])

    @property
    def registered_channel_count(self) -> int:
        """返回当前允许握手的入通道数量。"""
        return len(self._registrations)

    @property
    def active_connection_count(self) -> int:
        """返回当前已完成 HELLO 的活动连接数。"""
        return sum(len(item.active_writers) for item in self._registrations.values())

    @property
    def metrics(self) -> dict[str, int]:
        """返回连接与输入批次累计指标。"""
        return {
            "registered_channels": self.registered_channel_count,
            "active_connections": self.active_connection_count,
            "connections_opened": self._connections_opened,
            "connections_closed": self._connections_closed,
            "connection_errors": self._connection_errors,
            "batches_received": self._batches_received,
            "records_received": self._records_received,
        }

    async def start(self) -> None:
        """启动共享 TCP 监听器。"""
        if self._server is not None:
            return
        self._server = await self._start_server(self._handle_connection, self.host, self.port)

    async def close(self) -> None:
        """停止接收连接并关闭全部活动 writer。"""
        server = self._server
        self._server = None
        if server is not None:
            server.close()
            await server.wait_closed()
        registrations = list(self._registrations.values())
        self._registrations.clear()
        for registration in registrations:
            for writer in tuple(registration.active_writers):
                writer.close()
                with suppress(ConnectionError, OSError, RuntimeError):
                    await writer.wait_closed()

    async def register(
        self,
        identities: tuple[ChannelIdentity, ...],
        consumer: InputConsumer,
    ) -> None:
        """原子注册一个任务的全部预期入通道。"""
        if not self.running:
            raise RuntimeLifecycleError("数据服务器尚未启动")
        if len(set(identities)) != len(identities):
            raise RuntimeLifecycleError("任务入通道身份重复")
        async with self._lock:
            conflicts = [identity for identity in identities if identity in self._registrations]
            if conflicts:
                raise RuntimeLifecycleError(f"入通道已经注册: {conflicts[0]}")
            for identity in identities:
                self._registrations[identity] = _Registration(consumer)

    async def unregister(
        self,
        identities: tuple[ChannelIdentity, ...],
        consumer: InputConsumer,
    ) -> None:
        """移除任务入通道并关闭仍关联的连接。"""
        async with self._lock:
            registrations: list[_Registration] = []
            for identity in identities:
                registration = self._registrations.get(identity)
                if registration is not None and registration.consumer is consumer:
                    registrations.append(registration)
                    del self._registrations[identity]
        for registration in registrations:
            for writer in tuple(registration.active_writers):
                writer.close()
                with suppress(ConnectionError, OSError, RuntimeError):
                    await writer.wait_closed()

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        identity: ChannelIdentity | None = None
        registration: _Registration | None = None
        ended_normally = False
        try:
            hello = await read_frame(reader)
            identity = _identity_from_hello(hello)
            registration = self._registrations.get(identity)
            if registration is None:
                raise HandshakeError(f"没有为 HELLO 注册目标任务: {identity}")
            if registration.active_writers:
                raise HandshakeError(f"通道已有活动连接: {identity}")
            validate_hello(hello, identity)
            registration.active_writers.add(writer)
            self._connections_opened += 1
            self._log_connection(logging.INFO, "connection_opened", "数据连接已建立", identity)
            await self._read_stream(reader, registration.consumer, identity)
            ended_normally = True
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._connection_errors += 1
            self._log_connection(
                logging.ERROR,
                "connection_failed",
                "数据连接失败",
                identity,
                error=f"{type(exc).__name__}: {exc}",
                exc_info=exc,
            )
            if registration is not None and identity is not None:
                await registration.consumer.input_failed(identity, exc)
            else:
                with suppress(Exception):
                    await write_frame(
                        writer,
                        error_frame("HANDSHAKE_FAILED", str(exc)),
                    )
        finally:
            if registration is not None:
                registration.active_writers.discard(writer)
                self._connections_closed += 1
                self._log_connection(
                    logging.INFO,
                    "connection_closed",
                    "数据连接已关闭",
                    identity,
                    normal=ended_normally,
                )
            writer.close()
            with suppress(ConnectionError, OSError, RuntimeError):
                await writer.wait_closed()
            if not ended_normally and registration is not None and identity is not None:
                # _read_stream 的错误路径已经通知过 consumer; 这里只保留显式分支,
                # 防止未来修改把异常 EOF 当作正常结束。
                pass

    async def _read_stream(
        self,
        reader: asyncio.StreamReader,
        consumer: InputConsumer,
        identity: ChannelIdentity,
    ) -> None:
        heartbeat_sequence = -1
        while True:
            frame = await read_frame(reader)
            if frame.frame_type is FrameType.DATA_BATCH:
                records = records_from_data_batch(frame)
                await consumer.accept_records(identity, records)
                self._batches_received += 1
                self._records_received += len(records)
                continue
            if frame.frame_type is FrameType.HEARTBEAT:
                sequence = frame.payload.get("sequence")
                if (
                    isinstance(sequence, bool)
                    or not isinstance(sequence, int)
                    or sequence <= heartbeat_sequence
                ):
                    raise RuntimeConnectionError("HEARTBEAT sequence 必须严格递增")
                heartbeat_sequence = sequence
                continue
            if frame.frame_type is FrameType.END_OF_STREAM:
                if frame.payload:
                    raise RuntimeConnectionError("END_OF_STREAM payload 必须为空")
                await consumer.input_closed(identity)
                return
            if frame.frame_type is FrameType.ERROR:
                code = frame.payload.get("code")
                message = frame.payload.get("message")
                raise RuntimeConnectionError(f"远端任务错误 {code}: {message}")
            raise RuntimeConnectionError(f"握手后不允许帧类型 {frame.frame_type.value}")

    @staticmethod
    def _log_connection(
        level: int,
        event: str,
        message: str,
        identity: ChannelIdentity | None,
        *,
        exc_info: BaseException | bool | None = None,
        **fields,
    ) -> None:
        operator_id: str | None = None
        subtask: int | None = None
        if identity is not None:
            try:
                _, operator_id, raw_subtask = identity.downstream_task_id.rsplit(":", 2)
                subtask = int(raw_subtask)
            except (TypeError, ValueError):
                operator_id = None
                subtask = None
        log_event(
            logging.getLogger(__name__),
            level,
            event,
            message,
            component="data_plane_server",
            job_id=identity.job_id if identity is not None else None,
            operator_id=operator_id,
            subtask=subtask,
            exc_info=exc_info,
            upstream_task_id=(identity.upstream_task_id if identity is not None else None),
            downstream_task_id=(identity.downstream_task_id if identity is not None else None),
            **fields,
        )


def _identity_from_hello(frame: Frame) -> ChannelIdentity:
    if frame.frame_type is not FrameType.HELLO:
        raise HandshakeError(f"首帧必须是 HELLO, 实际为 {frame.frame_type.value}")
    payload: dict[str, JsonValue] = frame.payload
    required = {"job_id", "upstream_task_id", "downstream_task_id"}
    if set(payload) != required:
        raise HandshakeError("HELLO 字段必须恰好为 job_id/upstream_task_id/downstream_task_id")
    values = [payload[field] for field in sorted(required)]
    if not all(isinstance(value, str) for value in values):
        raise HandshakeError("HELLO 身份字段必须是字符串")
    return ChannelIdentity(
        job_id=cast(str, payload["job_id"]),
        upstream_task_id=cast(str, payload["upstream_task_id"]),
        downstream_task_id=cast(str, payload["downstream_task_id"]),
    )


__all__ = ["DataPlaneServer", "InputConsumer"]
