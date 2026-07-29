"""带 HELLO 握手、有界队列和批量发送的数据输出通道。"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from enum import StrEnum

from pystream.common import MessageType, RecordEnvelope
from pystream.runtime.protocol import (
    DEFAULT_MAX_BATCH_RECORDS,
    DEFAULT_MAX_FRAME_SIZE,
    AsyncFrameWriter,
    ChannelIdentity,
    control_frame,
    data_batch_frame,
    end_of_stream_frame,
    hello_frame,
    write_frame,
)


class ChannelError(RuntimeError):
    """数据通道生命周期或发送过程失败。"""


class ChannelClosedError(ChannelError):
    """调用方尝试向未运行的通道发送。"""


class ChannelState(StrEnum):
    """输出通道生命周期状态。"""

    NEW = "NEW"
    RUNNING = "RUNNING"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    FAILED = "FAILED"


_CLOSE = object()


class BoundedDataChannel:
    """通过有界队列串行发送记录批次，并把下游慢速传播为上游等待。"""

    def __init__(
        self,
        writer: AsyncFrameWriter,
        identity: ChannelIdentity,
        *,
        queue_capacity: int = 1_024,
        batch_size: int = 100,
        max_batch_records: int = DEFAULT_MAX_BATCH_RECORDS,
        max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
    ) -> None:
        if isinstance(queue_capacity, bool) or queue_capacity <= 0:
            raise ValueError("queue_capacity 必须是正整数")
        if isinstance(batch_size, bool) or batch_size <= 0:
            raise ValueError("batch_size 必须是正整数")
        if batch_size > max_batch_records:
            raise ValueError("batch_size 不能超过 max_batch_records")
        self._writer = writer
        self._identity = identity
        self._queue: asyncio.Queue[RecordEnvelope | object] = asyncio.Queue(maxsize=queue_capacity)
        self._batch_size = batch_size
        self._max_batch_records = max_batch_records
        self._max_frame_size = max_frame_size
        self._state = ChannelState.NEW
        self._sender_task: asyncio.Task[None] | None = None
        self._failure: BaseException | None = None
        self._records_sent = 0
        self._batches_sent = 0
        self._control_frames_sent = 0
        self._max_queue_depth = 0

    @property
    def state(self) -> ChannelState:
        """返回当前生命周期状态。"""
        return self._state

    @property
    def queue_size(self) -> int:
        """返回当前等待发送的记录或关闭信号数量。"""
        return self._queue.qsize()

    @property
    def queue_capacity(self) -> int:
        """返回有界队列容量。"""
        return self._queue.maxsize

    @property
    def max_queue_depth(self) -> int:
        """返回通道生命周期内观察到的最高队列深度。"""
        return self._max_queue_depth

    @property
    def records_sent(self) -> int:
        """返回已成功 drain 的记录数。"""
        return self._records_sent

    @property
    def batches_sent(self) -> int:
        """返回已成功 drain 的 DATA_BATCH 数。"""
        return self._batches_sent

    @property
    def control_frames_sent(self) -> int:
        """返回已成功 drain 的 CONTROL 帧数。"""
        return self._control_frames_sent

    async def start(self) -> None:
        """发送 HELLO 后启动唯一发送协程。"""
        if self._state is not ChannelState.NEW:
            raise ChannelError(f"通道只能启动一次, 当前状态 {self._state.value}")
        try:
            await write_frame(
                self._writer,
                hello_frame(self._identity),
                max_frame_size=self._max_frame_size,
            )
        except BaseException as exc:
            self._state = ChannelState.FAILED
            self._failure = exc
            await self._close_writer()
            raise ChannelError("发送 HELLO 失败") from exc
        self._state = ChannelState.RUNNING
        self._sender_task = asyncio.create_task(
            self._run_sender(),
            name=f"pystream-channel-{self._identity.upstream_task_id}"
            f"-to-{self._identity.downstream_task_id}",
        )

    async def send(self, record: RecordEnvelope) -> None:
        """等待有界队列腾出空间后接收一条记录。"""
        if not isinstance(record, RecordEnvelope):
            raise TypeError("record 必须是 RecordEnvelope")
        if self._state is not ChannelState.RUNNING:
            self._raise_not_running()
        await self._guarded_put(record)

    async def send_many(self, records: list[RecordEnvelope] | tuple[RecordEnvelope, ...]) -> None:
        """按调用顺序发送多条记录。"""
        for record in records:
            await self.send(record)

    async def close(self) -> None:
        """排空已接收记录、发送 END_OF_STREAM 并关闭 writer。"""
        if self._state is ChannelState.CLOSED:
            return
        if self._state is ChannelState.NEW:
            self._state = ChannelState.CLOSED
            await self._close_writer()
            return
        if self._state is ChannelState.FAILED:
            await self._close_writer()
            self._raise_failure()
        if self._state is ChannelState.CLOSING:
            await self.wait_closed()
            return

        self._state = ChannelState.CLOSING
        try:
            await self._guarded_put(_CLOSE, allow_closing=True)
            if self._sender_task is not None:
                await self._sender_task
            await write_frame(
                self._writer,
                end_of_stream_frame(),
                max_frame_size=self._max_frame_size,
            )
            self._state = ChannelState.CLOSED
        except BaseException as exc:
            self._failure = exc
            self._state = ChannelState.FAILED
            raise ChannelError("关闭数据通道失败") from exc
        finally:
            await self._close_writer()

    async def abort(self) -> None:
        """立即取消发送任务并关闭连接，不发送 END_OF_STREAM。"""
        if self._sender_task is not None and not self._sender_task.done():
            self._sender_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._sender_task
        self._state = ChannelState.CLOSED
        await self._close_writer()

    async def wait_closed(self) -> None:
        """等待发送任务结束；失败时向调用方传播原因。"""
        if self._sender_task is not None:
            try:
                await self._sender_task
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                self._failure = exc
                self._state = ChannelState.FAILED
        if self._state is ChannelState.FAILED:
            self._raise_failure()

    async def _guarded_put(
        self, item: RecordEnvelope | object, *, allow_closing: bool = False
    ) -> None:
        sender = self._sender_task
        if sender is None:
            raise ChannelClosedError("发送协程尚未启动")
        if not allow_closing and self._state is not ChannelState.RUNNING:
            self._raise_not_running()
        put_task = asyncio.create_task(self._queue.put(item))
        done, _ = await asyncio.wait(
            {put_task, sender},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if put_task in done:
            await put_task
            self._max_queue_depth = max(self._max_queue_depth, self._queue.qsize())
            return
        if sender in done:
            if not put_task.done():
                put_task.cancel()
                with suppress(asyncio.CancelledError):
                    await put_task
            try:
                sender.result()
            except BaseException as exc:
                self._failure = exc
                self._state = ChannelState.FAILED
                raise ChannelError("发送协程已经失败") from exc
            raise ChannelClosedError("发送协程已经结束")

    async def _run_sender(self) -> None:
        pending: RecordEnvelope | object | None = None
        try:
            while True:
                if pending is None:
                    first = await self._queue.get()
                else:
                    first = pending
                    pending = None
                if first is _CLOSE:
                    self._queue.task_done()
                    break
                if not isinstance(first, RecordEnvelope):  # pragma: no cover - 队列封装保证
                    self._queue.task_done()
                    raise ChannelError("输出队列包含未知消息")
                if first.message_type is not MessageType.DATA:
                    try:
                        await write_frame(
                            self._writer,
                            control_frame(first),
                            max_frame_size=self._max_frame_size,
                        )
                    finally:
                        self._queue.task_done()
                    self._control_frames_sent += 1
                    continue

                batch = [first]
                while len(batch) < self._batch_size:
                    try:
                        item = self._queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if item is _CLOSE or (
                        isinstance(item, RecordEnvelope)
                        and item.message_type is not MessageType.DATA
                    ):
                        pending = item
                        break
                    batch.append(item)

                records = [record for record in batch if isinstance(record, RecordEnvelope)]
                try:
                    await write_frame(
                        self._writer,
                        data_batch_frame(
                            records,
                            max_batch_records=self._max_batch_records,
                        ),
                        max_frame_size=self._max_frame_size,
                    )
                finally:
                    for _ in batch:
                        self._queue.task_done()
                self._records_sent += len(records)
                self._batches_sent += 1
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._failure = exc
            self._state = ChannelState.FAILED
            raise

    async def _close_writer(self) -> None:
        self._writer.close()
        with suppress(ConnectionError, OSError, RuntimeError):
            await self._writer.wait_closed()

    def _raise_not_running(self) -> None:
        if self._state is ChannelState.FAILED:
            self._raise_failure()
        raise ChannelClosedError(f"通道未运行, 当前状态 {self._state.value}")

    def _raise_failure(self) -> None:
        raise ChannelError("数据通道已失败") from self._failure


__all__ = [
    "BoundedDataChannel",
    "ChannelClosedError",
    "ChannelError",
    "ChannelState",
]
