"""单个物理任务的异步执行循环与跨节点路由。

TaskRuntime 复用控制面的 TaskDeployment、算子接口和数据通道。Source 任务从
异步连接器产生记录；普通任务把多个入通道合流到有界队列，再调用同步算子。
第一阶段没有断线恢复：异常 EOF、协议错误或算子异常会立即上报 FAILED。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

from pystream.api import Partitioning
from pystream.artifact import UDFLoader
from pystream.checkpoint import (
    CheckpointError,
    CheckpointStore,
    TaskSnapshotDescriptor,
    TransactionDescriptor,
    decode_state,
    encode_state,
)
from pystream.common import MessageType, RecordEnvelope, utc_now
from pystream.control import PhysicalChannel, TaskDeployment
from pystream.observability import log_event
from pystream.operators import OperatorTask
from pystream.runtime.channel import BoundedDataChannel
from pystream.runtime.errors import (
    RuntimeConnectionError,
    RuntimeLifecycleError,
    RuntimeTaskError,
)
from pystream.runtime.protocol import ChannelIdentity
from pystream.runtime.routing import ShuffleRouter
from pystream.runtime.server import DataPlaneServer


class AsyncRecordSource(Protocol):
    """TaskRuntime 驱动 Source 所需的异步接口。"""

    async def open(self) -> None:
        """建立外部输入连接。"""

    def records(self) -> AsyncIterator[RecordEnvelope]:
        """持续产生记录。"""

    async def commit(self) -> None:
        """提交已经成功发送到下游通道的输入位置。"""

    def acknowledge(self, record: RecordEnvelope) -> None:
        """确认一条 DATA 已成功排入全部下游。"""

    async def pause(self) -> None:
        """暂停外部输入并阻止产生新的记录。"""

    async def resume(self) -> None:
        """恢复外部输入。"""

    def snapshot_state(self) -> bytes:
        """返回版本化 Source 状态。"""

    def snapshot_checkpoint(self, checkpoint_id: int) -> bytes:
        """冻结指定 Barrier checkpoint 的 Source 状态。"""

    async def commit_checkpoint(self, checkpoint_id: int | None = None) -> None:
        """提交指定 frozen checkpoint；None 为 DRAIN 兼容路径。"""

    def abort_checkpoint(self, checkpoint_id: int) -> None:
        """丢弃指定 frozen checkpoint。"""

    async def restore_state(self, snapshot: bytes) -> None:
        """从版本化快照恢复外部输入位置和时间基线。"""

    def prepare_restored_checkpoint(
        self,
        checkpoint_id: int,
        snapshot: bytes,
    ) -> None:
        """从 durable decision 重建可提交的 frozen input position。"""

    async def close(self) -> None:
        """释放输入连接。"""


class TaskRuntimeState(StrEnum):
    """Worker 本地任务生命周期。"""

    CREATED = "CREATED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


FailureCallback = Callable[[str, str, BaseException], Awaitable[None]]
OpenConnection = Callable[
    [str, int],
    Awaitable[tuple[asyncio.StreamReader, asyncio.StreamWriter]],
]


@dataclass(frozen=True, slots=True)
class RuntimeSnapshot:
    """Worker 状态接口返回的不可变任务快照。"""

    task_id: str
    job_id: str
    operator_id: str
    subtask_index: int
    state: TaskRuntimeState
    error: str | None
    input_channels: int
    closed_input_channels: int
    output_channels: int
    records_in: int
    records_out: int
    input_queue_depth: int = 0
    input_queue_capacity: int = 0
    output_queue_depth: int = 0
    output_queue_capacity: int = 0
    max_output_queue_depth: int = 0
    batches_out: int = 0
    errors: int = 0
    operator_metrics: dict[str, int] = field(default_factory=dict)
    attempt_id: int = 0
    coordinator_epoch: int = 0
    restored_checkpoint_id: int | None = None
    barrier_alignment_duration_ms: int = 0
    barrier_blocked_inputs: int = 0
    source_pause_duration_ms: int = 0


@dataclass(frozen=True, slots=True)
class _InputEnded:
    identity: ChannelIdentity


@dataclass(frozen=True, slots=True)
class _InputRecord:
    identity: ChannelIdentity
    record: RecordEnvelope


@dataclass(slots=True)
class _OutputRoute:
    partitioning: Partitioning
    channels: tuple[tuple[int, BoundedDataChannel], ...]
    router: ShuffleRouter | None

    def select(self, record: RecordEnvelope) -> BoundedDataChannel:
        if self.partitioning is Partitioning.FORWARD:
            return self.channels[0][1]
        if self.router is None:  # pragma: no cover - 构造器保证
            raise RuntimeConnectionError("Shuffle route 缺少路由器")
        selected = self.router.route(record)
        for subtask, channel in self.channels:
            if subtask == selected:
                return channel
        raise RuntimeConnectionError(f"物理执行图缺少目标 subtask {selected}")


class TaskRuntime:
    """驱动一个 Source 或同步算子，并管理它的全部数据通道。"""

    def __init__(
        self,
        deployment: TaskDeployment,
        data_server: DataPlaneServer,
        *,
        source: AsyncRecordSource | None = None,
        operator: OperatorTask[RecordEnvelope] | None = None,
        udf_loader: UDFLoader | None = None,
        failure_callback: FailureCallback | None = None,
        open_connection: OpenConnection = asyncio.open_connection,
        input_queue_capacity: int = 1_024,
        channel_queue_capacity: int = 1_024,
        channel_batch_size: int = 100,
        timer_interval: float = 0.1,
        watermark_idle_timeout: float | None = None,
        monotonic_clock: Callable[[], float] = time.monotonic,
        checkpoint_enabled: bool = False,
        aligned_checkpoints: bool = False,
        checkpoint_store: CheckpointStore | None = None,
    ) -> None:
        if (source is None) == (operator is None):
            raise ValueError("TaskRuntime 必须且只能配置 source 或 operator")
        if input_queue_capacity <= 0 or channel_queue_capacity <= 0:
            raise ValueError("队列容量必须大于 0")
        if timer_interval <= 0:
            raise ValueError("timer_interval 必须大于 0")
        if watermark_idle_timeout is not None and watermark_idle_timeout <= 0:
            raise ValueError("watermark_idle_timeout 必须大于 0")
        if aligned_checkpoints and not checkpoint_enabled:
            raise ValueError("aligned_checkpoints 要求启用 checkpoint")
        self.deployment = deployment
        self.data_server = data_server
        self.source = source
        self.operator = operator
        self.udf_loader = udf_loader
        self._failure_callback = failure_callback
        self._open_connection = open_connection
        self._channel_queue_capacity = channel_queue_capacity
        self._channel_batch_size = channel_batch_size
        self._timer_interval = timer_interval
        self._watermark_idle_timeout = watermark_idle_timeout
        self._monotonic_clock = monotonic_clock
        self._checkpoint_enabled = checkpoint_enabled
        self._aligned_checkpoints = aligned_checkpoints
        self._checkpoint_store = checkpoint_store
        self._input_queue: asyncio.Queue[_InputRecord | _InputEnded] = asyncio.Queue(
            maxsize=input_queue_capacity
        )
        self._state = TaskRuntimeState.CREATED
        self._error: str | None = None
        self._run_task: asyncio.Task[None] | None = None
        self._cleanup_lock = asyncio.Lock()
        self._cleaned = False
        self._registered = False
        self._stopping = False
        self._closed_inputs: set[ChannelIdentity] = set()
        self._records_in = 0
        self._records_out = 0
        self._failure_count = 0
        self._routes: tuple[_OutputRoute, ...] = ()
        self._all_output_channels: tuple[BoundedDataChannel, ...] = ()
        task = deployment.task
        attempt_id = getattr(task, "attempt_id", 0)
        self._incoming_identities = tuple(
            ChannelIdentity(
                task.job_id,
                channel.source_task_id,
                channel.target_task_id,
                attempt_id=attempt_id,
                coordinator_epoch=deployment.coordinator_epoch,
            )
            for channel in deployment.incoming_channels
        )
        now = self._monotonic_clock()
        self._input_watermarks: dict[ChannelIdentity, datetime | None] = {
            identity: None for identity in self._incoming_identities
        }
        self._last_input_activity: dict[ChannelIdentity, float] = {
            identity: now for identity in self._incoming_identities
        }
        self._idle_inputs: set[ChannelIdentity] = set()
        self._last_output_watermark: datetime | None = None
        self._checkpoint_lock = asyncio.Lock()
        self._active_checkpoint_id: int | None = None
        self._checkpoint_drains: set[ChannelIdentity] = set()
        self._checkpoint_barriers: set[ChannelIdentity] = set()
        self._barrier_gates: dict[ChannelIdentity, asyncio.Event] = {}
        self._alignment_started_at: float | None = None
        self._last_alignment_duration_ms = 0
        self._barrier_blocked_inputs = 0
        self._source_pause_duration_ms = 0
        self._checkpoint_descriptor: TaskSnapshotDescriptor | None = None
        self._checkpoint_ready = asyncio.Event()
        self._last_closed_checkpoint_id = -1
        self._restored_checkpoint_pending: int | None = None
        self._source_idle = asyncio.Event()
        self._source_idle.set()
        if any(
            identity.downstream_task_id != task.task_id for identity in self._incoming_identities
        ):
            raise ValueError("TaskDeployment 包含目标不匹配的入通道")

    @property
    def task_id(self) -> str:
        """返回物理任务 ID。"""
        return self.deployment.task.task_id

    @property
    def state(self) -> TaskRuntimeState:
        """返回 Worker 本地状态。"""
        return self._state

    @property
    def snapshot(self) -> RuntimeSnapshot:
        """返回可安全序列化的当前状态快照。"""
        task = self.deployment.task
        return RuntimeSnapshot(
            task_id=task.task_id,
            job_id=task.job_id,
            operator_id=task.operator_id,
            subtask_index=task.subtask_index,
            state=self._state,
            error=self._error,
            input_channels=len(self._incoming_identities),
            closed_input_channels=len(self._closed_inputs),
            output_channels=len(self._all_output_channels),
            records_in=self._records_in,
            records_out=self._records_out,
            input_queue_depth=self._input_queue.qsize(),
            input_queue_capacity=self._input_queue.maxsize,
            output_queue_depth=sum(channel.queue_size for channel in self._all_output_channels),
            output_queue_capacity=sum(
                channel.queue_capacity for channel in self._all_output_channels
            ),
            max_output_queue_depth=max(
                (channel.max_queue_depth for channel in self._all_output_channels),
                default=0,
            ),
            batches_out=sum(channel.batches_sent for channel in self._all_output_channels),
            errors=self._failure_count,
            operator_metrics=self._operator_metrics(),
            attempt_id=task.attempt_id,
            coordinator_epoch=self.deployment.coordinator_epoch,
            restored_checkpoint_id=task.restored_checkpoint_id,
            barrier_alignment_duration_ms=self._last_alignment_duration_ms,
            barrier_blocked_inputs=self._barrier_blocked_inputs,
            source_pause_duration_ms=self._source_pause_duration_ms,
        )

    async def start(self) -> None:
        """注册入通道、打开算子、连接下游并启动后台执行循环。"""
        if self._state is not TaskRuntimeState.CREATED:
            raise RuntimeLifecycleError(f"任务只能启动一次, 当前状态 {self._state}")
        if not self.data_server.running:
            raise RuntimeLifecycleError("Worker 数据服务器尚未启动")
        self._state = TaskRuntimeState.STARTING
        try:
            if self._incoming_identities:
                await self.data_server.register(self._incoming_identities, self)
                self._registered = True
            if self.source is not None:
                await self.source.open()
            elif self.operator is not None:
                self.operator.open()
            await self._restore_checkpoint()
            self._routes, self._all_output_channels = await self._connect_outputs()
        except BaseException:
            await self._cleanup(graceful=False)
            self._state = TaskRuntimeState.FAILED
            raise
        self._state = TaskRuntimeState.RUNNING
        self._log(
            logging.INFO,
            "task_started",
            "物理任务已启动",
            input_channels=len(self._incoming_identities),
            output_channels=len(self._all_output_channels),
        )
        self._run_task = asyncio.create_task(
            self._run(),
            name=f"pystream-runtime-{self.task_id}",
        )

    async def stop(self) -> None:
        """幂等停止任务并清理连接、算子、UDF 命名空间和入通道注册。"""
        if self._state in {TaskRuntimeState.STOPPED, TaskRuntimeState.FAILED}:
            await self._cleanup(graceful=False)
            return
        if self._state is TaskRuntimeState.CREATED:
            self._state = TaskRuntimeState.STOPPED
            await self._cleanup(graceful=False)
            return
        self._stopping = True
        self._state = TaskRuntimeState.STOPPING
        run_task = self._run_task
        if run_task is not None and not run_task.done():
            run_task.cancel()
            with suppress(asyncio.CancelledError):
                await run_task
        await self._cleanup(graceful=True)
        self._state = TaskRuntimeState.STOPPED
        self._log(logging.INFO, "task_stopped", "物理任务已停止")

    async def wait(self) -> None:
        """等待后台循环完成；失败时抛出可诊断错误。"""
        if self._run_task is None:
            raise RuntimeLifecycleError("任务尚未启动")
        await self._run_task
        if self._state is TaskRuntimeState.FAILED:
            raise RuntimeTaskError(self._error or "任务失败")

    async def trigger_timers(self) -> None:
        """显式触发算子定时器，供运行循环和确定性集成测试复用。"""
        if self.operator is None:
            return
        outputs = self.operator.on_timer()
        if outputs:
            self._log(
                logging.INFO,
                "window_triggered",
                "处理时间窗口已触发",
                emitted_records=len(outputs),
                **self._operator_metrics(),
            )
        await self._emit_many(outputs)

    async def arm_checkpoint(self, checkpoint_id: int) -> None:
        """为一个严格递增的 Checkpoint 准备本地状态机。"""
        self._require_checkpoint_runtime()
        _require_checkpoint_id(checkpoint_id)
        async with self._checkpoint_lock:
            if self._state is not TaskRuntimeState.RUNNING:
                raise RuntimeLifecycleError(f"目标任务未运行: {self._state}")
            if checkpoint_id <= self._last_closed_checkpoint_id:
                raise RuntimeLifecycleError(
                    f"Checkpoint {checkpoint_id} 不大于已关闭编号 {self._last_closed_checkpoint_id}"
                )
            if self._active_checkpoint_id is not None:
                raise RuntimeLifecycleError(f"任务已有活动 Checkpoint {self._active_checkpoint_id}")
            self._active_checkpoint_id = checkpoint_id
            self._checkpoint_drains.clear()
            self._checkpoint_barriers.clear()
            self._release_barrier_gates()
            self._barrier_gates = (
                {identity: asyncio.Event() for identity in self._incoming_identities}
                if self._aligned_checkpoints
                else {}
            )
            self._alignment_started_at = None
            self._last_alignment_duration_ms = 0
            self._barrier_blocked_inputs = 0
            self._checkpoint_descriptor = None
            self._checkpoint_ready.clear()
            self._log(
                logging.INFO,
                "checkpoint_armed",
                "Task 已准备 Checkpoint",
                checkpoint_id=checkpoint_id,
            )

    async def trigger_checkpoint(self, checkpoint_id: int) -> TaskSnapshotDescriptor:
        """按交付保证注入 DRAIN 或短暂停顿的 aligned BARRIER。"""
        self._require_checkpoint_runtime()
        if self.source is None:
            raise RuntimeLifecycleError("只有 Source Task 可以触发 Checkpoint")
        async with self._checkpoint_lock:
            self._require_active_checkpoint(checkpoint_id)
            if self._checkpoint_descriptor is not None:
                return self._checkpoint_descriptor
            if self._aligned_checkpoints:
                return await self._trigger_aligned_source_checkpoint(checkpoint_id)
            await self.source.pause()
            await self._source_idle.wait()
            descriptor = await self._write_checkpoint_snapshot(checkpoint_id)
            await self._emit_control(_checkpoint_drain(self.task_id, checkpoint_id))
            self._checkpoint_descriptor = descriptor
            self._checkpoint_ready.set()
            self._log(
                logging.INFO,
                "checkpoint_task_snapshot",
                "Source Task 快照已写入并发送 DRAIN",
                checkpoint_id=checkpoint_id,
                snapshot_size=descriptor.size,
            )
            return descriptor

    async def _trigger_aligned_source_checkpoint(
        self,
        checkpoint_id: int,
    ) -> TaskSnapshotDescriptor:
        """冻结 Source 边界、排队 Barrier 后立即恢复持续消费。"""
        source = self.source
        if source is None:  # pragma: no cover - trigger_checkpoint 保证
            raise RuntimeLifecycleError("Source Task 缺少 Source")
        pause_started = self._monotonic_clock()
        frozen = False
        await source.pause()
        try:
            await self._source_idle.wait()
            snapshot = source.snapshot_checkpoint(checkpoint_id)
            frozen = True
            await self._emit_control(_checkpoint_barrier(self.task_id, checkpoint_id))
        except BaseException:
            if frozen:
                source.abort_checkpoint(checkpoint_id)
            raise
        finally:
            await source.resume()
            self._source_pause_duration_ms = max(
                0,
                int((self._monotonic_clock() - pause_started) * 1_000),
            )
        descriptor = await self._write_checkpoint_snapshot(
            checkpoint_id,
            snapshot=snapshot,
        )
        self._checkpoint_descriptor = descriptor
        self._checkpoint_ready.set()
        self._log(
            logging.INFO,
            "checkpoint_source_barrier",
            "Source 已冻结状态、发送 Barrier 并恢复消费",
            checkpoint_id=checkpoint_id,
            snapshot_size=descriptor.size,
            source_pause_duration_ms=self._source_pause_duration_ms,
        )
        return descriptor

    async def wait_checkpoint(self, checkpoint_id: int) -> TaskSnapshotDescriptor:
        """等待当前 Task 写完快照并成功转发 checkpoint control。"""
        self._require_checkpoint_runtime()
        self._require_active_checkpoint(checkpoint_id)
        await self._checkpoint_ready.wait()
        descriptor = self._checkpoint_descriptor
        if descriptor is None:  # pragma: no cover - Event 与 descriptor 同步设置
            raise RuntimeLifecycleError("Checkpoint ready 但缺少 Task snapshot descriptor")
        return descriptor

    async def complete_checkpoint(self, checkpoint_id: int) -> None:
        """完成已决定 checkpoint：先提交 Sink fragment，再提交 Source offset。"""
        self._require_checkpoint_runtime()
        async with self._checkpoint_lock:
            if (
                self._active_checkpoint_id is None
                and checkpoint_id == self._restored_checkpoint_pending
            ):
                if self._aligned_checkpoints and self.operator is not None:
                    commit_transaction = getattr(
                        self.operator,
                        "commit_transaction",
                        None,
                    )
                    if callable(commit_transaction) and getattr(
                        self.operator,
                        "transactional",
                        False,
                    ):
                        commit_transaction(checkpoint_id)
                if self.source is not None:
                    await self.source.commit_checkpoint(checkpoint_id)
                self._restored_checkpoint_pending = None
                self._last_closed_checkpoint_id = max(
                    self._last_closed_checkpoint_id,
                    checkpoint_id,
                )
                self._log(
                    logging.INFO,
                    "checkpoint_restored_finalized",
                    "Task 已完成恢复中的 DECIDED Checkpoint",
                    checkpoint_id=checkpoint_id,
                )
                return
            if (
                self._active_checkpoint_id is None
                and checkpoint_id == self._last_closed_checkpoint_id
            ):
                return
            self._require_active_checkpoint(checkpoint_id)
            if self._checkpoint_descriptor is None:
                raise RuntimeLifecycleError("Task snapshot 尚未完成")
            if self._aligned_checkpoints and self.operator is not None:
                commit_transaction = getattr(
                    self.operator,
                    "commit_transaction",
                    None,
                )
                if callable(commit_transaction) and getattr(
                    self.operator,
                    "transactional",
                    False,
                ):
                    commit_transaction(checkpoint_id)
            if self.source is not None:
                if self._aligned_checkpoints:
                    await self.source.commit_checkpoint(checkpoint_id)
                else:
                    await self.source.commit_checkpoint()
                    await self.source.resume()
            self._close_checkpoint(checkpoint_id)
            self._log(
                logging.INFO,
                "checkpoint_completed",
                "Task 已完成 Checkpoint",
                checkpoint_id=checkpoint_id,
            )

    async def abort_checkpoint(self, checkpoint_id: int) -> None:
        """中止活动 Checkpoint；Source 不提交 offset 并恢复消费。"""
        self._require_checkpoint_runtime()
        async with self._checkpoint_lock:
            if (
                self._active_checkpoint_id is None
                and checkpoint_id == self._last_closed_checkpoint_id
            ):
                return
            self._require_active_checkpoint(checkpoint_id)
            if self._aligned_checkpoints and self.operator is not None:
                abort_transaction = getattr(
                    self.operator,
                    "abort_transaction",
                    None,
                )
                if callable(abort_transaction) and getattr(
                    self.operator,
                    "transactional",
                    False,
                ):
                    abort_transaction(checkpoint_id)
            if self.source is not None:
                if self._aligned_checkpoints:
                    self.source.abort_checkpoint(checkpoint_id)
                await self.source.resume()
            self._close_checkpoint(checkpoint_id)
            self._log(
                logging.WARNING,
                "checkpoint_aborted",
                "Task 已中止 Checkpoint",
                checkpoint_id=checkpoint_id,
            )

    async def accept_records(
        self,
        identity: ChannelIdentity,
        records: tuple[RecordEnvelope, ...],
    ) -> None:
        """由 DataPlaneServer 按顺序投递一个输入批次。"""
        self._require_registered_identity(identity)
        if self._state is not TaskRuntimeState.RUNNING:
            raise RuntimeLifecycleError(f"目标任务未运行: {self._state}")
        for record in records:
            if record.message_type is not MessageType.DATA:
                raise RuntimeTaskError("DATA_BATCH 不能向 Runtime 投递控制消息")
            await self._input_queue.put(_InputRecord(identity, record))

    async def accept_control(
        self,
        identity: ChannelIdentity,
        record: RecordEnvelope,
    ) -> None:
        """由 DataPlaneServer 按通道顺序投递控制消息。"""
        self._require_registered_identity(identity)
        if self._state is not TaskRuntimeState.RUNNING:
            raise RuntimeLifecycleError(f"目标任务未运行: {self._state}")
        if record.message_type is MessageType.DATA:
            raise RuntimeTaskError("CONTROL 不能向 Runtime 投递 DATA")
        gate: asyncio.Event | None = None
        if record.message_type is MessageType.BARRIER:
            if not self._aligned_checkpoints:
                raise RuntimeTaskError("at_least_once Runtime 不接受 BARRIER")
            checkpoint_id = record.checkpoint_id
            if checkpoint_id is None:
                raise RuntimeTaskError("BARRIER 必须包含 checkpoint_id")
            self._require_active_checkpoint(checkpoint_id)
            gate = self._barrier_gates.get(identity)
            if gate is None:
                raise RuntimeTaskError(f"BARRIER 输入没有 gate: {identity.upstream_task_id}")
        await self._input_queue.put(_InputRecord(identity, record))
        if gate is not None:
            await gate.wait()

    async def input_closed(self, identity: ChannelIdentity) -> None:
        """把正常 EOS 放入同一有界合流队列。"""
        self._require_registered_identity(identity)
        if identity in self._closed_inputs:
            raise RuntimeConnectionError(f"入通道重复结束: {identity}")
        await self._input_queue.put(_InputEnded(identity))

    async def input_failed(self, identity: ChannelIdentity, error: BaseException) -> None:
        """异常输入连接使任务失败并向控制面传播。"""
        self._require_registered_identity(identity)
        await self._fail(RuntimeConnectionError(f"入通道 {identity} 失败: {error}"))

    async def _run(self) -> None:
        try:
            if self.source is not None:
                async for record in self.source.records():
                    self._source_idle.clear()
                    try:
                        if record.message_type is not MessageType.DATA:
                            if record.message_type is not MessageType.WATERMARK:
                                raise RuntimeTaskError(
                                    f"Source 不支持控制消息 {record.message_type.value}"
                                )
                            await self._emit_control(record)
                            continue
                        self._records_in += 1
                        await self._emit(record)
                        acknowledge = getattr(self.source, "acknowledge", None)
                        if callable(acknowledge):
                            acknowledge(record)
                        if not self._checkpoint_enabled:
                            await self.source.commit()
                    finally:
                        self._source_idle.set()
            else:
                await self._run_operator()
            await self._cleanup(graceful=True)
            if self._state is TaskRuntimeState.RUNNING:
                self._state = TaskRuntimeState.STOPPED
        except asyncio.CancelledError:
            if not self._stopping:
                await self._fail(RuntimeTaskError("任务执行协程被意外取消"))
            raise
        except BaseException as exc:
            await self._fail(exc)

    async def _run_operator(self) -> None:
        if not self._incoming_identities:
            raise RuntimeTaskError("非 Source 任务至少需要一个入通道")
        while len(self._closed_inputs) < len(self._incoming_identities):
            try:
                item = await asyncio.wait_for(
                    self._input_queue.get(),
                    timeout=self._timer_interval,
                )
            except TimeoutError:
                await self._refresh_idle_inputs()
                await self.trigger_timers()
                continue
            try:
                if isinstance(item, _InputEnded):
                    self._closed_inputs.add(item.identity)
                    continue
                self._mark_input_active(item.identity)
                record = item.record
                if record.message_type is not MessageType.DATA:
                    await self._handle_control(item.identity, record)
                    continue
                self._records_in += 1
                if self.operator is None:  # pragma: no cover - 构造器保证
                    raise RuntimeTaskError("普通任务缺少算子")
                metrics_before = self._operator_metrics()
                outputs = self.operator.process(record)
                await self._emit_many(outputs)
                metrics_after = self._operator_metrics()
                if metrics_after.get("late_records", 0) > metrics_before.get("late_records", 0):
                    self._log(
                        logging.WARNING,
                        "late_record_dropped",
                        "事件时间记录晚于当前 Watermark, 已丢弃",
                        record_id=record.record_id,
                        event_time=(
                            record.event_time.isoformat() if record.event_time is not None else None
                        ),
                    )
                if metrics_after.get("changelog_records", 0) > metrics_before.get(
                    "changelog_records", 0
                ):
                    self._log(
                        logging.INFO,
                        "changelog_emitted",
                        "Reduce 已产生 Changelog",
                        emitted_records=len(outputs),
                    )
                if metrics_after.get("retractions_applied", 0) > metrics_before.get(
                    "retractions_applied", 0
                ):
                    self._log(
                        logging.INFO,
                        "retract_applied",
                        "Reduce 已撤回旧贡献",
                        record_id=record.record_id,
                    )
                if metrics_after.get("retract_state_deletes", 0) > metrics_before.get(
                    "retract_state_deletes", 0
                ):
                    self._log(
                        logging.INFO,
                        "retract_state_deleted",
                        "Reduce 撤回后删除空状态",
                        record_id=record.record_id,
                    )
                await self.trigger_timers()
            finally:
                self._input_queue.task_done()
        await self.trigger_timers()

    async def _emit_many(self, records: list[RecordEnvelope]) -> None:
        for record in records:
            await self._emit(record)

    async def _emit(self, record: RecordEnvelope) -> None:
        if record.message_type is not MessageType.DATA:
            raise RuntimeTaskError("_emit 只能发送 DATA")
        for route in self._routes:
            await route.select(record).send(record)
            self._records_out += 1

    async def _emit_control(self, record: RecordEnvelope) -> None:
        """把控制消息广播到全部物理输出通道。"""
        if record.message_type is MessageType.DATA:
            raise RuntimeTaskError("_emit_control 不能发送 DATA")
        for channel in self._all_output_channels:
            await channel.send(record)

    async def _handle_control(
        self,
        identity: ChannelIdentity,
        record: RecordEnvelope,
    ) -> None:
        if record.message_type is MessageType.BARRIER:
            await self._handle_checkpoint_barrier(identity, record)
            return
        if record.message_type is MessageType.CHECKPOINT_DRAIN:
            if self._aligned_checkpoints:
                raise RuntimeTaskError("exactly_once Runtime 不接受 CHECKPOINT_DRAIN")
            await self._handle_checkpoint_drain(identity, record)
            return
        if record.message_type is not MessageType.WATERMARK:
            raise RuntimeTaskError(f"中级运行时尚未处理控制消息 {record.message_type.value}")
        if record.event_time is None:
            raise RuntimeTaskError("WATERMARK 必须包含 event_time")
        previous = self._input_watermarks[identity]
        if previous is not None and record.event_time < previous:
            raise RuntimeTaskError(
                f"入通道 Watermark 回退: previous={previous.isoformat()}, "
                f"actual={record.event_time.isoformat()}"
            )
        self._input_watermarks[identity] = record.event_time
        await self._advance_watermark()

    async def _handle_checkpoint_barrier(
        self,
        identity: ChannelIdentity,
        record: RecordEnvelope,
    ) -> None:
        checkpoint_id = record.checkpoint_id
        if checkpoint_id is None:
            raise RuntimeTaskError("BARRIER 必须包含 checkpoint_id")
        self._require_checkpoint_runtime()
        async with self._checkpoint_lock:
            if (
                self._active_checkpoint_id is None
                and checkpoint_id == self._last_closed_checkpoint_id
            ):
                self._log(
                    logging.INFO,
                    "checkpoint_barrier_released_after_close",
                    "Checkpoint 关闭后释放已入队 Barrier",
                    checkpoint_id=checkpoint_id,
                    upstream_task_id=identity.upstream_task_id,
                )
                return
            self._require_active_checkpoint(checkpoint_id)
            if identity in self._checkpoint_barriers:
                raise RuntimeTaskError(
                    f"Checkpoint {checkpoint_id} 收到重复 BARRIER: {identity.upstream_task_id}"
                )
            if self._alignment_started_at is None:
                self._alignment_started_at = self._monotonic_clock()
            self._checkpoint_barriers.add(identity)
            self._barrier_blocked_inputs = max(
                self._barrier_blocked_inputs,
                len(self._checkpoint_barriers),
            )
            if self._checkpoint_barriers != set(self._incoming_identities):
                return
            descriptor = await self._write_checkpoint_snapshot(checkpoint_id)
            await self._emit_control(_checkpoint_barrier(self.task_id, checkpoint_id))
            self._checkpoint_descriptor = descriptor
            self._checkpoint_ready.set()
            started_at = self._alignment_started_at
            if started_at is not None:
                self._last_alignment_duration_ms = max(
                    0,
                    int((self._monotonic_clock() - started_at) * 1_000),
                )
            self._release_barrier_gates()
            self._log(
                logging.INFO,
                "checkpoint_barrier_aligned",
                "Task 已收齐 Barrier、写入快照并转发",
                checkpoint_id=checkpoint_id,
                blocked_inputs=len(self._checkpoint_barriers),
                alignment_duration_ms=self._last_alignment_duration_ms,
                snapshot_size=descriptor.size,
            )

    async def _handle_checkpoint_drain(
        self,
        identity: ChannelIdentity,
        record: RecordEnvelope,
    ) -> None:
        checkpoint_id = record.checkpoint_id
        if checkpoint_id is None:
            raise RuntimeTaskError("CHECKPOINT_DRAIN 必须包含 checkpoint_id")
        self._require_checkpoint_runtime()
        async with self._checkpoint_lock:
            if checkpoint_id <= self._last_closed_checkpoint_id:
                self._log(
                    logging.WARNING,
                    "checkpoint_drain_ignored",
                    "忽略已关闭 Checkpoint 的延迟 DRAIN",
                    checkpoint_id=checkpoint_id,
                    upstream_task_id=identity.upstream_task_id,
                )
                return
            self._require_active_checkpoint(checkpoint_id)
            if identity in self._checkpoint_drains:
                raise RuntimeTaskError(
                    f"Checkpoint {checkpoint_id} 收到重复 DRAIN: {identity.upstream_task_id}"
                )
            self._checkpoint_drains.add(identity)
            if self._checkpoint_drains != set(self._incoming_identities):
                return
            descriptor = await self._write_checkpoint_snapshot(checkpoint_id)
            await self._emit_control(_checkpoint_drain(self.task_id, checkpoint_id))
            self._checkpoint_descriptor = descriptor
            self._checkpoint_ready.set()
            self._log(
                logging.INFO,
                "checkpoint_task_snapshot",
                "Task 已收齐 DRAIN 并写入快照",
                checkpoint_id=checkpoint_id,
                input_drains=len(self._checkpoint_drains),
                snapshot_size=descriptor.size,
            )

    async def _write_checkpoint_snapshot(
        self,
        checkpoint_id: int,
        *,
        snapshot: bytes | None = None,
    ) -> TaskSnapshotDescriptor:
        store = self._checkpoint_store
        if store is None:  # pragma: no cover - _require_checkpoint_runtime 保证
            raise RuntimeLifecycleError("Checkpoint Store 未配置")
        target = self.source if self.source is not None else self.operator
        if target is None:  # pragma: no cover - 构造器保证
            raise RuntimeLifecycleError("Task 缺少可快照的 Source 或 Operator")
        transactions: tuple[TransactionDescriptor, ...] = ()
        if self._aligned_checkpoints and self.operator is not None:
            pre_commit = getattr(self.operator, "pre_commit", None)
            if callable(pre_commit) and getattr(
                self.operator,
                "transactional",
                False,
            ):
                transaction = pre_commit(checkpoint_id)
                if not isinstance(transaction, TransactionDescriptor):
                    raise RuntimeTaskError("事务 Sink pre_commit 必须返回 TransactionDescriptor")
                transactions = (transaction,)
        try:
            resolved_snapshot = snapshot if snapshot is not None else target.snapshot_state()
            snapshot_text = resolved_snapshot.decode("utf-8")
        except (AttributeError, UnicodeDecodeError) as exc:
            raise RuntimeTaskError(f"Task 状态快照不是版本化 UTF-8 JSON: {exc}") from exc
        input_watermarks = [
            {
                "upstream_task_id": identity.upstream_task_id,
                "watermark": watermark.isoformat() if watermark is not None else None,
            }
            for identity, watermark in sorted(
                self._input_watermarks.items(),
                key=lambda item: item[0].upstream_task_id,
            )
        ]
        state = {
            "kind": "source" if self.source is not None else "operator",
            "snapshot": snapshot_text,
            "input_watermarks": input_watermarks,
            "last_output_watermark": (
                self._last_output_watermark.isoformat()
                if self._last_output_watermark is not None
                else None
            ),
        }
        task = self.deployment.task
        write_task = asyncio.create_task(
            asyncio.to_thread(
                store.write_task_snapshot,
                job_id=task.job_id,
                checkpoint_id=checkpoint_id,
                attempt_id=getattr(task, "attempt_id", 0),
                coordinator_epoch=self.deployment.coordinator_epoch,
                task_id=task.task_id,
                operator_id=task.operator_id,
                state=state,
                transactions=transactions,
            )
        )
        try:
            return await asyncio.shield(write_task)
        except asyncio.CancelledError:
            await write_task
            raise

    def _require_checkpoint_runtime(self) -> None:
        if not self._checkpoint_enabled:
            raise RuntimeLifecycleError("任务未启用 Checkpoint")
        if self._checkpoint_store is None:
            raise RuntimeLifecycleError("任务未配置 Checkpoint Store")

    def _require_active_checkpoint(self, checkpoint_id: int) -> None:
        _require_checkpoint_id(checkpoint_id)
        if self._active_checkpoint_id != checkpoint_id:
            raise RuntimeLifecycleError(
                f"Checkpoint {checkpoint_id} 未 arm, 当前活动编号为 {self._active_checkpoint_id}"
            )

    def _close_checkpoint(self, checkpoint_id: int) -> None:
        self._release_barrier_gates()
        self._last_closed_checkpoint_id = checkpoint_id
        self._active_checkpoint_id = None
        self._checkpoint_drains.clear()
        self._checkpoint_barriers.clear()
        self._barrier_gates.clear()
        self._alignment_started_at = None
        self._checkpoint_descriptor = None
        self._checkpoint_ready.clear()

    def _release_barrier_gates(self) -> None:
        """解除全部入通道 gate；重复调用保持幂等。"""
        for gate in self._barrier_gates.values():
            gate.set()

    def _mark_input_active(self, identity: ChannelIdentity) -> None:
        self._last_input_activity[identity] = self._monotonic_clock()
        if identity in self._idle_inputs:
            self._idle_inputs.remove(identity)
            self._log(
                logging.INFO,
                "input_active",
                "输入通道恢复活跃",
                upstream_task_id=identity.upstream_task_id,
            )

    async def _refresh_idle_inputs(self) -> None:
        timeout = self._watermark_idle_timeout
        if timeout is None:
            return
        now = self._monotonic_clock()
        changed = False
        for identity, last_activity in self._last_input_activity.items():
            if (
                identity not in self._closed_inputs
                and identity not in self._idle_inputs
                and now - last_activity >= timeout
            ):
                self._idle_inputs.add(identity)
                changed = True
                self._log(
                    logging.INFO,
                    "input_idle",
                    "输入通道超过空闲阈值",
                    upstream_task_id=identity.upstream_task_id,
                    idle_seconds=now - last_activity,
                )
        if changed:
            await self._advance_watermark()

    async def _advance_watermark(self) -> None:
        active = [
            identity
            for identity in self._incoming_identities
            if identity not in self._closed_inputs and identity not in self._idle_inputs
        ]
        if not active:
            return
        watermarks = [self._input_watermarks[identity] for identity in active]
        if any(watermark is None for watermark in watermarks):
            return
        candidate = min(watermark for watermark in watermarks if watermark is not None)
        if self._last_output_watermark is not None and candidate <= self._last_output_watermark:
            return
        self._last_output_watermark = candidate
        if self.operator is None:  # pragma: no cover - Source 不进入该路径
            raise RuntimeTaskError("普通任务缺少算子")
        outputs = self.operator.on_watermark(candidate)
        await self._emit_many(outputs)
        control = RecordEnvelope(
            record_id=f"watermark:{self.task_id}:{candidate.isoformat()}",
            payload={},
            processing_time=utc_now(),
            event_time=candidate,
            message_type=MessageType.WATERMARK,
            headers={"active_inputs": len(active)},
        )
        await self._emit_control(control)
        self._log(
            logging.INFO,
            "watermark_advanced",
            "任务 Watermark 已推进",
            watermark=candidate.isoformat(),
            active_inputs=len(active),
            emitted_records=len(outputs),
        )

    async def _restore_checkpoint(self) -> None:
        descriptors = self.deployment.restore_descriptors
        if not descriptors:
            return
        store = self._checkpoint_store
        if store is None:
            raise RuntimeLifecycleError("恢复 Task 必须配置 Checkpoint Store")
        task = self.deployment.task
        checkpoint_id = task.restored_checkpoint_id
        if checkpoint_id is None:
            raise RuntimeLifecycleError("恢复 descriptor 存在但 Task 缺少 restored_checkpoint_id")
        requires_finalization = False
        if self._aligned_checkpoints and store.has_decision(task.job_id, checkpoint_id):
            try:
                await asyncio.to_thread(
                    store.read_finalization,
                    task.job_id,
                    checkpoint_id,
                )
            except CheckpointError:
                requires_finalization = True
        states = await asyncio.gather(
            *(asyncio.to_thread(store.read_task_snapshot, item) for item in descriptors)
        )
        for descriptor in descriptors:
            if (
                descriptor.job_id != task.job_id
                or descriptor.operator_id != task.operator_id
                or descriptor.checkpoint_id != checkpoint_id
                or descriptor.attempt_id >= task.attempt_id
            ):
                raise RuntimeLifecycleError("恢复 descriptor 与当前 Task identity 不匹配")

        if self.source is not None:
            snapshot = _merge_kafka_source_snapshots(
                tuple(zip(descriptors, states, strict=True)),
                primary_task_id=task.task_id,
            )
            await self.source.restore_state(snapshot)
            if requires_finalization:
                prepare_restored = getattr(
                    self.source,
                    "prepare_restored_checkpoint",
                    None,
                )
                if not callable(prepare_restored):
                    raise RuntimeLifecycleError("Exactly-once Source 不支持恢复 DECIDED checkpoint")
                prepare_restored(checkpoint_id, snapshot)
        else:
            if len(descriptors) != 1 or descriptors[0].task_id != task.task_id:
                raise RuntimeLifecycleError("非 Source Task 必须只恢复自身 descriptor")
            state = _validate_runtime_restore_state(states[0], expected_kind="operator")
            snapshot = state["snapshot"]
            if not isinstance(snapshot, str):
                raise RuntimeLifecycleError("Task restore snapshot 必须是 UTF-8 JSON 字符串")
            if self.operator is None:  # pragma: no cover - 构造器保证
                raise RuntimeLifecycleError("恢复 Task 缺少 Operator")
            self.operator.restore_state(snapshot.encode("utf-8"))
            if requires_finalization and getattr(
                self.operator,
                "transactional",
                False,
            ):
                restore_transaction = getattr(
                    self.operator,
                    "restore_transaction",
                    None,
                )
                if not callable(restore_transaction):
                    raise RuntimeLifecycleError("Exactly-once Sink 不支持恢复 DECIDED transaction")
                for descriptor in descriptors:
                    for transaction in descriptor.transactions:
                        restore_transaction(transaction)
            self._restore_runtime_watermarks(state)

        if requires_finalization:
            self._restored_checkpoint_pending = checkpoint_id
        self._log(
            logging.INFO,
            "task_state_restored",
            "Task 已从完整 Checkpoint 恢复",
            checkpoint_id=checkpoint_id,
            source_descriptors=len(descriptors),
        )

    def _restore_runtime_watermarks(self, state: dict[str, object]) -> None:
        raw_inputs = state["input_watermarks"]
        if not isinstance(raw_inputs, list):
            raise RuntimeLifecycleError("Task restore input_watermarks 必须是 array")
        restored: dict[str, datetime | None] = {}
        for index, item in enumerate(raw_inputs):
            if not isinstance(item, dict) or set(item) != {
                "upstream_task_id",
                "watermark",
            }:
                raise RuntimeLifecycleError(f"Task restore input_watermarks[{index}] 字段错误")
            upstream_task_id = item["upstream_task_id"]
            if not isinstance(upstream_task_id, str) or not upstream_task_id:
                raise RuntimeLifecycleError(
                    f"Task restore input_watermarks[{index}] upstream_task_id 非法"
                )
            if upstream_task_id in restored:
                raise RuntimeLifecycleError("Task restore 包含重复 upstream_task_id")
            restored[upstream_task_id] = _parse_restore_time(
                item["watermark"],
                f"input_watermarks[{index}].watermark",
            )
        expected = {identity.upstream_task_id for identity in self._incoming_identities}
        if set(restored) != expected:
            raise RuntimeLifecycleError("Task restore input_watermarks 与物理输入集合不匹配")
        for identity in self._incoming_identities:
            self._input_watermarks[identity] = restored[identity.upstream_task_id]
        self._last_output_watermark = _parse_restore_time(
            state["last_output_watermark"],
            "last_output_watermark",
        )
        self._idle_inputs.clear()
        now = self._monotonic_clock()
        self._last_input_activity = {identity: now for identity in self._incoming_identities}

    async def _connect_outputs(
        self,
    ) -> tuple[tuple[_OutputRoute, ...], tuple[BoundedDataChannel, ...]]:
        grouped: dict[tuple[str, Partitioning], list[PhysicalChannel]] = defaultdict(list)
        for channel in self.deployment.outgoing_channels:
            if channel.source_task_id != self.task_id:
                raise ValueError("TaskDeployment 包含源任务不匹配的出通道")
            target_operator, _ = _task_coordinates(channel.target_task_id)
            grouped[(target_operator, channel.partitioning)].append(channel)

        routes: list[_OutputRoute] = []
        all_channels: list[BoundedDataChannel] = []
        try:
            for (_, partitioning), physical_channels in sorted(
                grouped.items(),
                key=lambda item: (item[0][0], item[0][1].value),
            ):
                opened: list[tuple[int, BoundedDataChannel]] = []
                for physical in sorted(
                    physical_channels,
                    key=lambda item: _task_coordinates(item.target_task_id)[1],
                ):
                    endpoint = physical.target_endpoint
                    if endpoint is None:
                        raise RuntimeConnectionError(f"通道 {physical.channel_id} 尚未绑定目标端点")
                    _, target_subtask = _task_coordinates(physical.target_task_id)
                    _, writer = await self._open_connection(endpoint.host, endpoint.port)
                    channel = BoundedDataChannel(
                        writer,
                        ChannelIdentity(
                            self.deployment.task.job_id,
                            self.task_id,
                            physical.target_task_id,
                            attempt_id=getattr(self.deployment.task, "attempt_id", 0),
                            coordinator_epoch=self.deployment.coordinator_epoch,
                        ),
                        queue_capacity=self._channel_queue_capacity,
                        batch_size=self._channel_batch_size,
                    )
                    await channel.start()
                    opened.append((target_subtask, channel))
                    all_channels.append(channel)
                router = None
                if partitioning is not Partitioning.FORWARD:
                    router = ShuffleRouter(
                        partitioning,
                        downstream_parallelism=len(opened),
                        upstream_subtask=self.deployment.task.subtask_index,
                    )
                routes.append(
                    _OutputRoute(
                        partitioning=partitioning,
                        channels=tuple(opened),
                        router=router,
                    )
                )
        except BaseException:
            for channel in reversed(all_channels):
                with suppress(Exception):
                    await channel.abort()
            raise
        return tuple(routes), tuple(all_channels)

    async def _fail(self, error: BaseException) -> None:
        if self._state is TaskRuntimeState.FAILED:
            return
        self._error = f"{type(error).__name__}: {error}"
        self._failure_count += 1
        self._state = TaskRuntimeState.FAILED
        self._log(
            logging.ERROR,
            "task_failed",
            "物理任务执行失败",
            error=self._error,
            exc_info=error,
        )
        await self._cleanup(graceful=False)
        if self._failure_callback is not None:
            with suppress(Exception):
                await self._failure_callback(
                    self.deployment.task.job_id,
                    self.task_id,
                    error,
                )
        run_task = self._run_task
        current = asyncio.current_task()
        if run_task is not None and run_task is not current and not run_task.done():
            run_task.cancel()

    async def _cleanup(self, *, graceful: bool) -> None:
        async with self._cleanup_lock:
            if self._cleaned:
                return
            self._cleaned = True
            self._release_barrier_gates()
            if self.source is not None:
                with suppress(Exception):
                    await self.source.close()
            if graceful:
                for channel in self._all_output_channels:
                    with suppress(Exception):
                        await channel.close()
            else:
                for channel in self._all_output_channels:
                    with suppress(Exception):
                        await channel.abort()
            if self.operator is not None:
                with suppress(Exception):
                    self.operator.close()
            if self._registered:
                await self.data_server.unregister(self._incoming_identities, self)
                self._registered = False
            if self.udf_loader is not None:
                self.udf_loader.close()

    def _require_registered_identity(self, identity: ChannelIdentity) -> None:
        if identity not in self._incoming_identities:
            raise RuntimeConnectionError(f"任务不接受入通道 {identity}")

    def _operator_metrics(self) -> dict[str, int]:
        target = self.source if self.source is not None else self.operator
        if target is None:
            return {}
        for attribute in ("metrics", "state_metrics"):
            value = getattr(target, attribute, None)
            if isinstance(value, dict):
                return {
                    str(key): metric
                    for key, metric in value.items()
                    if isinstance(metric, int) and not isinstance(metric, bool)
                }
        return {}

    def _log(
        self,
        level: int,
        event: str,
        message: str,
        *,
        exc_info: BaseException | bool | None = None,
        **fields,
    ) -> None:
        task = self.deployment.task
        log_event(
            logging.getLogger(__name__),
            level,
            event,
            message,
            component="task_runtime",
            job_id=task.job_id,
            operator_id=task.operator_id,
            subtask=task.subtask_index,
            worker_id=task.worker_id,
            exc_info=exc_info,
            task_id=task.task_id,
            **fields,
        )


def _validate_runtime_restore_state(
    document: object,
    *,
    expected_kind: str,
) -> dict[str, object]:
    if not isinstance(document, dict) or set(document) != {
        "kind",
        "snapshot",
        "input_watermarks",
        "last_output_watermark",
    }:
        raise RuntimeLifecycleError("Task restore state 字段集合不匹配")
    if document["kind"] != expected_kind:
        raise RuntimeLifecycleError(
            f"Task restore kind 不匹配: {document['kind']!r} != {expected_kind!r}"
        )
    return document


def _merge_kafka_source_snapshots(
    entries: tuple[
        tuple[TaskSnapshotDescriptor, dict[str, object]],
        ...,
    ],
    *,
    primary_task_id: str,
) -> bytes:
    topic: str | None = None
    partitions: dict[tuple[str, int], dict[str, object]] = {}
    primary_watermark: object = None
    primary_count = 0
    for descriptor, outer in entries:
        state = _validate_runtime_restore_state(outer, expected_kind="source")
        if state["input_watermarks"] != [] or state["last_output_watermark"] is not None:
            raise RuntimeLifecycleError("Source restore 包含非法 Runtime Watermark 状态")
        snapshot = state["snapshot"]
        if not isinstance(snapshot, str):
            raise RuntimeLifecycleError("Source restore snapshot 必须是 UTF-8 JSON 字符串")
        try:
            source = decode_state(snapshot.encode("utf-8"), "kafka-source")
        except CheckpointError as exc:
            raise RuntimeLifecycleError(f"Kafka Source restore snapshot 非法: {exc}") from exc
        if set(source) != {"topic", "partitions", "last_watermark"}:
            raise RuntimeLifecycleError("Kafka Source restore 字段集合不匹配")
        source_topic = source["topic"]
        if not isinstance(source_topic, str) or not source_topic:
            raise RuntimeLifecycleError("Kafka Source restore topic 非法")
        if topic is None:
            topic = source_topic
        elif topic != source_topic:
            raise RuntimeLifecycleError("Kafka Source restore topic 不一致")
        raw_partitions = source["partitions"]
        if not isinstance(raw_partitions, list):
            raise RuntimeLifecycleError("Kafka Source restore partitions 必须是 array")
        for index, item in enumerate(raw_partitions):
            if not isinstance(item, dict) or set(item) != {
                "topic",
                "partition",
                "next_offset",
                "max_event_time",
            }:
                raise RuntimeLifecycleError(f"Kafka Source restore partitions[{index}] 字段错误")
            partition_topic = item["topic"]
            partition = item["partition"]
            next_offset = item["next_offset"]
            if (
                partition_topic != source_topic
                or isinstance(partition, bool)
                or not isinstance(partition, int)
                or partition < 0
                or (
                    next_offset is not None
                    and (
                        isinstance(next_offset, bool)
                        or not isinstance(next_offset, int)
                        or next_offset < 0
                    )
                )
            ):
                raise RuntimeLifecycleError(
                    f"Kafka Source restore partitions[{index}] identity/offset 非法"
                )
            _parse_restore_time(
                item["max_event_time"],
                f"partitions[{index}].max_event_time",
            )
            key = (source_topic, partition)
            if key in partitions:
                raise RuntimeLifecycleError(
                    f"Kafka Source restore 包含重复 partition {source_topic}:{partition}"
                )
            partitions[key] = dict(item)
        _parse_restore_time(source["last_watermark"], "last_watermark")
        if descriptor.task_id == primary_task_id:
            primary_count += 1
            primary_watermark = source["last_watermark"]
    if topic is None or primary_count != 1:
        raise RuntimeLifecycleError("Source restore 缺少唯一当前 task descriptor")
    try:
        return encode_state(
            "kafka-source",
            {
                "topic": topic,
                "partitions": [partitions[key] for key in sorted(partitions)],
                "last_watermark": primary_watermark,
            },
        )
    except CheckpointError as exc:
        raise RuntimeLifecycleError(f"合并 Kafka Source restore 失败: {exc}") from exc


def _parse_restore_time(value: object, field: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeLifecycleError(f"Task restore {field} 必须是 ISO-8601 字符串或 null")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RuntimeLifecycleError(f"Task restore {field} 不是合法 ISO-8601 时间") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RuntimeLifecycleError(f"Task restore {field} 必须包含时区")
    return parsed.astimezone(UTC)


def _require_checkpoint_id(checkpoint_id: int) -> None:
    if isinstance(checkpoint_id, bool) or not isinstance(checkpoint_id, int) or checkpoint_id < 0:
        raise RuntimeLifecycleError("checkpoint_id 必须是非负整数")


def _checkpoint_drain(task_id: str, checkpoint_id: int) -> RecordEnvelope:
    return RecordEnvelope(
        record_id=f"checkpoint-drain:{task_id}:{checkpoint_id}",
        payload={},
        processing_time=utc_now(),
        message_type=MessageType.CHECKPOINT_DRAIN,
        checkpoint_id=checkpoint_id,
    )


def _checkpoint_barrier(task_id: str, checkpoint_id: int) -> RecordEnvelope:
    return RecordEnvelope(
        record_id=f"checkpoint-barrier:{task_id}:{checkpoint_id}",
        payload={},
        processing_time=utc_now(),
        message_type=MessageType.BARRIER,
        checkpoint_id=checkpoint_id,
    )


def _task_coordinates(task_id: str) -> tuple[str, int]:
    try:
        _, operator_id, raw_subtask = task_id.rsplit(":", 2)
        subtask = int(raw_subtask)
    except (ValueError, TypeError) as exc:
        raise RuntimeConnectionError(f"非法物理任务 ID: {task_id!r}") from exc
    if not operator_id or subtask < 0:
        raise RuntimeConnectionError(f"非法物理任务 ID: {task_id!r}")
    return operator_id, subtask


__all__ = [
    "AsyncRecordSource",
    "FailureCallback",
    "RuntimeSnapshot",
    "TaskRuntime",
    "TaskRuntimeState",
]
