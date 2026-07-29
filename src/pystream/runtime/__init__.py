"""任务运行时与跨节点数据通道。

该模块负责长度前缀 TCP 帧、Shuffle 路由、有界队列和
基础背压。第一阶段连接断开会向控制面报告任务失败。
"""

from pystream.runtime.channel import (
    BoundedDataChannel,
    ChannelClosedError,
    ChannelError,
    ChannelState,
)
from pystream.runtime.errors import (
    RuntimeConnectionError,
    RuntimeErrorBase,
    RuntimeLifecycleError,
    RuntimeTaskError,
)
from pystream.runtime.protocol import (
    ChannelIdentity,
    ConnectionClosedError,
    Frame,
    FrameTooLargeError,
    FrameType,
    HandshakeError,
    IncrementalFrameDecoder,
    MalformedFrameError,
    ProtocolError,
    VersionMismatchError,
    control_frame,
    data_batch_frame,
    encode_frame,
    end_of_stream_frame,
    error_frame,
    heartbeat_frame,
    hello_frame,
    read_frame,
    record_from_control,
    records_from_data_batch,
    validate_hello,
    write_frame,
)
from pystream.runtime.routing import (
    RoutingError,
    ShuffleRouter,
    canonical_json,
    stable_hash_partition,
)
from pystream.runtime.server import DataPlaneServer, InputConsumer
from pystream.runtime.task import (
    AsyncRecordSource,
    FailureCallback,
    RuntimeSnapshot,
    TaskRuntime,
    TaskRuntimeState,
)

__all__ = [
    "AsyncRecordSource",
    "BoundedDataChannel",
    "ChannelClosedError",
    "ChannelError",
    "ChannelIdentity",
    "ChannelState",
    "ConnectionClosedError",
    "DataPlaneServer",
    "FailureCallback",
    "Frame",
    "FrameTooLargeError",
    "FrameType",
    "HandshakeError",
    "IncrementalFrameDecoder",
    "InputConsumer",
    "MalformedFrameError",
    "ProtocolError",
    "RoutingError",
    "RuntimeConnectionError",
    "RuntimeErrorBase",
    "RuntimeLifecycleError",
    "RuntimeSnapshot",
    "RuntimeTaskError",
    "ShuffleRouter",
    "TaskRuntime",
    "TaskRuntimeState",
    "VersionMismatchError",
    "canonical_json",
    "control_frame",
    "data_batch_frame",
    "encode_frame",
    "end_of_stream_frame",
    "error_frame",
    "heartbeat_frame",
    "hello_frame",
    "read_frame",
    "record_from_control",
    "records_from_data_batch",
    "stable_hash_partition",
    "validate_hello",
    "write_frame",
]
