"""流算子与处理时间窗口。

本包提供统一生命周期、可注入 Clock、Map、KeyBy 和 keyed Reduce 滚动窗口。
Kafka Source 与文件 Sink 由后续连接器任务实现，并复用这里的算子接口。
"""

from pystream.operators.base import (
    BaseOperator,
    JsonValue,
    OperatorContext,
    OperatorState,
    OperatorTask,
    RecordLike,
    canonical_json,
    clone_record,
    validate_json_value,
)
from pystream.operators.clock import Clock, ManualClock, SystemClock, require_utc
from pystream.operators.connectors import (
    AsyncKafkaConsumer,
    FileSinkOperator,
    KafkaConsumerFactory,
    KafkaJsonSource,
    KafkaMessage,
    PayloadValidator,
)
from pystream.operators.core import KeyByOperator, MapOperator
from pystream.operators.errors import (
    BadRecordError,
    ConnectorError,
    FileSinkError,
    KafkaSourceError,
    OperatorError,
    OperatorLifecycleError,
    RecordValidationError,
    UnsupportedStateOperation,
)
from pystream.operators.window import (
    ReduceWindowOperator,
    TimeWindow,
    TumblingProcessingTimeWindowAssigner,
)

__all__ = [
    "AsyncKafkaConsumer",
    "BadRecordError",
    "BaseOperator",
    "Clock",
    "ConnectorError",
    "FileSinkError",
    "FileSinkOperator",
    "JsonValue",
    "KafkaConsumerFactory",
    "KafkaJsonSource",
    "KafkaMessage",
    "KafkaSourceError",
    "KeyByOperator",
    "ManualClock",
    "MapOperator",
    "OperatorContext",
    "OperatorError",
    "OperatorLifecycleError",
    "OperatorState",
    "OperatorTask",
    "PayloadValidator",
    "RecordLike",
    "RecordValidationError",
    "ReduceWindowOperator",
    "SystemClock",
    "TimeWindow",
    "TumblingProcessingTimeWindowAssigner",
    "UnsupportedStateOperation",
    "canonical_json",
    "clone_record",
    "require_utc",
    "validate_json_value",
]
