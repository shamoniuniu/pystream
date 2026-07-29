"""跨模块共享的稳定基础类型。

该模块承载记录信封、错误类型和少量无业务依赖的工具。它不得反向依赖控制面、
运行时、算子或命令行模块。
"""

from pystream.common.json_pointer import (
    JsonPointerError,
    resolve_json_pointer,
    validate_json_pointer,
)
from pystream.common.records import (
    ChangeKind,
    JsonScalar,
    JsonValue,
    MessageType,
    RecordEnvelope,
    RecordValidationError,
    utc_now,
)

__all__ = [
    "ChangeKind",
    "JsonPointerError",
    "JsonScalar",
    "JsonValue",
    "MessageType",
    "RecordEnvelope",
    "RecordValidationError",
    "resolve_json_pointer",
    "utc_now",
    "validate_json_pointer",
]
