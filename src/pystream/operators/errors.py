"""算子层错误类型。

本模块将生命周期错误、输入记录错误和第一阶段尚未支持的状态操作区分开，
便于 TaskRuntime 将失败原因稳定地上报给控制面。
"""


class OperatorError(RuntimeError):
    """所有算子执行错误的基类。"""


class OperatorLifecycleError(OperatorError):
    """算子被以非法生命周期顺序调用。"""


class RecordValidationError(OperatorError, ValueError):
    """记录或用户 UDF 返回值不满足算子契约。"""


class UnsupportedStateOperation(OperatorError, NotImplementedError):
    """第一阶段不支持状态快照或恢复。"""


class ConnectorError(OperatorError):
    """外部 Source 或 Sink 连接器执行失败。"""


class KafkaSourceError(ConnectorError):
    """Kafka Source 启停、消费或提交 offset 失败。"""


class BadRecordError(KafkaSourceError, ValueError):
    """Kafka 消息不能按 Source 数据契约转换为记录信封。"""


class FileSinkError(ConnectorError):
    """文件 Sink 创建、写入、刷新或关闭失败。"""


__all__ = [
    "BadRecordError",
    "ConnectorError",
    "FileSinkError",
    "KafkaSourceError",
    "OperatorError",
    "OperatorLifecycleError",
    "RecordValidationError",
    "UnsupportedStateOperation",
]
