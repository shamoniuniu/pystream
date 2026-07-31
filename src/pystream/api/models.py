"""PyStream v1 作业配置模型。

本模块只描述可公开提交的 YAML 契约，并执行单个字段或算子内部的约束。
跨算子关系、拓扑和分区属性由 :mod:`pystream.api.graph` 统一验证。
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from pystream.common import JsonPointerError, validate_json_pointer

API_VERSION = "pystream/v1"
_DURATION_PATTERN = re.compile(r"^(?P<value>[1-9][0-9]*)(?P<unit>ms|s|m|h)$")
_NON_NEGATIVE_DURATION_PATTERN = re.compile(r"^(?P<value>0|[1-9][0-9]*)(?P<unit>ms|s|m|h)$")
_UDF_REFERENCE_PATTERN = r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*$"
_DURATION_FACTORS = {"ms": 1, "s": 1_000, "m": 60_000, "h": 3_600_000}


def _duration_milliseconds(value: str, *, allow_zero: bool = False) -> int:
    pattern = _NON_NEGATIVE_DURATION_PATTERN if allow_zero else _DURATION_PATTERN
    match = pattern.fullmatch(value)
    if match is None:
        qualifier = "非负" if allow_zero else "正"
        raise ValueError(f"持续时间必须是{qualifier}整数加 ms/s/m/h 单位")
    return int(match.group("value")) * _DURATION_FACTORS[match.group("unit")]


class StrictModel(BaseModel):
    """拒绝未知字段并冻结配置对象的公共模型基类。"""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        loc_by_alias=True,
    )


class OperatorType(StrEnum):
    """第一阶段支持的逻辑算子类型。"""

    SOURCE = "source"
    MAP = "map"
    KEY_BY = "key_by"
    REDUCE = "reduce"
    SINK = "sink"


class Partitioning(StrEnum):
    """逻辑边可选择的数据分区策略。"""

    FORWARD = "forward"
    REBALANCE = "rebalance"
    HASH = "hash"


class DeliveryGuarantee(StrEnum):
    """作业对状态、输入位置和 Sink 可见副作用的交付保证。"""

    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


class JobMetadata(StrictModel):
    """作业的人类可读元数据。"""

    name: Annotated[str, Field(min_length=1, max_length=128)]


class EventTimeExecutionConfig(StrictModel):
    """作业级有限乱序与空闲输入策略。"""

    max_out_of_orderness: str
    idle_timeout: str = "30s"

    @field_validator("max_out_of_orderness")
    @classmethod
    def validate_out_of_orderness(cls, value: str) -> str:
        """校验 Watermark 最大乱序时间非负。"""
        _duration_milliseconds(value, allow_zero=True)
        return value

    @field_validator("idle_timeout")
    @classmethod
    def validate_idle_timeout(cls, value: str) -> str:
        """校验空闲输入超时为正。"""
        _duration_milliseconds(value)
        return value

    @property
    def max_out_of_orderness_milliseconds(self) -> int:
        """返回最大乱序毫秒数。"""
        return _duration_milliseconds(self.max_out_of_orderness, allow_zero=True)

    @property
    def idle_timeout_seconds(self) -> float:
        """返回空闲输入超时秒数。"""
        return _duration_milliseconds(self.idle_timeout) / 1_000


class CheckpointConfig(StrictModel):
    """停流协调 Checkpoint 的周期和失败边界。"""

    interval: str = "10s"
    timeout: str = "30s"
    max_consecutive_failures: Annotated[int, Field(strict=True, ge=1, le=100)] = 3

    @field_validator("interval", "timeout")
    @classmethod
    def validate_duration(cls, value: str) -> str:
        """校验周期和超时均为正持续时间。"""
        _duration_milliseconds(value)
        return value

    @property
    def interval_seconds(self) -> float:
        """返回 Checkpoint 周期秒数。"""
        return _duration_milliseconds(self.interval) / 1_000

    @property
    def timeout_seconds(self) -> float:
        """返回 Checkpoint 超时秒数。"""
        return _duration_milliseconds(self.timeout) / 1_000


class RestartConfig(StrictModel):
    """整作业恢复重试策略。"""

    max_attempts: Annotated[int, Field(strict=True, ge=0, le=100)] = 3
    delay: str = "2s"

    @field_validator("delay")
    @classmethod
    def validate_delay(cls, value: str) -> str:
        """允许测试或用户显式使用 0ms 重试延迟。"""
        _duration_milliseconds(value, allow_zero=True)
        return value

    @property
    def delay_seconds(self) -> float:
        """返回重试延迟秒数。"""
        return _duration_milliseconds(self.delay, allow_zero=True) / 1_000


class ExecutionConfig(StrictModel):
    """可选高级运行语义；缺失时保持第一阶段行为。"""

    delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.EXACTLY_ONCE
    event_time: EventTimeExecutionConfig | None = None
    checkpoint: CheckpointConfig = CheckpointConfig()
    restart: RestartConfig = RestartConfig()


class TumblingWindowConfig(StrictModel):
    """处理时间或事件时间滚动窗口配置。"""

    type: Literal["tumbling"] = "tumbling"
    time_characteristic: Literal["processing", "event"] = "processing"
    size: str = "300s"

    @model_validator(mode="after")
    def validate_size(self) -> TumblingWindowConfig:
        """确保持续时间语法合法且至少为一毫秒。"""
        if _DURATION_PATTERN.fullmatch(self.size) is None:
            raise ValueError("size 必须是正整数加 ms/s/m/h 单位, 例如 300s")
        return self

    @property
    def size_milliseconds(self) -> int:
        """返回窗口大小的毫秒值。"""
        match = _DURATION_PATTERN.fullmatch(self.size)
        if match is None:  # pragma: no cover - 模型构造时已经验证
            raise ValueError(f"非法窗口大小: {self.size}")
        return int(match.group("value")) * _DURATION_FACTORS[match.group("unit")]

    @property
    def size_seconds(self) -> float:
        """返回窗口大小的秒值，供定时器和测试使用。"""
        return self.size_milliseconds / 1_000


class KafkaSourceConfig(StrictModel):
    """Kafka JSON Source 的连接器配置。"""

    connector: Literal["kafka"]
    topic: Annotated[str, Field(min_length=1, max_length=249)]
    value_format: Literal["json"] = "json"
    bootstrap_servers: Annotated[str, Field(min_length=1)] = "kafka:9092"
    group_id: Annotated[str | None, Field(min_length=1, max_length=255)] = None
    bad_record_policy: Literal["fail", "skip"] = "fail"
    validator: Annotated[str | None, Field(pattern=_UDF_REFERENCE_PATTERN)] = None
    event_time: EventTimeExtractorConfig | None = None


class EventTimeExtractorConfig(StrictModel):
    """Source payload 的 RFC3339 事件时间提取规则。"""

    pointer: Annotated[str, Field(max_length=1_024)]
    format: Literal["rfc3339"] = "rfc3339"

    @field_validator("pointer")
    @classmethod
    def validate_pointer(cls, value: str) -> str:
        """在作业解析阶段拒绝非法 RFC 6901 路径。"""
        try:
            return validate_json_pointer(value)
        except JsonPointerError as exc:
            raise ValueError(str(exc)) from exc


class FileSinkConfig(StrictModel):
    """CSV 文件 Sink 的连接器配置。"""

    supports_exactly_once: ClassVar[bool] = True

    connector: Literal["file"]
    format: Literal["csv"] = "csv"
    output_path: Annotated[str, Field(min_length=1)] = "/data/output"
    columns: Annotated[list[str], Field(min_length=1, max_length=64)] | None = None

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, value: list[str] | None) -> list[str] | None:
        """校验所有输出列均为唯一合法 JSON Pointer。"""
        if value is None:
            return None
        if len(set(value)) != len(value):
            raise ValueError("columns 不能包含重复 JSON Pointer")
        try:
            return [validate_json_pointer(pointer) for pointer in value]
        except JsonPointerError as exc:
            raise ValueError(str(exc)) from exc


ConnectorConfig = Annotated[
    KafkaSourceConfig | FileSinkConfig,
    Field(discriminator="connector"),
]


class OperatorSpec(StrictModel):
    """单个逻辑算子的公开配置。"""

    id: Annotated[
        str,
        Field(
            min_length=1,
            max_length=128,
            pattern=r"^[A-Za-z][A-Za-z0-9_-]*$",
        ),
    ]
    type: OperatorType
    parallelism: Annotated[int, Field(strict=True, ge=1, le=1024)] = 1
    udf: Annotated[
        str | None,
        Field(pattern=_UDF_REFERENCE_PATTERN),
    ] = None
    retract_udf: Annotated[
        str | None,
        Field(pattern=_UDF_REFERENCE_PATTERN),
    ] = None
    emit_mode: Literal["final", "changelog"] = "final"
    config: ConnectorConfig | None = None
    window: TumblingWindowConfig | None = None

    @model_validator(mode="after")
    def validate_operator_contract(self) -> OperatorSpec:
        """校验算子类型与 UDF、连接器、窗口之间的局部关系。"""
        udf_types = {OperatorType.MAP, OperatorType.KEY_BY, OperatorType.REDUCE}
        if self.type in udf_types and self.udf is None:
            raise ValueError(f"{self.type.value} 算子必须配置 udf")
        if self.type not in udf_types and self.udf is not None:
            raise ValueError(f"{self.type.value} 算子不能配置 udf")
        if self.type is not OperatorType.REDUCE:
            if self.retract_udf is not None:
                raise ValueError(f"{self.type.value} 算子不能配置 retract_udf")
            if "emit_mode" in self.model_fields_set:
                raise ValueError(f"{self.type.value} 算子不能配置 emit_mode")

        if self.type is OperatorType.SOURCE:
            if not isinstance(self.config, KafkaSourceConfig):
                raise ValueError("source 算子必须配置 Kafka connector")
        elif self.type is OperatorType.SINK:
            if not isinstance(self.config, FileSinkConfig):
                raise ValueError("sink 算子必须配置 File connector")
        elif self.config is not None:
            raise ValueError(f"{self.type.value} 算子不能配置 connector")

        if self.type is OperatorType.REDUCE:
            if self.window is None:
                raise ValueError("reduce 算子必须配置处理时间滚动窗口或事件时间滚动窗口")
        elif self.window is not None:
            raise ValueError(f"{self.type.value} 算子不能配置 window")
        return self


class EdgeSpec(StrictModel):
    """两个逻辑算子之间的有向边。"""

    from_: Annotated[
        str,
        Field(alias="from", min_length=1, max_length=128),
    ]
    to: Annotated[str, Field(min_length=1, max_length=128)]


class JobDefinition(StrictModel):
    """完整且经过字段级校验的 v1 作业定义。"""

    api_version: Literal[API_VERSION]
    job: JobMetadata
    execution: ExecutionConfig | None = None
    operators: Annotated[list[OperatorSpec], Field(min_length=2)]
    edges: Annotated[list[EdgeSpec], Field(min_length=1)]


__all__ = [
    "API_VERSION",
    "CheckpointConfig",
    "ConnectorConfig",
    "DeliveryGuarantee",
    "EdgeSpec",
    "EventTimeExecutionConfig",
    "EventTimeExtractorConfig",
    "ExecutionConfig",
    "FileSinkConfig",
    "JobDefinition",
    "JobMetadata",
    "KafkaSourceConfig",
    "OperatorSpec",
    "OperatorType",
    "Partitioning",
    "RestartConfig",
    "TumblingWindowConfig",
]
