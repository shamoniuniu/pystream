"""PyStream v1 作业配置模型。

本模块只描述可公开提交的 YAML 契约，并执行单个字段或算子内部的约束。
跨算子关系、拓扑和分区属性由 :mod:`pystream.api.graph` 统一验证。
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

API_VERSION = "pystream/v1"
_DURATION_PATTERN = re.compile(r"^(?P<value>[1-9][0-9]*)(?P<unit>ms|s|m|h)$")
_UDF_REFERENCE_PATTERN = r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*$"


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


class JobMetadata(StrictModel):
    """作业的人类可读元数据。"""

    name: Annotated[str, Field(min_length=1, max_length=128)]


class TumblingWindowConfig(StrictModel):
    """处理时间滚动窗口配置。"""

    type: Literal["tumbling"] = "tumbling"
    time_characteristic: Literal["processing"] = "processing"
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
        factors = {"ms": 1, "s": 1_000, "m": 60_000, "h": 3_600_000}
        return int(match.group("value")) * factors[match.group("unit")]

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


class FileSinkConfig(StrictModel):
    """CSV 文件 Sink 的连接器配置。"""

    connector: Literal["file"]
    format: Literal["csv"] = "csv"
    output_path: Annotated[str, Field(min_length=1)] = "/data/output"


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
                raise ValueError("reduce 算子必须配置处理时间滚动窗口")
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
    operators: Annotated[list[OperatorSpec], Field(min_length=2)]
    edges: Annotated[list[EdgeSpec], Field(min_length=1)]


__all__ = [
    "API_VERSION",
    "ConnectorConfig",
    "EdgeSpec",
    "FileSinkConfig",
    "JobDefinition",
    "JobMetadata",
    "KafkaSourceConfig",
    "OperatorSpec",
    "OperatorType",
    "Partitioning",
    "TumblingWindowConfig",
]
