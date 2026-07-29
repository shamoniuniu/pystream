"""作业定义与 DataStream API。

该模块把严格 YAML 配置转换为经过校验的逻辑 DAG，并维护数据流的 key 与分区属性。
所有配置错误都在进入调度器之前失败，不依赖控制面或运行时实现。
"""

from __future__ import annotations

from pathlib import Path

from pystream.api.errors import ConfigIssue, JobConfigError
from pystream.api.graph import DataStream, StreamEdge, StreamGraph, build_stream_graph
from pystream.api.models import (
    API_VERSION,
    CheckpointConfig,
    EdgeSpec,
    EventTimeExecutionConfig,
    EventTimeExtractorConfig,
    ExecutionConfig,
    FileSinkConfig,
    JobDefinition,
    JobMetadata,
    KafkaSourceConfig,
    OperatorSpec,
    OperatorType,
    Partitioning,
    RestartConfig,
    TumblingWindowConfig,
)
from pystream.api.parser import load_job_yaml, parse_job_yaml


def parse_stream_graph(content: str) -> StreamGraph:
    """从 YAML 文本完成字段校验和逻辑图构建。"""
    return build_stream_graph(parse_job_yaml(content))


def load_stream_graph(path: str | Path) -> StreamGraph:
    """从 UTF-8 YAML 文件完成字段校验和逻辑图构建。"""
    return build_stream_graph(load_job_yaml(path))


__all__ = [
    "API_VERSION",
    "CheckpointConfig",
    "ConfigIssue",
    "DataStream",
    "EdgeSpec",
    "EventTimeExecutionConfig",
    "EventTimeExtractorConfig",
    "ExecutionConfig",
    "FileSinkConfig",
    "JobConfigError",
    "JobDefinition",
    "JobMetadata",
    "KafkaSourceConfig",
    "OperatorSpec",
    "OperatorType",
    "Partitioning",
    "RestartConfig",
    "StreamEdge",
    "StreamGraph",
    "TumblingWindowConfig",
    "build_stream_graph",
    "load_job_yaml",
    "load_stream_graph",
    "parse_job_yaml",
    "parse_stream_graph",
]
