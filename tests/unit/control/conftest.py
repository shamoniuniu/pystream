"""控制面单元测试共享作业图。"""

from __future__ import annotations

import pytest
import yaml

from pystream.api import API_VERSION, StreamGraph, parse_stream_graph


def _operator(operator_id: str, operator_type: str, parallelism: int) -> dict:
    result: dict[str, object] = {
        "id": operator_id,
        "type": operator_type,
        "parallelism": parallelism,
    }
    if operator_type == "source":
        result["config"] = {"connector": "kafka", "topic": "words"}
    elif operator_type == "sink":
        result["config"] = {"connector": "file", "format": "csv"}
    elif operator_type in {"map", "key_by", "reduce"}:
        result["udf"] = f"wordcount_udfs:{operator_type}"
    if operator_type == "reduce":
        result["window"] = {"size": "300s"}
    return result


@pytest.fixture
def linear_graph() -> StreamGraph:
    """返回并发度为 2/2/2/3/1 的完整 WordCount 图。"""
    document = {
        "api_version": API_VERSION,
        "job": {"name": "wordcount"},
        "operators": [
            _operator("words", "source", 2),
            _operator("normalize", "map", 2),
            _operator("by_word", "key_by", 2),
            _operator("totals", "reduce", 3),
            _operator("output", "sink", 1),
        ],
        "edges": [
            {"from": "words", "to": "normalize"},
            {"from": "normalize", "to": "by_word"},
            {"from": "by_word", "to": "totals"},
            {"from": "totals", "to": "output"},
        ],
    }
    return parse_stream_graph(yaml.safe_dump(document, sort_keys=False))


@pytest.fixture
def two_task_graph() -> StreamGraph:
    """返回最小 Source -> Sink 图。"""
    document = {
        "api_version": API_VERSION,
        "job": {"name": "small"},
        "operators": [
            _operator("words", "source", 1),
            _operator("output", "sink", 1),
        ],
        "edges": [{"from": "words", "to": "output"}],
    }
    return parse_stream_graph(yaml.safe_dump(document, sort_keys=False))
