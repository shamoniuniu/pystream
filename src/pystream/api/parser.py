"""严格 YAML 解析和路径化配置错误转换。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError
from yaml.constructor import ConstructorError
from yaml.nodes import MappingNode

from pystream.api.errors import ConfigIssue, JobConfigError, format_path
from pystream.api.models import JobDefinition


class _UniqueKeyLoader(yaml.SafeLoader):
    """拒绝重复 mapping key 的 SafeLoader。"""


def _construct_unique_mapping(
    loader: _UniqueKeyLoader,
    node: MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as exc:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "mapping key 必须可哈希",
                key_node.start_mark,
            ) from exc
        if duplicate:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"发现重复字段 {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _clean_pydantic_message(message: str) -> str:
    prefix = "Value error, "
    return message.removeprefix(prefix)


def parse_job_yaml(content: str) -> JobDefinition:
    """把 YAML 文本解析为字段级合法的 :class:`JobDefinition`。"""
    try:
        document = yaml.load(content, Loader=_UniqueKeyLoader)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        location = f"(第 {mark.line + 1} 行, 第 {mark.column + 1} 列)" if mark else ""
        problem = getattr(exc, "problem", None) or str(exc)
        raise JobConfigError([ConfigIssue("$", f"YAML 语法错误{location}: {problem}")]) from exc

    if not isinstance(document, dict):
        raise JobConfigError([ConfigIssue("$", "作业 YAML 根节点必须是 mapping")])

    try:
        return JobDefinition.model_validate(document)
    except ValidationError as exc:
        issues = [
            ConfigIssue(
                format_path(error["loc"]),
                _clean_pydantic_message(error["msg"]),
            )
            for error in exc.errors(include_url=False)
        ]
        raise JobConfigError(issues) from exc


def load_job_yaml(path: str | Path) -> JobDefinition:
    """从 UTF-8 文件读取并解析作业定义。"""
    source = Path(path)
    try:
        content = source.read_text(encoding="utf-8")
    except OSError as exc:
        raise JobConfigError(
            [ConfigIssue("$", f"无法读取作业文件 {source}: {exc.strerror or exc}")]
        ) from exc
    return parse_job_yaml(content)


__all__ = ["load_job_yaml", "parse_job_yaml"]
