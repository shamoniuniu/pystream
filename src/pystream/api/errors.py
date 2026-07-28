"""作业配置错误及其稳定的人类可读表示。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def format_path(parts: tuple[Any, ...] | list[Any]) -> str:
    """把 Pydantic/YAML 路径转换为 ``operators[0].parallelism`` 形式。"""
    path = ""
    for part in parts:
        if isinstance(part, int):
            path += f"[{part}]"
        else:
            path += ("." if path else "") + str(part)
    return path or "$"


@dataclass(frozen=True, slots=True)
class ConfigIssue:
    """一条带配置路径的校验问题。"""

    path: str
    message: str

    def __str__(self) -> str:
        """返回适合 CLI 展示的一行错误。"""
        return f"{self.path}: {self.message}"


class JobConfigError(ValueError):
    """一个或多个作业配置问题的聚合异常。"""

    def __init__(self, issues: list[ConfigIssue] | tuple[ConfigIssue, ...]) -> None:
        if not issues:
            raise ValueError("JobConfigError 至少需要一条问题")
        self.issues = tuple(issues)
        super().__init__("\n".join(str(issue) for issue in self.issues))


__all__ = ["ConfigIssue", "JobConfigError", "format_path"]
