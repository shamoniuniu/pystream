"""PyStream 统一结构化日志。

服务进程输出一行一个 JSON object。所有记录都包含稳定的定位字段；某个维度
不适用时写入 ``null``，便于本地搜索和后续日志采集系统按同一 schema 解析。
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import UTC, datetime
from typing import Any, TextIO

STANDARD_FIELDS = (
    "component",
    "job_id",
    "operator_id",
    "subtask",
    "worker_id",
    "event",
)
_LOG_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__)


class JsonLogFormatter(logging.Formatter):
    """把标准 ``LogRecord`` 转为稳定 UTF-8 JSON。"""

    def format(self, record: logging.LogRecord) -> str:
        document: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "component": getattr(record, "component", record.name),
            "job_id": getattr(record, "job_id", None),
            "operator_id": getattr(record, "operator_id", None),
            "subtask": getattr(record, "subtask", None),
            "worker_id": getattr(record, "worker_id", None),
            "event": getattr(record, "event", "log"),
            "message": record.getMessage(),
        }
        if record.exc_info:
            document["exception"] = self.formatException(record.exc_info)
        for name, value in record.__dict__.items():
            if (
                name not in _LOG_RECORD_FIELDS
                and name not in document
                and name not in {"message", "asctime"}
            ):
                document[name] = value
        return json.dumps(document, ensure_ascii=False, default=str, separators=(",", ":"))


def configure_logging(
    level: str | int | None = None,
    *,
    stream: TextIO | None = None,
    force: bool = False,
) -> None:
    """配置根 logger；默认级别由 ``PYSTREAM_LOG_LEVEL`` 控制。"""
    resolved_level = level or os.getenv("PYSTREAM_LOG_LEVEL", "INFO")
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(JsonLogFormatter())
    logging.basicConfig(
        level=resolved_level,
        handlers=[handler],
        force=force,
    )


def log_event(
    logger: logging.Logger,
    level: int,
    event: str,
    message: str,
    *,
    component: str,
    job_id: str | None = None,
    operator_id: str | None = None,
    subtask: int | None = None,
    worker_id: str | None = None,
    exc_info: BaseException | bool | None = None,
    **fields: Any,
) -> None:
    """写入带标准定位字段的结构化事件。"""
    extra = {
        "component": component,
        "job_id": job_id,
        "operator_id": operator_id,
        "subtask": subtask,
        "worker_id": worker_id,
        "event": event,
        **fields,
    }
    logger.log(level, message, extra=extra, exc_info=exc_info)


__all__ = [
    "STANDARD_FIELDS",
    "JsonLogFormatter",
    "configure_logging",
    "log_event",
]
