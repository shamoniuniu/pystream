"""结构化日志 schema 和配置测试。"""

from __future__ import annotations

import io
import json
import logging
from unittest.mock import patch

from pystream.observability import JsonLogFormatter, configure_logging, log_event


def test_json_formatter_始终包含标准定位字段() -> None:
    record = logging.LogRecord(
        name="pystream.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="窗口已触发",
        args=(),
        exc_info=None,
    )
    record.component = "task_runtime"
    record.job_id = "job-1"
    record.operator_id = "totals"
    record.subtask = 2
    record.worker_id = "worker-3"
    record.event = "window_triggered"
    record.emitted_records = 4

    document = json.loads(JsonLogFormatter().format(record))

    assert document["timestamp"].endswith("+00:00")
    assert document["level"] == "INFO"
    assert document["component"] == "task_runtime"
    assert document["job_id"] == "job-1"
    assert document["operator_id"] == "totals"
    assert document["subtask"] == 2
    assert document["worker_id"] == "worker-3"
    assert document["event"] == "window_triggered"
    assert document["message"] == "窗口已触发"
    assert document["emitted_records"] == 4


def test_json_formatter_为不适用维度写null() -> None:
    record = logging.LogRecord(
        name="pystream.control",
        level=logging.WARNING,
        pathname=__file__,
        lineno=1,
        msg="控制面事件",
        args=(),
        exc_info=None,
    )

    document = json.loads(JsonLogFormatter().format(record))

    assert document["component"] == "pystream.control"
    assert document["event"] == "log"
    assert document["job_id"] is None
    assert document["operator_id"] is None
    assert document["subtask"] is None
    assert document["worker_id"] is None


def test_configure_logging_安装json_formatter() -> None:
    stream = io.StringIO()
    with patch("pystream.observability.logging.logging.basicConfig") as basic_config:
        configure_logging("INFO", stream=stream, force=True)

    arguments = basic_config.call_args.kwargs
    assert arguments["level"] == "INFO"
    assert arguments["force"] is True
    assert isinstance(arguments["handlers"][0].formatter, JsonLogFormatter)


def test_log_event_输出单行json() -> None:
    stream = io.StringIO()
    logger = logging.Logger("pystream.test", level=logging.INFO)
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonLogFormatter())
    logger.addHandler(handler)

    log_event(
        logger,
        logging.INFO,
        "worker_registered",
        "Worker 已注册",
        component="jobmanager",
        worker_id="worker-1",
        total_slots=4,
    )

    lines = stream.getvalue().splitlines()
    assert len(lines) == 1
    document = json.loads(lines[0])
    assert document["component"] == "jobmanager"
    assert document["worker_id"] == "worker-1"
    assert document["event"] == "worker_registered"
    assert document["total_slots"] == 4
