"""CLI 作业校验、打包和 mock 控制面的命令流程测试。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pystream import __version__
from pystream.artifact import sha256_file
from pystream.cli import (
    EXIT_ERROR,
    EXIT_JOB_FAILED,
    EXIT_OK,
    EXIT_TIMEOUT,
    build_parser,
    main,
)
from pystream.client import ClientError

JOB_YAML = """\
api_version: pystream/v1
job:
  name: wordcount
operators:
  - id: words
    type: source
    parallelism: 2
    config:
      connector: kafka
      topic: words
      value_format: json
  - id: normalize
    type: map
    parallelism: 2
    udf: wordcount_udfs:normalize
  - id: by_word
    type: key_by
    parallelism: 2
    udf: wordcount_udfs:word_key
  - id: totals
    type: reduce
    parallelism: 3
    udf: wordcount_udfs:add_counts
    window:
      type: tumbling
      time_characteristic: processing
      size: 5s
  - id: output
    type: sink
    parallelism: 1
    config:
      connector: file
      format: csv
edges:
  - from: words
    to: normalize
  - from: normalize
    to: by_word
  - from: by_word
    to: totals
  - from: totals
    to: output
"""


class FakeClient:
    """同时作为客户端工厂和命令调用记录器。"""

    def __init__(
        self,
        *,
        submit_response=None,
        status_responses=None,
        cancel_response=None,
        error: Exception | None = None,
    ) -> None:
        self.submit_response = submit_response or {"job_id": "job-1", "status": "SUBMITTED"}
        self.status_responses = list(
            status_responses or [{"job_id": "job-1", "name": "wordcount", "status": "RUNNING"}]
        )
        self.cancel_response = cancel_response or {
            "job_id": "job-1",
            "name": "wordcount",
            "status": "CANCELLED",
            "released_slots": 10,
            "tasks": [],
        }
        self.error = error
        self.factory_calls: list[tuple[str, float]] = []
        self.submit_calls: list[tuple[bytes, str, str]] = []
        self.status_calls: list[str] = []
        self.cancel_calls: list[str] = []

    def __call__(self, base_url: str, *, timeout: float):
        self.factory_calls.append((base_url, timeout))
        return self

    def submit(self, artifact: bytes, sha256: str, filename: str):
        if self.error:
            raise self.error
        self.submit_calls.append((artifact, sha256, filename))
        return self.submit_response

    def status(self, job_id: str):
        if self.error:
            raise self.error
        self.status_calls.append(job_id)
        if len(self.status_responses) > 1:
            return self.status_responses.pop(0)
        return self.status_responses[0]

    def cancel(self, job_id: str):
        if self.error:
            raise self.error
        self.cancel_calls.append(job_id)
        return self.cancel_response


@pytest.fixture
def job_dir(tmp_path: Path) -> Path:
    """创建可校验和打包的 WordCount 作业目录。"""
    root = tmp_path / "wordcount"
    root.mkdir()
    (root / "job.yaml").write_text(JOB_YAML, encoding="utf-8")
    (root / "wordcount_udfs.py").write_text(
        "def normalize(value):\n"
        "    return {'word': value['word'].lower(), 'count': value['count']}\n"
        "def word_key(value):\n"
        "    return value['word']\n"
        "def add_counts(left, right):\n"
        "    return {'word': left['word'], 'count': left['count'] + right['count']}\n",
        encoding="utf-8",
    )
    return root


def test_build_parser_包含程序信息和版本选项():
    parser = build_parser()

    assert parser.prog == "pystream"
    assert parser.description == "PyStream 简易分布式流计算系统"
    assert parser.parse_args([]) is not None
    assert any("--version" in action.option_strings for action in parser._actions)


def test_main_无参数时显示帮助并正常退出(capsys):
    exit_code = main([])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "usage: pystream" in captured.out
    assert "PyStream 简易分布式流计算系统" in captured.out


def test_main_版本参数显示当前版本(capsys):
    with pytest.raises(SystemExit) as exc_info:
        main(["--version"])

    captured = capsys.readouterr()
    assert exc_info.value.code == 0
    assert captured.out.strip() == f"pystream {__version__}"


def test_validate_展示_dag_tasks_和分区(job_dir, capsys):
    exit_code = main(["validate", str(job_dir / "job.yaml")])

    captured = capsys.readouterr()
    assert exit_code == EXIT_OK
    assert "job: wordcount" in captured.out
    assert "total_tasks: 10" in captured.out
    assert "words -> normalize: forward" in captured.out
    assert "by_word -> totals: hash" in captured.out


def test_validate_配置错误返回稳定退出码(tmp_path, capsys):
    source = tmp_path / "job.yaml"
    source.write_text("api_version: wrong\n", encoding="utf-8")

    exit_code = main(["validate", str(source)])

    captured = capsys.readouterr()
    assert exit_code == EXIT_ERROR
    assert "错误:" in captured.err
    assert "api_version" in captured.err


def test_package_输出制品清单与摘要(job_dir, tmp_path, capsys):
    output = tmp_path / "artifacts"

    exit_code = main(["package", str(job_dir), "--output-dir", str(output)])

    captured = capsys.readouterr()
    bundles = list(output.glob("artifact_*.zip"))
    assert exit_code == EXIT_OK
    assert len(bundles) == 1
    assert f"sha256: {sha256_file(bundles[0])}" in captured.out
    assert "job.yaml: size=" in captured.out
    assert "wordcount_udfs.py: size=" in captured.out


def test_submit_上传已验证_zip_并显示_job_id(job_dir, tmp_path, capsys):
    output = tmp_path / "artifacts"
    assert main(["package", str(job_dir), "--output-dir", str(output)]) == EXIT_OK
    bundle = next(output.glob("artifact_*.zip"))
    capsys.readouterr()
    client = FakeClient()

    exit_code = main(
        [
            "submit",
            str(bundle),
            "--jobmanager-url",
            "http://manager:9090",
            "--http-timeout",
            "2",
        ],
        client_factory=client,
    )

    captured = capsys.readouterr()
    assert exit_code == EXIT_OK
    assert "job_id: job-1" in captured.out
    assert "status: SUBMITTED" in captured.out
    assert client.factory_calls == [("http://manager:9090", 2.0)]
    artifact, digest, filename = client.submit_calls[0]
    assert artifact == bundle.read_bytes()
    assert digest == sha256_file(bundle)
    assert filename == bundle.name


def test_status_展示任务位置错误且失败作业返回非零(capsys):
    client = FakeClient(
        status_responses=[
            {
                "job_id": "job-1",
                "name": "wordcount",
                "status": "FAILED",
                "error": "worker lost",
                "tasks": [
                    {
                        "operator_id": "totals",
                        "subtask": 1,
                        "status": "FAILED",
                        "worker_id": "worker-2",
                        "slot": 0,
                        "error": "connection closed",
                    }
                ],
            }
        ]
    )

    exit_code = main(["status", "job-1"], client_factory=client)

    captured = capsys.readouterr()
    assert exit_code == EXIT_JOB_FAILED
    assert "status: FAILED" in captured.out
    assert "error: worker lost" in captured.out
    assert "totals[1] status=FAILED worker=worker-2 slot=0" in captured.out
    assert "error=connection closed" in captured.out


def test_status_json_输出稳定_json(capsys):
    response = {"status": "RUNNING", "job_id": "job-1", "tasks": []}
    client = FakeClient(status_responses=[response])

    exit_code = main(["status", "job-1", "--json"], client_factory=client)

    captured = capsys.readouterr()
    assert exit_code == EXIT_OK
    assert json.loads(captured.out) == response


def test_cancel_轮询到终态并展示释放资源(capsys):
    client = FakeClient(
        cancel_response={"job_id": "job-1", "status": "CANCELLING"},
        status_responses=[
            {"job_id": "job-1", "status": "RUNNING", "tasks": []},
            {
                "job_id": "job-1",
                "name": "wordcount",
                "status": "CANCELLED",
                "released_slots": 10,
                "tasks": [],
            },
        ],
    )

    exit_code = main(
        ["cancel", "job-1", "--poll-interval", "0.001"],
        client_factory=client,
    )

    captured = capsys.readouterr()
    assert exit_code == EXIT_OK
    assert client.cancel_calls == ["job-1"]
    assert client.status_calls == ["job-1", "job-1"]
    assert "status: CANCELLED" in captured.out
    assert "released_slots: 10" in captured.out


def test_cancel_等待超时返回专用退出码(monkeypatch, capsys):
    client = FakeClient(
        cancel_response={"job_id": "job-1", "status": "CANCELLING"},
        status_responses=[{"job_id": "job-1", "status": "RUNNING", "tasks": []}],
    )
    moments = iter([0.0, 2.0])
    monkeypatch.setattr("pystream.cli.time.monotonic", lambda: next(moments))
    monkeypatch.setattr("pystream.cli.time.sleep", lambda _: None)

    exit_code = main(
        ["cancel", "job-1", "--wait-timeout", "1"],
        client_factory=client,
    )

    captured = capsys.readouterr()
    assert exit_code == EXIT_TIMEOUT
    assert "进入终态超时" in captured.err


def test_http_错误转换为错误退出码(capsys):
    client = FakeClient(error=ClientError("JobManager HTTP 503: unavailable"))

    exit_code = main(["status", "job-1"], client_factory=client)

    captured = capsys.readouterr()
    assert exit_code == EXIT_ERROR
    assert "HTTP 503" in captured.err
