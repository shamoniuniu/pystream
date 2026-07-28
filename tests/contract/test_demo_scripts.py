"""WordCount 等待和验证脚本的离线契约测试。"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"


def _load_script(name: str) -> ModuleType:
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载脚本 {name}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_load_script("_demo")
wait_for_window = _load_script("wait_for_window")
verify_wordcount = _load_script("verify_wordcount")


class RunningClient:
    """始终返回 RUNNING 的最小作业客户端。"""

    def status(self, job_id: str) -> dict[str, object]:
        assert job_id == "job-1"
        return {"job_id": job_id, "status": "RUNNING"}


def _output_file(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "job-1" / "output" / "part-00000.csv"
    path.parent.mkdir(parents=True)
    path.write_text(content, encoding="utf-8")
    return path


def test_wait_for_window_部分结果不会提前成功(tmp_path: Path, monkeypatch) -> None:
    path = _output_file(tmp_path, "2026/07/26T12:00:10,apple,1\n")
    clock = {"now": 0.0, "sleeps": 0}

    monkeypatch.setattr(wait_for_window.time, "monotonic", lambda: clock["now"])

    def finish_output(seconds: float) -> None:
        clock["sleeps"] += 1
        clock["now"] += seconds
        path.write_text(
            "2026/07/26T12:00:10,apple,2\n2026/07/26T12:00:10,pie,1\n",
            encoding="utf-8",
        )

    monkeypatch.setattr(wait_for_window.time, "sleep", finish_output)

    files = wait_for_window.wait_for_complete_output(
        RunningClient(),
        job_id="job-1",
        output_root=tmp_path,
        timeout=2,
        poll_interval=0.1,
    )

    assert files == (path,)
    assert clock["sleeps"] == 1


def test_wait_for_window_部分结果持续存在时超时(tmp_path: Path, monkeypatch) -> None:
    _output_file(tmp_path, "2026/07/26T12:00:10,apple,1\n")
    clock = {"now": 0.0}

    monkeypatch.setattr(wait_for_window.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(
        wait_for_window.time,
        "sleep",
        lambda seconds: clock.__setitem__("now", clock["now"] + seconds),
    )

    with pytest.raises(TimeoutError, match=r"当前计数.*apple.*1"):
        wait_for_window.wait_for_complete_output(
            RunningClient(),
            job_id="job-1",
            output_root=tmp_path,
            timeout=0.2,
            poll_interval=0.1,
        )


def test_verify_wordcount_错误计数失败(tmp_path: Path, monkeypatch) -> None:
    _output_file(
        tmp_path,
        "2026/07/26T12:00:10,apple,1\n2026/07/26T12:00:10,pie,1\n",
    )
    status = {
        "job_id": "job-1",
        "status": "RUNNING",
        "tasks": [
            {
                "task_id": "job-1:by_word:0",
                "operator_id": "by_word",
                "worker_id": "worker-1",
            },
            {
                "task_id": "job-1:totals:0",
                "operator_id": "totals",
                "worker_id": "worker-2",
            },
        ],
    }

    class Client:
        def __init__(self, _: str) -> None:
            pass

        def status(self, _: str) -> dict[str, object]:
            return status

    monkeypatch.setattr(verify_wordcount, "JobManagerClient", Client)

    with pytest.raises(RuntimeError, match="WordCount 结果不匹配"):
        verify_wordcount.main(["--job-id", "job-1", "--output-root", str(tmp_path)])


def test_verify_wordcount_缺少跨worker_hash证据失败() -> None:
    tasks = [
        {
            "task_id": "job-1:by_word:0",
            "operator_id": "by_word",
            "worker_id": "worker-1",
        },
        {
            "task_id": "job-1:totals:0",
            "operator_id": "totals",
            "worker_id": "worker-1",
        },
    ]

    with pytest.raises(RuntimeError, match="跨 Worker HASH Shuffle"):
        verify_wordcount.cross_worker_hash_channels(tasks)


def test_verify_wordcount_生成跨worker_hash通道证据() -> None:
    tasks = [
        {
            "task_id": "job-1:by_word:0",
            "operator_id": "by_word",
            "worker_id": "worker-1",
        },
        {
            "task_id": "job-1:by_word:1",
            "operator_id": "by_word",
            "worker_id": "worker-2",
        },
        {
            "task_id": "job-1:totals:0",
            "operator_id": "totals",
            "worker_id": "worker-2",
        },
    ]

    channels = verify_wordcount.cross_worker_hash_channels(tasks)

    assert channels == [
        {
            "source_task_id": "job-1:by_word:0",
            "source_worker_id": "worker-1",
            "target_task_id": "job-1:totals:0",
            "target_worker_id": "worker-2",
            "partitioning": "hash",
        }
    ]
