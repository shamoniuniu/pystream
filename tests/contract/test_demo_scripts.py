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
intermediate_demo = _load_script("_intermediate_demo")
submit_intermediate = _load_script("submit_intermediate")
wait_for_window = _load_script("wait_for_window")
verify_wordcount = _load_script("verify_wordcount")
wait_for_checkpoint = _load_script("wait_for_checkpoint")
verify_intermediate = _load_script("verify_intermediate")
inject_worker_failure = _load_script("inject_worker_failure")


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


def test_submit_intermediate_仅在临时副本覆盖checkpoint周期(tmp_path: Path) -> None:
    job_dir = tmp_path / "source"
    job_dir.mkdir()
    original_job = {
        "execution": {
            "checkpoint": {
                "interval": "10s",
                "timeout": "30s",
            }
        }
    }
    (job_dir / "job.yaml").write_text(
        submit_intermediate.yaml.safe_dump(original_job),
        encoding="utf-8",
    )
    (job_dir / "udfs.py").write_text("VALUE = 1\n", encoding="utf-8")
    staging_root = tmp_path / "staging"
    staging_root.mkdir()

    staged_job_dir = submit_intermediate.stage_job_with_checkpoint_interval(
        job_dir,
        staging_root,
        "60s",
    )

    original = submit_intermediate.yaml.safe_load(
        (job_dir / "job.yaml").read_text(encoding="utf-8")
    )
    staged = submit_intermediate.yaml.safe_load(
        (staged_job_dir / "job.yaml").read_text(encoding="utf-8")
    )
    assert original["execution"]["checkpoint"]["interval"] == "10s"
    assert staged["execution"]["checkpoint"]["interval"] == "60s"
    assert (staged_job_dir / "udfs.py").read_text(encoding="utf-8") == "VALUE = 1\n"


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


def test_intermediate输出验证要求恢复窗口重复且baseline精确一次() -> None:
    observed = intermediate_demo.EXPECTED_ROWS + intermediate_demo.RECOVERY_PHASE_ROWS

    duplicates = verify_intermediate.validate_output(
        observed,
        require_duplicate=True,
    )

    assert duplicates == intermediate_demo.RECOVERY_PHASE_ROWS
    with pytest.raises(RuntimeError, match=r"未观察到.*重复"):
        verify_intermediate.validate_output(
            intermediate_demo.EXPECTED_ROWS,
            require_duplicate=True,
        )


def test_intermediate输出验证拒绝baseline重放和未知行() -> None:
    baseline_replayed = intermediate_demo.EXPECTED_ROWS + intermediate_demo.BASELINE_ROWS
    with pytest.raises(RuntimeError, match="Checkpoint 前"):
        verify_intermediate.validate_output(
            baseline_replayed,
            require_duplicate=False,
        )

    unexpected = intermediate_demo.EXPECTED_ROWS.copy()
    unexpected[("2026/07/29T00:00:15", 1, 99)] = 1
    with pytest.raises(RuntimeError, match="非预期行"):
        verify_intermediate.validate_output(
            unexpected,
            require_duplicate=False,
        )


@pytest.mark.asyncio
async def test_intermediate_kafka_offset等待metadata后严格验证lag(monkeypatch) -> None:
    class MetadataConsumer:
        def __init__(self) -> None:
            self.metadata_calls = 0
            self.topic_refreshes = 0
            self.stopped = False

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            self.stopped = True

        async def topics(self):
            self.topic_refreshes += 1
            return {"intermediate-words"}

        def partitions_for_topic(self, topic: str):
            assert topic == "intermediate-words"
            self.metadata_calls += 1
            return None if self.metadata_calls < 3 else {0, 1}

        async def end_offsets(self, partitions):
            return {partition: 6 for partition in partitions}

    class OffsetConsumer:
        def __init__(self) -> None:
            self.stopped = False

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            self.stopped = True

        async def committed(self, partition):
            return 6

    metadata_consumer = MetadataConsumer()
    offset_consumer = OffsetConsumer()

    def consumer_factory(*topics, **kwargs):
        if topics:
            assert topics == ("intermediate-words",)
            assert kwargs["group_id"] is None
            return metadata_consumer
        assert kwargs["group_id"] == "group-1"
        return offset_consumer

    monkeypatch.setattr(verify_intermediate, "AIOKafkaConsumer", consumer_factory)

    offsets = await verify_intermediate.kafka_offsets(
        "kafka:9092",
        "intermediate-words",
        "group-1",
        metadata_timeout=1,
        poll_interval=0,
    )

    assert metadata_consumer.metadata_calls == 3
    assert metadata_consumer.topic_refreshes == 3
    assert metadata_consumer.stopped
    assert offset_consumer.stopped
    assert offsets == [
        {
            "partition": 0,
            "committed_offset": 6,
            "end_offset": 6,
            "lag": 0,
        },
        {
            "partition": 1,
            "committed_offset": 6,
            "end_offset": 6,
            "lag": 0,
        },
    ]


def test_checkpoint_ready要求恢复后的新checkpoint() -> None:
    status = {
        "status": "RUNNING",
        "attempt": 1,
        "checkpoint": {"last_completed_id": 3},
        "tasks": [
            {"restored_checkpoint_id": 2},
            {"restored_checkpoint_id": 2},
        ],
    }

    assert wait_for_checkpoint.checkpoint_ready(
        status,
        min_checkpoint=1,
        min_attempt=1,
        require_post_recovery=True,
    )
    status["checkpoint"] = {"last_completed_id": 2}
    assert not wait_for_checkpoint.checkpoint_ready(
        status,
        min_checkpoint=1,
        min_attempt=1,
        require_post_recovery=True,
    )


def test_wait_for_checkpoint_触发请求使用命令行超时(monkeypatch) -> None:
    observed: dict[str, object] = {}

    class TriggerClient:
        def __init__(self, base_url: str, *, timeout: float) -> None:
            observed["base_url"] = base_url
            observed["timeout"] = timeout

        def trigger_checkpoint(self, job_id: str) -> dict[str, object]:
            observed["triggered_job_id"] = job_id
            return {"checkpoint_id": 1}

        def status(self, job_id: str) -> dict[str, object]:
            assert job_id == "job-1"
            return {
                "status": "RUNNING",
                "attempt": 0,
                "checkpoint": {"last_completed_id": 1},
            }

    monkeypatch.setattr(wait_for_checkpoint, "JobManagerClient", TriggerClient)

    assert (
        wait_for_checkpoint.main(
            [
                "--job-id",
                "job-1",
                "--jobmanager-url",
                "http://manager",
                "--trigger",
                "--timeout",
                "45",
            ]
        )
        == 0
    )
    assert observed == {
        "base_url": "http://manager",
        "timeout": 45.0,
        "triggered_job_id": "job-1",
    }


def test_failure_injection选择状态算子所在worker() -> None:
    status = {
        "tasks": [
            {
                "operator_id": "normalize",
                "worker_id": "worker-1",
            },
            {
                "operator_id": "word_totals",
                "worker_id": "worker-2",
            },
        ]
    }

    assert inject_worker_failure.select_target_worker(status, "word_totals") == "worker-2"


def test_failure_injection使用持久化恢复次数而非瞬态状态() -> None:
    assert inject_worker_failure._recovery_attempts({"recovery": {"attempts": 2}}) == 2
    with pytest.raises(RuntimeError, match=r"recovery\.attempts"):
        inject_worker_failure._recovery_attempts({"recovery": {"attempts": True}})
