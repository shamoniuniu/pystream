"""WordCount 示例、镜像和 Compose 静态契约测试。"""

from __future__ import annotations

import shutil
from pathlib import Path

import yaml

from pystream.api import Partitioning, load_stream_graph
from pystream.artifact import UDFKind, UDFLoader, build_job_bundle
from pystream.service import build_parser

ROOT = Path(__file__).resolve().parents[2]


def test_wordcount_样例契约和_udf(tmp_path: Path) -> None:
    job_root = tmp_path / "wordcount"
    shutil.copytree(
        ROOT / "examples" / "wordcount", job_root, ignore=shutil.ignore_patterns("__pycache__")
    )
    graph = load_stream_graph(job_root / "job.yaml")

    assert graph.definition.job.name == "wordcount"
    assert graph.total_parallelism == 10
    assert graph.operator("words").parallelism == 2
    assert graph.operator("totals").parallelism == 3
    assert graph.operator("totals").window is not None
    assert graph.operator("totals").window.size == "10s"
    assert graph.incoming_edges("totals")[0].partitioning is Partitioning.HASH

    with UDFLoader(job_root, job_id="contract") as loader:
        normalize = loader.load("wordcount_udfs:normalize", UDFKind.MAP)
        key = loader.load("wordcount_udfs:word_key", UDFKind.KEY_SELECTOR)
        reduce = loader.load("wordcount_udfs:add_counts", UDFKind.REDUCE)
        first = normalize({"word": "APPLE", "count": 1})
        second = normalize({"word": "apple", "count": 1})
        assert first == {"word": "apple", "count": 1}
        assert key(first) == "apple"
        assert reduce(first, second) == {"word": "apple", "count": 2}


def test_intermediate_样例契约和_retract_udf(tmp_path: Path) -> None:
    job_root = tmp_path / "intermediate"
    shutil.copytree(
        ROOT / "examples" / "intermediate",
        job_root,
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    graph = load_stream_graph(job_root / "job.yaml")

    assert graph.definition.job.name == "event-time-retract"
    assert graph.total_parallelism == 12
    assert graph.operator("word_totals").emit_mode == "changelog"
    assert graph.data_stream("word_totals").changelog is True
    assert graph.operator("count_distribution").retract_udf is not None

    with UDFLoader(job_root, job_id="intermediate-contract") as loader:
        normalize = loader.load(
            "event_time_retract_udfs:normalize",
            UDFKind.MAP,
        )
        add_word_counts = loader.load(
            "event_time_retract_udfs:add_word_counts",
            UDFKind.REDUCE,
        )
        to_bucket = loader.load(
            "event_time_retract_udfs:to_count_bucket",
            UDFKind.MAP,
        )
        add_bucket = loader.load(
            "event_time_retract_udfs:add_bucket",
            UDFKind.REDUCE,
        )
        remove_bucket = loader.load(
            "event_time_retract_udfs:remove_bucket",
            UDFKind.RETRACT,
        )
        first = normalize({"word": "APPLE", "count": 1, "event_time": "2026-07-26T12:00:01Z"})
        total = add_word_counts(first, {"word": "apple", "count": 1})
        contribution = to_bucket(total)

        assert total == {"word": "apple", "count": 2}
        assert add_bucket(contribution, contribution) == {
            "count": 2,
            "word_count": 2,
        }
        assert remove_bucket(contribution, contribution) is None


def test_service_入口参数覆盖jobmanager和worker() -> None:
    parser = build_parser()
    manager = parser.parse_args(["jobmanager", "--artifact-root", "/tmp/artifacts"])
    worker = parser.parse_args(
        [
            "worker",
            "--worker-id",
            "worker-1",
            "--control-address",
            "http://worker-1:8081",
            "--data-host",
            "worker-1",
        ]
    )

    assert manager.port == 8080
    assert worker.port == 8081
    assert worker.data_port == 9000
    assert worker.slots == 4


def test_wordcount_制品忽略python缓存(tmp_path: Path) -> None:
    job_root = tmp_path / "wordcount"
    shutil.copytree(
        ROOT / "examples" / "wordcount", job_root, ignore=shutil.ignore_patterns("__pycache__")
    )
    cache = job_root / "__pycache__"
    cache.mkdir(exist_ok=True)
    (cache / "generated.pyc").write_bytes(b"generated")

    bundle = build_job_bundle(job_root, tmp_path / "bundles")

    assert {entry.path for entry in bundle.manifest.files} == {
        "job.yaml",
        "wordcount_udfs.py",
    }


def test_dockerfile_固定python311并使用非root多阶段镜像() -> None:
    content = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "PYTHON_VERSION=3.11.9" in content
    assert " AS builder" in content
    assert " AS runtime" in content
    assert "USER 10001:10001" in content
    assert "PYSTREAM_HOME=/opt/pystream" in content
    assert "COPY --from=builder /opt/venv /opt/venv" in content
    assert "latest" not in content


def test_compose_包含固定kafka_jobmanager_三worker和工具容器() -> None:
    compose = yaml.safe_load((ROOT / "deploy" / "compose.yaml").read_text(encoding="utf-8"))
    services = compose["services"]
    expected = {
        "kafka",
        "kafka-init",
        "jobmanager",
        "worker-1",
        "worker-2",
        "worker-3",
        "tools",
    }
    assert expected <= services.keys()
    assert services["kafka"]["image"] == "apache/kafka:3.9.1"
    assert services["kafka-init"]["image"] == "apache/kafka:3.9.1"
    assert services["kafka"]["environment"]["KAFKA_PROCESS_ROLES"] == "broker,controller"
    assert "pystream-output" in compose["volumes"]

    for index in range(1, 4):
        worker = services[f"worker-{index}"]
        command = worker["command"]
        assert command[command.index("--slots") + 1] == "4"
        assert command[command.index("--data-host") + 1] == f"worker-{index}"
        assert worker["user"] == "10001:10001"
        assert worker["read_only"] is True
        assert worker["cap_drop"] == ["ALL"]
        assert "healthcheck" in worker
        assert worker["deploy"]["resources"]["limits"]["memory"] == "512M"
        assert "pystream-output:/data/output" in worker["volumes"]

    init_command = services["kafka-init"]["command"]
    assert init_command[init_command.index("--partitions") + 1] == "2"
    assert services["jobmanager"]["ports"] == ["8080:8080"]
    assert services["tools"]["profiles"] == ["tools"]


def test_演示脚本齐全并被复制进运行镜像() -> None:
    names = {
        "produce_wordcount.py",
        "submit_wordcount.py",
        "wait_for_window.py",
        "verify_wordcount.py",
        "cleanup_wordcount.py",
    }
    scripts = ROOT / "scripts"

    assert names <= {path.name for path in scripts.glob("*.py")}
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "COPY --chown=pystream:pystream scripts /opt/pystream/scripts" in dockerfile
