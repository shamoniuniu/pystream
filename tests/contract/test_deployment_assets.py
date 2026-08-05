"""WordCount 示例、镜像和 Compose 静态契约测试。"""

from __future__ import annotations

import shutil
import tomllib
from pathlib import Path

import yaml

from pystream import __version__
from pystream.api import Partitioning, load_stream_graph
from pystream.artifact import UDFKind, UDFLoader, build_job_bundle
from pystream.service import build_parser

ROOT = Path(__file__).resolve().parents[2]


def test_v03版本身份一致() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    compose = yaml.safe_load((ROOT / "deploy" / "compose.yaml").read_text(encoding="utf-8"))

    assert project["project"]["version"] == "0.3.0"
    assert __version__ == "0.3.0"
    assert compose["x-pystream-service"]["image"] == "pystream:0.3.0"


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
    manager = parser.parse_args(
        [
            "jobmanager",
            "--artifact-root",
            "/tmp/artifacts",
            "--object-store-endpoint",
            "http://object-store:9000",
            "--object-store-access-key-file",
            "/run/secrets/access-key",
            "--object-store-secret-key-file",
            "/run/secrets/secret-key",
            "--jobmanager-id",
            "jobmanager-1",
        ]
    )
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
    assert manager.object_store_bucket == "pystream"
    assert manager.object_store_access_key_file.as_posix() == "/run/secrets/access-key"
    assert manager.jobmanager_id == "jobmanager-1"
    assert manager.leader_lease_ttl == 10.0
    assert manager.leader_renew_interval == 3.0
    assert manager.leader_poll_interval == 1.0
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
    assert (
        "PYTHON_BASE_DIGEST=sha256:8fb099199b9f2d70342674bd9dbccd3ed03a258f26bbd1d556822c6dfc60c317"
    ) in content
    assert " AS builder" in content
    assert " AS runtime" in content
    assert "USER 10001:10001" in content
    assert "PYSTREAM_HOME=/opt/pystream" in content
    assert "COPY --from=builder /opt/venv /opt/venv" in content
    assert "mkdir -p /data/artifacts /data/checkpoints /data/output" in content
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
    kafka_image = (
        "apache/kafka:3.9.1@sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed"
    )
    assert services["kafka"]["image"] == kafka_image
    assert services["kafka-init"]["image"] == kafka_image
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
        assert "pystream-checkpoints:/data/checkpoints" in worker["volumes"]

    init_command = services["kafka-init"]["command"]
    init_script = init_command[-1]
    assert "words intermediate-words" in init_script
    assert "--partitions 2" in init_script
    assert services["jobmanager"]["ports"] == ["8080:8080"]
    assert "pystream-checkpoints:/data/checkpoints" in services["jobmanager"]["volumes"]
    assert services["tools"]["profiles"] == ["tools"]


def test_advanced_compose_包含单节点core和四节点双盘ha对象存储() -> None:
    compose_path = ROOT / "deploy" / "compose.advanced.yaml"
    content = compose_path.read_text(encoding="utf-8")
    compose = yaml.safe_load(content)
    services = compose["services"]
    pinned_images = {
        services["minio-core"]["image"],
        services["object-store-init-core"]["image"],
        services["object-store"]["image"],
        services["kafka"]["image"],
    }

    assert compose["name"] == "pystream-advanced"
    assert all("@sha256:" in image and ":latest" not in image for image in pinned_images)
    assert services["minio-core"]["profiles"] == ["core"]
    assert services["minio-core"]["command"][-1] == "/data"
    assert services["object-store-init-core"]["profiles"] == ["core"]
    assert services["object-store"]["profiles"] == ["ha"]
    assert services["object-store-init-ha"]["profiles"] == ["ha"]
    assert services["object-store-init-core"]["environment"]["MC_CONFIG_DIR"] == "/tmp/.mc"
    assert services["object-store-init-ha"]["environment"]["MC_CONFIG_DIR"] == "/tmp/.mc"

    for index in range(1, 5):
        node = services[f"minio-{index}"]
        assert node["profiles"] == ["ha"]
        assert node["command"][-1] == "https://minio-{1...4}/data{1...2}"
        assert node["volumes"] == [
            f"pystream-minio-{index}-data-1:/data1",
            f"pystream-minio-{index}-data-2:/data2",
        ]
        assert node["image"] == services["minio-core"]["image"]

    storage_config = (ROOT / "deploy" / "haproxy" / "storage.cfg").read_text(encoding="utf-8")
    assert (
        "http-check send meth GET uri /minio/health/ready ver HTTP/1.1 hdr Host minio"
    ) in storage_config
    assert "option httpclose" in storage_config
    assert all(
        f"server minio-{index} minio-{index}:9000 ssl verify required" in storage_config
        for index in range(1, 5)
    )


def test_advanced_compose_包含双jobmanager和leader_only路由() -> None:
    compose = yaml.safe_load(
        (ROOT / "deploy" / "compose.advanced.yaml").read_text(encoding="utf-8")
    )
    services = compose["services"]

    for index in range(1, 3):
        manager = services[f"jobmanager-{index}"]
        command = manager["command"]
        assert manager["profiles"] == ["ha"]
        assert command[command.index("--jobmanager-id") + 1] == f"jobmanager-{index}"
        assert (
            manager["environment"]["PYSTREAM_OBJECT_STORE_ENDPOINT"] == "https://object-store:9000"
        )
        assert manager["depends_on"]["object-store-init-ha"]["condition"] == (
            "service_completed_successfully"
        )

    router = services["jobmanager-router"]
    assert router["profiles"] == ["ha"]
    assert router["ports"] == ["8080:8080"]
    assert router["expose"] == ["8082"]
    assert "./haproxy/advanced.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro" in router["volumes"]
    assert "https://127.0.0.1:8080/health" in router["healthcheck"]["test"][-1]

    for index in range(1, 4):
        worker = services[f"worker-ha-{index}"]
        command = worker["command"]
        assert worker["profiles"] == ["ha"]
        assert command[command.index("--jobmanager-url") + 1] == ("https://jobmanager-router:8082")
        assert worker["depends_on"]["jobmanager-router"]["condition"] == "service_healthy"

    router_config = (ROOT / "deploy" / "haproxy" / "advanced.cfg").read_text(encoding="utf-8")
    assert "bind :8080" in router_config
    assert "bind :8082" in router_config
    assert "ssl crt /run/secrets/haproxy-pem" in router_config
    assert "mode tcp" in router_config
    assert "check-ssl verify required" in router_config
    assert "uri /health/leader" in router_config
    assert "uri /health/active" in router_config
    assert "nameserver docker_dns 127.0.0.11:53" in router_config
    assert router_config.count("resolvers docker resolve-prefer ipv4 init-addr libc,none") == 4
    assert all(
        router_config.count(f"server jobmanager-{index} jobmanager-{index}:8080") == 2
        for index in range(1, 3)
    )


def test_advanced_compose_对象存储凭据只通过secret文件注入() -> None:
    compose = yaml.safe_load(
        (ROOT / "deploy" / "compose.advanced.yaml").read_text(encoding="utf-8")
    )
    services = compose["services"]
    secret_names = {"object-store-access-key", "object-store-secret-key"}

    assert secret_names < set(compose["secrets"])
    assert all(
        definition["file"].startswith("${PYSTREAM_OBJECT_STORE_")
        for name, definition in compose["secrets"].items()
        if name in secret_names
    )
    for name in ("minio-core", "minio-1", "minio-2", "minio-3", "minio-4"):
        service = services[name]
        mounted = {item if isinstance(item, str) else item["source"] for item in service["secrets"]}
        assert secret_names < mounted
        assert "pystream-ca" in mounted
        assert set(service["environment"]) == {
            "MINIO_ROOT_USER_FILE",
            "MINIO_ROOT_PASSWORD_FILE",
            "MC_CONFIG_DIR",
        }
    for name in (
        "jobmanager",
        "jobmanager-1",
        "jobmanager-2",
        "worker-1",
        "worker-2",
        "worker-3",
        "worker-ha-1",
        "worker-ha-2",
        "worker-ha-3",
    ):
        service = services[name]
        mounted = {item if isinstance(item, str) else item["source"] for item in service["secrets"]}
        assert secret_names < mounted
        assert "pystream-ca" in mounted
        environment = service["environment"]
        assert environment["PYSTREAM_OBJECT_STORE_ACCESS_KEY_FILE"].startswith("/run/secrets/")
        assert environment["PYSTREAM_OBJECT_STORE_SECRET_KEY_FILE"].startswith("/run/secrets/")
        assert environment["PYSTREAM_OBJECT_STORE_CA_FILE"] == "/run/secrets/pystream-ca"


def test_advanced_compose_启用tls_kafka和prometheus安全契约() -> None:
    compose = yaml.safe_load(
        (ROOT / "deploy" / "compose.advanced.yaml").read_text(encoding="utf-8")
    )
    services = compose["services"]
    kafka = services["kafka"]

    assert kafka["environment"]["KAFKA_LISTENERS"].startswith("SSL://")
    assert kafka["environment"]["KAFKA_SSL_CLIENT_AUTH"] == "required"
    assert {
        "kafka-server-keystore",
        "kafka-truststore",
        "kafka-keystore-password",
    } <= {item if isinstance(item, str) else item["source"] for item in kafka["secrets"]}
    assert "PLAINTEXT://kafka:9092" not in str(kafka)

    prometheus = services["prometheus"]
    assert "@sha256:" in prometheus["image"]
    assert prometheus["profiles"] == ["core", "ha"]
    assert {
        "pystream-ca",
        "prometheus-cert",
        "prometheus-key",
    } == {item if isinstance(item, str) else item["source"] for item in prometheus["secrets"]}
    assert (
        "./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro" in (prometheus["volumes"])
    )

    for name in (
        "worker-1",
        "worker-2",
        "worker-3",
        "worker-ha-1",
        "worker-ha-2",
        "worker-ha-3",
    ):
        worker = services[name]
        command = worker["command"]
        assert command[command.index("--control-address") + 1].startswith("https://")
        assert command[command.index("--jobmanager-url") + 1].startswith("https://")
        assert worker["environment"]["PYSTREAM_KAFKA_CERT_FILE"] == ("/run/secrets/service-cert")

    storage_config = (ROOT / "deploy" / "haproxy" / "storage.cfg").read_text(encoding="utf-8")
    assert "bind :9000 ssl crt /run/secrets/object-store-pem" in storage_config
    assert storage_config.count("ssl verify required ca-file /run/secrets/pystream-ca") == 4


def test_演示脚本齐全并被复制进运行镜像() -> None:
    names = {
        "produce_wordcount.py",
        "submit_wordcount.py",
        "wait_for_window.py",
        "verify_wordcount.py",
        "cleanup_wordcount.py",
        "produce_intermediate.py",
        "submit_intermediate.py",
        "wait_for_intermediate.py",
        "wait_for_checkpoint.py",
        "inject_worker_failure.py",
        "verify_intermediate.py",
        "cleanup_intermediate.py",
    }
    scripts = ROOT / "scripts"

    assert names <= {path.name for path in scripts.glob("*.py")}
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "COPY --chown=pystream:pystream scripts /opt/pystream/scripts" in dockerfile
    failure_injection = (scripts / "inject_worker_failure.py").read_text(encoding="utf-8")
    normalized_failure_injection = " ".join(failure_injection.split())
    assert '"docker", "exec"' in normalized_failure_injection
    assert '"kill -9 $(cat /proc/1/task/1/children)"' in normalized_failure_injection
    assert '"docker", "kill"' not in normalized_failure_injection


def test_中级验收使用可超时的create_start_inspect编排并验证清理() -> None:
    content = (ROOT / "scripts" / "run_intermediate_acceptance.ps1").read_text(encoding="utf-8")

    assert "System.Diagnostics.Process" in content
    assert "WaitForExit($TimeoutSeconds * 1000)" in content
    assert "Initialize-DockerProject" in content
    assert '"network", "create"' in content
    assert '"volume", "create"' in content
    assert "intermediate-docker-resources.tsv" in content
    assert "Start-DockerContainers" in content
    assert "docker_start_cli_retry=" in content
    assert "docker start timed out after 3 attempts" in content
    normalized_content = " ".join(content.split())
    assert '"--checkpoint-interval" "1h"' in normalized_content
    assert normalized_content.count('"--trigger"') == 2
    assert "Invoke-Docker start" not in content
    assert "Wait-ContainerExit" in content
    assert "{{.State.Status}} {{.State.ExitCode}}" in content
    assert '@("wait",' not in content
    assert "Assert-ComposeProjectRemoved" in content
    assert "Invoke-Docker compose" not in content
    assert " run --rm " not in content
    assert "network ls" not in content
    assert "volume ls" not in content
    assert "ps -aq" not in content
