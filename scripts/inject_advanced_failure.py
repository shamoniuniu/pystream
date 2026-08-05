"""Inject deterministic Worker, active JobManager, or MinIO failures."""

from __future__ import annotations

import argparse
import json
import subprocess
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from _advanced_demo import DEFAULT_JOBMANAGER_URL

from pystream.client import JobManagerClient

DEFAULT_COMPOSE_FILE = Path(__file__).resolve().parents[1] / "deploy" / "compose.advanced.yaml"
FAILURE_HOLDER_IMAGE = (
    "haproxy:3.2.5-alpine@sha256:8007effce89a08af0236b9529a0daab5b8b36fa939f4162f28201f1bf8731dbf"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inject advanced acceptance failures")
    parser.add_argument(
        "failure",
        choices=("worker", "active-jobmanager", "minio"),
    )
    parser.add_argument("--profile", choices=("core", "ha"), required=True)
    parser.add_argument("--compose-file", type=Path, default=DEFAULT_COMPOSE_FILE)
    parser.add_argument("--job-id")
    parser.add_argument("--jobmanager-url", default=DEFAULT_JOBMANAGER_URL)
    parser.add_argument("--operator-id", default="word_totals")
    parser.add_argument("--service")
    parser.add_argument("--evidence-path", type=Path)
    return parser


def _run(command: Sequence[str], *, timeout: float = 60.0) -> str:
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=timeout,
    )
    return result.stdout.strip()


def _compose_container(compose_file: Path, profile: str, service: str) -> str:
    container = _run(
        [
            "docker",
            "compose",
            "-f",
            str(compose_file),
            "--profile",
            profile,
            "ps",
            "-q",
            service,
        ]
    )
    if not container:
        raise RuntimeError(f"Compose service has no container: {service}")
    return container


def _inspect(container: str) -> dict[str, object]:
    documents = json.loads(_run(["docker", "inspect", container]))
    if not isinstance(documents, list) or len(documents) != 1:
        raise RuntimeError(f"docker inspect returned invalid data for {container}")
    document = documents[0]
    if not isinstance(document, dict):
        raise RuntimeError("docker inspect item must be an object")
    return document


def _container_state(document: dict[str, object]) -> dict[str, object]:
    state = document.get("State")
    if not isinstance(state, dict):
        raise RuntimeError("docker inspect is missing State")
    return {
        "restart_count": document.get("RestartCount"),
        "started_at": state.get("StartedAt"),
        "running": state.get("Running"),
    }


def _network_identity(document: dict[str, object]) -> tuple[str, str]:
    settings = document.get("NetworkSettings")
    networks = settings.get("Networks") if isinstance(settings, dict) else None
    if not isinstance(networks, dict) or len(networks) != 1:
        raise RuntimeError("Failure target must have exactly one Docker network")
    network, raw_details = next(iter(networks.items()))
    address = raw_details.get("IPAddress") if isinstance(raw_details, dict) else None
    if not isinstance(network, str) or not isinstance(address, str) or not address:
        raise RuntimeError("Failure target is missing its Docker network identity")
    return network, address


def _target_worker(status: dict[str, object], operator_id: str) -> str:
    tasks = status.get("tasks")
    if not isinstance(tasks, list):
        raise RuntimeError("Job status is missing tasks")
    for task in tasks:
        if not isinstance(task, dict) or task.get("operator_id") != operator_id:
            continue
        worker_id = task.get("worker_id")
        if isinstance(worker_id, str) and worker_id:
            return worker_id
    raise RuntimeError(f"No Worker hosts operator {operator_id!r}")


def _active_jobmanager(compose_file: Path) -> tuple[str, str, dict[str, object]]:
    probe = (
        "import json,ssl,urllib.request;"
        "c=ssl.create_default_context(cafile='/run/secrets/pystream-ca');"
        "c.load_cert_chain('/run/secrets/service-cert','/run/secrets/service-key');"
        "print(json.dumps(json.load(urllib.request.urlopen("
        "'https://localhost:8080/health',context=c,timeout=3))))"
    )
    observations: dict[str, object] = {}
    for service in ("jobmanager-1", "jobmanager-2"):
        container = _compose_container(compose_file, "ha", service)
        try:
            health = json.loads(_run(["docker", "exec", container, "python", "-c", probe]))
        except (subprocess.SubprocessError, json.JSONDecodeError):
            continue
        observations[service] = health
        if isinstance(health, dict) and health.get("role") == "ACTIVE":
            return service, container, observations
    raise RuntimeError(f"Exactly one active JobManager was not found: {observations}")


def inject(args: argparse.Namespace) -> dict[str, object]:
    injected_at = datetime.now(UTC).isoformat()
    compose_file = args.compose_file.resolve()
    if args.failure == "worker":
        if not args.job_id:
            raise ValueError("--job-id is required for Worker failure")
        status = JobManagerClient(args.jobmanager_url, timeout=10).status(args.job_id)
        worker_id = _target_worker(status, args.operator_id)
        service = (
            worker_id if args.profile == "core" else f"worker-ha-{worker_id.rsplit('-', 1)[1]}"
        )
        container = _compose_container(compose_file, args.profile, service)
        before = _container_state(_inspect(container))
        _run(
            [
                "docker",
                "exec",
                container,
                "/bin/sh",
                "-c",
                "kill -9 $(cat /proc/1/task/1/children)",
            ]
        )
        evidence = {
            "failure": "worker",
            "profile": args.profile,
            "job_id": args.job_id,
            "operator_id": args.operator_id,
            "worker_id": worker_id,
            "service": service,
            "container_id": container,
            "before": before,
            "injected_at": injected_at,
        }
    elif args.failure == "active-jobmanager":
        if args.profile != "ha":
            raise ValueError("active-jobmanager failure requires --profile ha")
        service, container, observations = _active_jobmanager(compose_file)
        before = _container_state(_inspect(container))
        _run(["docker", "stop", "-t", "0", container])
        evidence = {
            "failure": "active-jobmanager",
            "profile": args.profile,
            "service": service,
            "container_id": container,
            "health_observations": observations,
            "before": before,
            "injected_at": injected_at,
        }
    else:
        if args.profile != "ha":
            raise ValueError("MinIO failure requires --profile ha")
        service = args.service or "minio-1"
        if service not in {"minio-1", "minio-2", "minio-3", "minio-4"}:
            raise ValueError("--service must identify one MinIO node")
        container = _compose_container(compose_file, args.profile, service)
        inspected = _inspect(container)
        before = _container_state(inspected)
        network, address = _network_identity(inspected)
        holder_name = f"pystream-advanced-{service}-failure-holder"
        subprocess.run(
            ["docker", "rm", "-f", holder_name],
            check=False,
            capture_output=True,
            timeout=30,
        )
        _run(["docker", "rm", "-f", container])
        holder = _run(
            [
                "docker",
                "create",
                "--name",
                holder_name,
                "--label",
                "com.docker.compose.project=pystream-advanced",
                "--label",
                "com.docker.compose.service=failure-holder",
                "--network",
                network,
                "--ip",
                address,
                "--network-alias",
                service,
                "--entrypoint",
                "/bin/sh",
                FAILURE_HOLDER_IMAGE,
                "-c",
                "sleep 86400",
            ]
        )
        _run(["docker", "start", holder])
        evidence = {
            "failure": "minio",
            "profile": args.profile,
            "service": service,
            "container_id": container,
            "before": before,
            "failure_mode": "container_stopped_with_non_listening_ip_holder",
            "network": network,
            "reserved_address": address,
            "holder_container_id": holder,
            "injected_at": injected_at,
        }
    if args.evidence_path is not None:
        path = args.evidence_path.resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)
    return evidence


def main(argv: Sequence[str] | None = None) -> int:
    evidence = inject(build_parser().parse_args(argv))
    print(json.dumps(evidence, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
