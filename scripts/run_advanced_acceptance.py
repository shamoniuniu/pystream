"""Run reproducible Core or HA advanced Docker acceptance."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import ssl
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from pystream.client import ClientError, JobManagerClient

ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "deploy" / "compose.advanced.yaml"
PKI_ROOT = ROOT / "run" / "pki"
IMAGE = "pystream:0.3.0"
CORE_SERVICES = (
    "kafka",
    "kafka-init",
    "minio-core",
    "object-store-init-core",
    "jobmanager",
    "jobmanager-router-core",
    "worker-1",
    "worker-2",
    "worker-3",
    "prometheus",
)
HA_SERVICES = (
    "kafka",
    "kafka-init",
    "minio-1",
    "minio-2",
    "minio-3",
    "minio-4",
    "object-store",
    "object-store-init-ha",
    "jobmanager-1",
    "jobmanager-2",
    "jobmanager-router",
    "worker-ha-1",
    "worker-ha-2",
    "worker-ha-3",
    "prometheus",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run advanced Docker acceptance")
    parser.add_argument("--profile", choices=("core", "ha"), required=True)
    parser.add_argument("--evidence-path", type=Path, required=True)
    parser.add_argument("--runtime-log-path", type=Path, required=True)
    parser.add_argument("--keep-environment", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    return parser


class Acceptance:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.profile = args.profile
        self.evidence: dict[str, object] = {
            "schema_version": 1,
            "profile": self.profile,
            "started_at": datetime.now(UTC).isoformat(),
            "scenarios": [],
        }
        self.env = os.environ.copy()
        self.env["PYSTREAM_PKI_ROOT"] = str(PKI_ROOT)
        self.env["PYSTREAM_ENABLE_TEST_HOOKS"] = "true"
        self.env["PYSTREAM_EXTERNAL_TOKEN_FILE"] = str(PKI_ROOT / "secrets" / "external-token")
        self.env["PYSTREAM_TLS_CA_FILE"] = str(PKI_ROOT / "ca.crt")
        self.tool_url = (
            "https://jobmanager-router-core:8080"
            if self.profile == "core"
            else "https://jobmanager-router:8080"
        )
        self.env["PYSTREAM_JOBMANAGER_URL"] = self.tool_url
        self.client: JobManagerClient | None = None

    def run(self) -> None:
        self._down()
        self._generate_pki()
        if not self.args.skip_build:
            self._command(["docker", "build", "-t", IMAGE, "."])
        self._up()
        self.client = JobManagerClient(
            "https://localhost:8080",
            timeout=120,
            token_file=PKI_ROOT / "secrets" / "external-token",
            ca_file=PKI_ROOT / "ca.crt",
        )
        if self.profile == "core":
            self._run_core()
        else:
            self._run_ha()
        self.evidence["result"] = "passed"

    def finish(self, failure: BaseException | None) -> None:
        runtime_error: str | None = None
        try:
            self._save_runtime_logs()
        except Exception as exc:  # pragma: no cover - best effort after daemon failure
            runtime_error = f"{type(exc).__name__}: {exc}"
        cleanup_error: str | None = None
        if not self.args.keep_environment:
            try:
                self._down()
                shutil.rmtree(PKI_ROOT, ignore_errors=True)
                self.evidence["resource_cleanup"] = self._resource_counts()
                if any(self.evidence["resource_cleanup"].values()):
                    raise RuntimeError(
                        f"Advanced Docker resources remain: {self.evidence['resource_cleanup']}"
                    )
                self.evidence["secret_directory_removed"] = not PKI_ROOT.exists()
            except Exception as exc:
                cleanup_error = f"{type(exc).__name__}: {exc}"
        if runtime_error is not None:
            self.evidence["runtime_log_error"] = runtime_error
        if failure is not None:
            self.evidence["result"] = "failed"
            self.evidence["error"] = f"{type(failure).__name__}: {failure}"
        if cleanup_error is not None:
            self.evidence["result"] = "failed"
            self.evidence["cleanup_error"] = cleanup_error
        self.evidence["finished_at"] = datetime.now(UTC).isoformat()
        path = self.args.evidence_path.resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.evidence, indent=2) + "\n", encoding="utf-8")
        if cleanup_error is not None:
            raise RuntimeError(cleanup_error)

    def _run_core(self) -> None:
        self._basic_regression()
        self._at_least_once_regression()
        self._tool("produce_advanced.py")
        baseline = self._run_exactly_once_scenario("baseline")
        before_barrier = self._run_exactly_once_scenario(
            "before_barrier",
            hook="before_barrier",
        )
        before_decision = self._run_exactly_once_scenario(
            "before_decision",
            hook="before_decision",
        )
        reference = baseline["verification"]["visible_rows"]
        for scenario in (before_barrier, before_decision):
            if scenario["verification"]["visible_rows"] != reference:
                raise RuntimeError(f"Committed output diff is non-zero: {scenario['name']}")
            scenario["committed_output_diff_rows"] = 0
        self.evidence["core"] = {
            "baseline_job_id": baseline["job_id"],
            "fault_scenarios": [before_barrier["name"], before_decision["name"]],
            "committed_output_diff_rows": 0,
            "kafka_lag": 0,
        }

    def _run_ha(self) -> None:
        security = self._security_evidence()
        leadership = self._leadership()
        if leadership["active_count"] != 1:
            raise RuntimeError(f"Expected one active JobManager: {leadership}")
        self._tool("produce_advanced.py")
        job_id = self._submit()
        client = self._client()
        before = client.status(job_id)
        before_epoch = _integer(before, "coordinator_epoch")
        before_attempt = _integer(before, "attempt")
        client.arm_checkpoint_test_hook("after_decision")
        with ThreadPoolExecutor(max_workers=1) as executor:
            checkpoint = executor.submit(client.trigger_checkpoint, job_id)
            reached = self._wait_hook("after_decision", timeout=30)
            injection = self._inject(
                "active-jobmanager",
                job_id=job_id,
                evidence_name="ha-active-jobmanager-failure.json",
            )
            failed_at = time.monotonic()
            self._expect_checkpoint_disconnect(checkpoint)
        recovered = self._wait_status(
            job_id,
            lambda status: (
                status.get("status") == "RUNNING"
                and isinstance(status.get("coordinator_epoch"), int)
                and status["coordinator_epoch"] > before_epoch
                and isinstance(status.get("checkpoint"), dict)
                and status["checkpoint"].get("last_finalized_id")
                == reached["context"]["checkpoint_id"]
            ),
            timeout=30,
        )
        takeover_seconds = time.monotonic() - failed_at
        verification_after_takeover = self._verify(
            job_id,
            min_attempt=before_attempt + 1,
            min_epoch=before_epoch + 1,
        )
        restarted_standby = self._restart_jobmanager_standby(str(injection["service"]))
        storage_started = time.monotonic()
        minio = self._inject(
            "minio",
            job_id=job_id,
            evidence_name="ha-minio-failure.json",
        )
        storage_backend_down_seconds = self._wait_storage_backend_down(
            str(minio["service"]),
            timeout=20,
        )
        storage_leader_health = self._wait_external_leader(
            timeout=120,
            stable_for=12,
        )
        storage_ready = self._wait_status(
            job_id,
            lambda status: status.get("status") == "RUNNING",
            timeout=30,
        )
        storage_recovery_seconds = time.monotonic() - storage_started
        checkpoint_after_storage_failure = self._trigger_checkpoint_retry(
            job_id,
            timeout=120,
        )
        status_after_storage_failure = client.status(job_id)

        worker_before = client.status(job_id)
        worker_started = time.monotonic()
        worker = self._inject(
            "worker",
            job_id=job_id,
            evidence_name="ha-worker-failure.json",
        )
        worker_recovered = self._wait_status(
            job_id,
            lambda status: (
                status.get("status") == "RUNNING"
                and isinstance(status.get("attempt"), int)
                and status["attempt"] > worker_before["attempt"]
            ),
            timeout=60,
        )
        worker_recovery_seconds = time.monotonic() - worker_started
        self._trigger_checkpoint_retry(job_id, timeout=60)
        final_verification = self._verify(
            job_id,
            min_attempt=_integer(worker_recovered, "attempt"),
            min_epoch=_integer(recovered, "coordinator_epoch"),
        )
        if final_verification["visible_rows"] != verification_after_takeover["visible_rows"]:
            raise RuntimeError("HA combined-fault committed output diff is non-zero")
        metrics = self._prometheus_evidence()
        scenario = {
            "name": "ha_combined_faults",
            "job_id": job_id,
            "hook_reached": reached,
            "active_jobmanager_failure": injection,
            "takeover_seconds": takeover_seconds,
            "status_after_takeover": recovered,
            "verification_after_takeover": verification_after_takeover,
            "restarted_standby": restarted_standby,
            "minio_failure": minio,
            "storage_backend_down_seconds": storage_backend_down_seconds,
            "storage_recovery_seconds": storage_recovery_seconds,
            "storage_leader_health": storage_leader_health,
            "storage_ready_status": storage_ready,
            "checkpoint_after_storage_failure": checkpoint_after_storage_failure,
            "status_after_storage_failure": status_after_storage_failure,
            "worker_failure": worker,
            "worker_recovery_seconds": worker_recovery_seconds,
            "status_after_worker_recovery": worker_recovered,
            "final_verification": final_verification,
            "committed_output_diff_rows": 0,
        }
        self.evidence["scenarios"].append(scenario)
        self.evidence["ha"] = {
            "leadership_before_failure": leadership,
            "security": security,
            "takeover_seconds": takeover_seconds,
            "worker_recovery_seconds": worker_recovery_seconds,
            "storage_read_write_after_node_loss": True,
            "storage_recovery_seconds": storage_recovery_seconds,
            "restarted_failed_active_as_standby": True,
            "committed_output_diff_rows": 0,
            "kafka_lag": 0,
            "prometheus": metrics,
        }

    def _run_exactly_once_scenario(
        self,
        name: str,
        *,
        hook: str | None = None,
    ) -> dict[str, object]:
        client = self._client()
        job_id = self._submit()
        before = client.status(job_id)
        scenario: dict[str, object] = {
            "name": name,
            "job_id": job_id,
            "status_before": before,
        }
        if hook is None:
            scenario["checkpoint"] = client.trigger_checkpoint(job_id)
        else:
            client.arm_checkpoint_test_hook(hook)
            with ThreadPoolExecutor(max_workers=1) as executor:
                checkpoint = executor.submit(client.trigger_checkpoint, job_id)
                reached = self._wait_hook(hook, timeout=30)
                injection_started = time.monotonic()
                injection = self._inject(
                    "worker",
                    job_id=job_id,
                    evidence_name=f"core-{hook}-worker-failure.json",
                )
                client.release_checkpoint_test_hook(hook, action="fail")
                self._expect_checkpoint_failure(checkpoint)
            recovered = self._wait_status(
                job_id,
                lambda status: (
                    status.get("status") == "RUNNING"
                    and isinstance(status.get("attempt"), int)
                    and status["attempt"] > before["attempt"]
                ),
                timeout=60,
            )
            recovery_seconds = time.monotonic() - injection_started
            scenario.update(
                {
                    "hook_reached": reached,
                    "worker_failure": injection,
                    "recovery_seconds": recovery_seconds,
                    "status_after_recovery": recovered,
                    "checkpoint_after_recovery": client.trigger_checkpoint(job_id),
                }
            )
        scenario["verification"] = self._verify(
            job_id,
            min_attempt=_integer(client.status(job_id), "attempt"),
            min_epoch=_integer(client.status(job_id), "coordinator_epoch"),
        )
        self._tool(
            "cleanup_advanced.py",
            "--job-id",
            job_id,
            "--keep-output",
        )
        self.evidence["scenarios"].append(scenario)
        return scenario

    def _basic_regression(self) -> None:
        self._tool("produce_wordcount.py")
        job_id = self._job_id(self._tool("submit_wordcount.py"))
        self._tool("wait_for_window.py", "--job-id", job_id)
        self._tool("verify_wordcount.py", "--job-id", job_id)
        self._tool("cleanup_wordcount.py", "--job-id", job_id)
        self.evidence["basic_wordcount_regression"] = "passed"

    def _at_least_once_regression(self) -> None:
        self._tool("produce_intermediate.py", "--phase", "baseline")
        job_id = self._job_id(
            self._tool(
                "submit_intermediate.py",
                "--checkpoint-interval",
                "1h",
            )
        )
        self._tool(
            "wait_for_intermediate.py",
            "--job-id",
            job_id,
            "--phase",
            "baseline",
        )
        self._tool(
            "wait_for_checkpoint.py",
            "--job-id",
            job_id,
            "--min-checkpoint",
            "1",
            "--trigger",
        )
        self._tool("cleanup_intermediate.py", "--job-id", job_id)
        self.evidence["explicit_at_least_once_regression"] = "passed"

    def _security_evidence(self) -> dict[str, object]:
        url = "https://localhost:8080/v1/workers"
        context = ssl.create_default_context(cafile=str(PKI_ROOT / "ca.crt"))
        statuses: dict[str, object] = {}
        for name, authorization in (
            ("missing_token", None),
            ("wrong_token", "Bearer invalid"),
        ):
            headers = {} if authorization is None else {"Authorization": authorization}
            try:
                urlopen(Request(url, headers=headers), context=context, timeout=5)
            except HTTPError as exc:
                statuses[name] = exc.code
            else:
                statuses[name] = 200
        try:
            urlopen(url, timeout=5)
        except URLError as exc:
            statuses["wrong_ca_rejected"] = isinstance(
                getattr(exc, "reason", None),
                ssl.SSLCertVerificationError,
            )
        else:
            statuses["wrong_ca_rejected"] = False
        statuses["valid_token_status"] = (
            200 if isinstance(self._client().workers().get("workers"), list) else 500
        )
        internal_probe = (
            "import ssl,urllib.error,urllib.request;"
            "c=ssl.create_default_context(cafile='/run/secrets/pystream-ca');"
            "c.load_cert_chain('/run/secrets/tools-cert','/run/secrets/tools-key');"
            "r=urllib.request.Request('https://worker-ha-1:8081/tasks/not-allowed');"
            "\ntry:\n urllib.request.urlopen(r,context=c,timeout=5)\n"
            "except urllib.error.HTTPError as e:\n print(e.code)"
        )
        raw = self._tool_python("-c", internal_probe).strip()
        statuses["wrong_internal_identity_status"] = int(raw.splitlines()[-1])
        if statuses != {
            "missing_token": 401,
            "wrong_token": 401,
            "wrong_ca_rejected": True,
            "valid_token_status": 200,
            "wrong_internal_identity_status": 403,
        }:
            raise RuntimeError(f"Security acceptance failed: {statuses}")
        return statuses

    def _leadership(self) -> dict[str, object]:
        health: dict[str, object] = {}
        probe = (
            "import json,ssl,urllib.request;"
            "c=ssl.create_default_context(cafile='/run/secrets/pystream-ca');"
            "c.load_cert_chain('/run/secrets/service-cert','/run/secrets/service-key');"
            "print(json.dumps(json.load(urllib.request.urlopen("
            "'https://localhost:8080/health',context=c,timeout=3))))"
        )
        for service in ("jobmanager-1", "jobmanager-2"):
            container = self._compose_output("ps", "-q", service).strip()
            document = json.loads(
                self._command(
                    ["docker", "exec", container, "python", "-c", probe],
                    capture=True,
                )
            )
            health[service] = document
        active = [
            service
            for service, document in health.items()
            if isinstance(document, dict) and document.get("role") == "ACTIVE"
        ]
        return {
            "active_count": len(active),
            "active_service": active[0] if len(active) == 1 else None,
            "health": health,
        }

    def _prometheus_evidence(self) -> dict[str, object]:
        with urlopen("http://localhost:9090/api/v1/targets", timeout=10) as response:
            targets = json.load(response)
        with urlopen("http://localhost:9090/api/v1/rules", timeout=10) as response:
            rules = json.load(response)
        active_targets = targets["data"]["activeTargets"]
        healthy_targets = [
            target
            for target in active_targets
            if target.get("health") == "up"
            and target.get("labels", {}).get("job") in {"pystream-jobmanagers", "pystream-workers"}
        ]
        rule_groups = rules["data"]["groups"]
        rule_count = sum(len(group.get("rules", [])) for group in rule_groups)
        if len(healthy_targets) != 5 or rule_count != 8:
            raise RuntimeError(
                f"Prometheus evidence mismatch: targets={len(healthy_targets)}, rules={rule_count}"
            )
        return {
            "healthy_jobmanager_worker_targets": len(healthy_targets),
            "rule_count": rule_count,
        }

    def _restart_jobmanager_standby(self, service: str) -> dict[str, object]:
        if service not in {"jobmanager-1", "jobmanager-2"}:
            raise RuntimeError(f"Invalid stopped JobManager service: {service}")
        self._command([*self._compose_prefix(), "start", service])
        deadline = time.monotonic() + 30
        last_health = "unknown"
        while time.monotonic() < deadline:
            container = self._compose_output("ps", "-q", service).strip()
            last_health = self._command(
                [
                    "docker",
                    "inspect",
                    "--format",
                    "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                    container,
                ],
                capture=True,
            ).strip()
            if last_health == "healthy":
                leadership = self._leadership()
                if leadership["active_count"] == 1:
                    return {
                        "service": service,
                        "health": last_health,
                        "leadership": leadership,
                    }
            time.sleep(0.5)
        raise TimeoutError(
            f"Restarted JobManager did not become healthy standby: {service}={last_health}"
        )

    def _wait_storage_backend_down(self, service: str, *, timeout: float) -> float:
        container = self._compose_output("ps", "-q", "object-store").strip()
        marker = f"Server minio_cluster/{service} is DOWN"
        started = time.monotonic()
        deadline = started + timeout
        while time.monotonic() < deadline:
            result = subprocess.run(
                ["docker", "logs", container],
                cwd=ROOT,
                env=self.env,
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=15,
            )
            logs = result.stdout + result.stderr
            if marker in logs:
                return time.monotonic() - started
            time.sleep(0.25)
        raise TimeoutError(f"Storage proxy did not remove failed backend {service}")

    def _wait_external_leader(
        self,
        *,
        timeout: float,
        stable_for: float = 0,
    ) -> dict[str, object]:
        context = ssl.create_default_context(cafile=str(PKI_ROOT / "ca.crt"))
        deadline = time.monotonic() + timeout
        last: dict[str, object] = {}
        stable_epoch: int | None = None
        stable_since: float | None = None
        while time.monotonic() < deadline:
            try:
                with urlopen(
                    "https://localhost:8080/health/leader",
                    context=context,
                    timeout=3,
                ) as response:
                    document = json.load(response)
            except (HTTPError, URLError, TimeoutError):
                time.sleep(0.25)
                continue
            if isinstance(document, dict):
                last = document
                if document.get("role") == "ACTIVE" and document.get("leader_ready") is True:
                    epoch = document.get("coordinator_epoch")
                    if not isinstance(epoch, int):
                        stable_epoch = None
                        stable_since = None
                    elif epoch != stable_epoch:
                        stable_epoch = epoch
                        stable_since = time.monotonic()
                    elif stable_since is not None and time.monotonic() - stable_since >= stable_for:
                        return document
                else:
                    stable_epoch = None
                    stable_since = None
            time.sleep(0.25)
        raise TimeoutError(f"No ready leader after storage failure: {last}")

    def _wait_hook(self, hook: str, *, timeout: float) -> dict[str, object]:
        deadline = time.monotonic() + timeout
        last: dict[str, object] = {}
        while time.monotonic() < deadline:
            last = self._client().checkpoint_test_hook()
            if last.get("hook") == hook and last.get("status") == "REACHED":
                return last
            time.sleep(0.1)
        raise TimeoutError(f"Checkpoint hook {hook} was not reached: {last}")

    def _wait_status(
        self,
        job_id: str,
        predicate: Callable[[dict[str, object]], bool],
        *,
        timeout: float,
    ) -> dict[str, object]:
        deadline = time.monotonic() + timeout
        last: dict[str, object] = {}
        while time.monotonic() < deadline:
            try:
                last = self._client().status(job_id)
            except ClientError:
                time.sleep(0.2)
                continue
            if predicate(last):
                return last
            if last.get("status") in {"FAILED", "REJECTED", "CANCELLED"}:
                raise RuntimeError(f"Job entered terminal state while waiting: {last}")
            time.sleep(0.2)
        raise TimeoutError(f"Job status did not reach acceptance state: {last}")

    def _trigger_checkpoint_retry(
        self,
        job_id: str,
        *,
        timeout: float,
    ) -> dict[str, object]:
        deadline = time.monotonic() + timeout
        last_error: ClientError | None = None
        while time.monotonic() < deadline:
            try:
                return self._client().trigger_checkpoint(job_id)
            except ClientError as exc:
                last_error = exc
                time.sleep(0.5)
        raise TimeoutError(f"Checkpoint did not succeed after HA transition: {last_error}")

    @staticmethod
    def _expect_checkpoint_failure(future: Future[dict[str, object]]) -> None:
        try:
            future.result(timeout=30)
        except ClientError:
            return
        raise RuntimeError("Faulted checkpoint unexpectedly succeeded")

    @staticmethod
    def _expect_checkpoint_disconnect(future: Future[dict[str, object]]) -> None:
        try:
            future.result(timeout=30)
        except (ClientError, TimeoutError):
            return
        raise RuntimeError("Killed active JobManager checkpoint unexpectedly returned success")

    def _verify(
        self,
        job_id: str,
        *,
        min_attempt: int,
        min_epoch: int,
    ) -> dict[str, object]:
        return self._tool_json(
            "verify_advanced.py",
            "--job-id",
            job_id,
            "--min-attempt",
            str(min_attempt),
            "--min-epoch",
            str(min_epoch),
        )

    def _submit(self) -> str:
        return self._job_id(self._tool("submit_advanced.py"))

    def _inject(
        self,
        failure: str,
        *,
        job_id: str,
        evidence_name: str,
    ) -> dict[str, object]:
        arguments = [
            sys.executable,
            str(ROOT / "scripts" / "inject_advanced_failure.py"),
            failure,
            "--profile",
            self.profile,
            "--compose-file",
            str(COMPOSE_FILE),
            "--job-id",
            job_id,
            "--jobmanager-url",
            "https://localhost:8080",
            "--evidence-path",
            str(ROOT / "reports" / evidence_name),
        ]
        return json.loads(self._command(arguments, capture=True))

    def _tool_json(self, script: str, *arguments: str) -> dict[str, object]:
        output = self._tool(script, *arguments)
        document = json.loads(output)
        if not isinstance(document, dict):
            raise RuntimeError(f"Tool {script} did not return a JSON object")
        return document

    def _tool(self, script: str, *arguments: str) -> str:
        return self._tool_python(f"/opt/pystream/scripts/{script}", *arguments)

    def _tool_python(self, *arguments: str) -> str:
        command = [
            *self._compose_prefix(),
            "run",
            "--rm",
            "-T",
            "-e",
            f"PYSTREAM_JOBMANAGER_URL={self.tool_url}",
            "tools",
            "python",
            *arguments,
        ]
        output = self._command(command, capture=True)
        print(output)
        return output

    @staticmethod
    def _job_id(output: str) -> str:
        for line in reversed(output.splitlines()):
            if line.startswith("job_id="):
                job_id = line.partition("=")[2].strip()
                if job_id:
                    return job_id
        raise RuntimeError(f"Tool output did not contain job_id: {output}")

    def _client(self) -> JobManagerClient:
        if self.client is None:
            raise RuntimeError("Acceptance client has not been initialized")
        return self.client

    def _generate_pki(self) -> None:
        self._command(
            [
                sys.executable,
                str(ROOT / "scripts" / "generate_dev_pki.py"),
                "--output",
                str(PKI_ROOT),
                "--force",
            ]
        )

    def _up(self) -> None:
        services = CORE_SERVICES if self.profile == "core" else HA_SERVICES
        self._command(
            [
                *self._compose_prefix(),
                "up",
                "-d",
                "--wait",
                "--wait-timeout",
                "300",
                *services,
            ],
            timeout=600,
        )

    def _down(self) -> None:
        holders = _nonempty_lines(
            self._command(
                [
                    "docker",
                    "ps",
                    "-aq",
                    "--filter",
                    "label=com.docker.compose.project=pystream-advanced",
                    "--filter",
                    "label=com.docker.compose.service=failure-holder",
                ],
                capture=True,
                check=False,
            )
        )
        if holders:
            self._command(["docker", "rm", "-f", *holders], check=False)
        paused = _nonempty_lines(
            self._command(
                [
                    "docker",
                    "ps",
                    "-q",
                    "--filter",
                    "label=com.docker.compose.project=pystream-advanced",
                    "--filter",
                    "status=paused",
                ],
                capture=True,
                check=False,
            )
        )
        if paused:
            self._command(["docker", "unpause", *paused], check=False)
        self._command(
            [
                *self._compose_prefix(),
                "down",
                "--volumes",
                "--remove-orphans",
                "--timeout",
                "10",
            ],
            timeout=180,
            check=False,
        )

    def _save_runtime_logs(self) -> None:
        output = self._compose_output("logs", "--no-color", "--timestamps")
        path = self.args.runtime_log_path.resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(output + "\n", encoding="utf-8")

    def _resource_counts(self) -> dict[str, int]:
        label = "label=com.docker.compose.project=pystream-advanced"
        return {
            "containers": len(
                _nonempty_lines(
                    self._command(
                        ["docker", "ps", "-aq", "--filter", label],
                        capture=True,
                    )
                )
            ),
            "networks": len(
                _nonempty_lines(
                    self._command(
                        ["docker", "network", "ls", "-q", "--filter", label],
                        capture=True,
                    )
                )
            ),
            "volumes": len(
                _nonempty_lines(
                    self._command(
                        ["docker", "volume", "ls", "-q", "--filter", label],
                        capture=True,
                    )
                )
            ),
        }

    def _compose_output(self, *arguments: str) -> str:
        return self._command(
            self._compose_prefix() + list(arguments),
            capture=True,
        )

    def _compose_prefix(self) -> list[str]:
        return [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "--profile",
            self.profile,
        ]

    def _command(
        self,
        command: Sequence[str],
        *,
        capture: bool = False,
        check: bool = True,
        timeout: float = 300,
    ) -> str:
        print("+ " + " ".join(command))
        result = subprocess.run(
            command,
            cwd=ROOT,
            env=self.env,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout,
        )
        if capture:
            if result.stderr:
                print(result.stderr, end="")
            output = result.stdout.strip()
        else:
            if result.stdout:
                print(result.stdout, end="")
            if result.stderr:
                print(result.stderr, end="")
            output = ""
        if check and result.returncode != 0:
            raise subprocess.CalledProcessError(
                result.returncode,
                command,
                output=result.stdout,
                stderr=result.stderr,
            )
        return output


def _integer(document: dict[str, object], field: str) -> int:
    value = document.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"{field} must be an integer: {document}")
    return value


def _nonempty_lines(value: str) -> list[str]:
    return [line for line in value.splitlines() if line.strip()]


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    acceptance = Acceptance(args)
    failure: BaseException | None = None
    try:
        acceptance.run()
    except BaseException as exc:
        failure = exc
    try:
        acceptance.finish(failure)
    except BaseException as cleanup_exc:
        if failure is None:
            failure = cleanup_exc
    if failure is not None:
        raise failure
    print(f"advanced_{args.profile}_acceptance=passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
