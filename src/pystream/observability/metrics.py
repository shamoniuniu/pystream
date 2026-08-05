"""控制面与 Worker 服务使用的低基数 Prometheus 指标。"""

from __future__ import annotations

import time
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

from pystream.security import certificate_expiry_timestamp

if TYPE_CHECKING:
    from pystream.control import JobManager, LeaderCoordinator
    from pystream.runtime import RuntimeSnapshot

_LEADER_ROLES = ("ACTIVE", "STANDBY", "PROTECTIVE")
_CHECKPOINT_PHASES = (
    "IDLE",
    "ARMED",
    "ALIGNING",
    "PREPARED",
    "DECIDED",
    "FINALIZING",
    "FINALIZED",
    "ABORTED",
    "RECOVERING",
)
_TRANSACTION_STATES = ("active", "prepared", "committed", "aborted")


class PyStreamMetrics:
    """Own a per-process registry so tests and embedded apps stay isolated."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.leader_role = Gauge(
            "pystream_leader_role",
            "Current JobManager role as a one-hot gauge.",
            ("role",),
            registry=self.registry,
        )
        self.leader_epoch = Gauge(
            "pystream_leader_epoch",
            "Current coordinator fencing epoch.",
            registry=self.registry,
        )
        self.lease_remaining = Gauge(
            "pystream_lease_remaining_seconds",
            "Seconds until the current leader lease expires.",
            registry=self.registry,
        )
        self.lease_renew_failures = Counter(
            "pystream_lease_renew_failures_total",
            "Leader lease renew failures.",
            registry=self.registry,
        )
        self.checkpoint_phase = Gauge(
            "pystream_checkpoint_phase",
            "Current checkpoint phase as a one-hot gauge.",
            ("phase",),
            registry=self.registry,
        )
        self.checkpoint_duration = Histogram(
            "pystream_checkpoint_duration_seconds",
            "End-to-end checkpoint coordination duration.",
            buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
            registry=self.registry,
        )
        self.barrier_alignment = Histogram(
            "pystream_barrier_alignment_seconds",
            "Latest observed task barrier alignment duration.",
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5),
            registry=self.registry,
        )
        self.barrier_inputs_blocked = Gauge(
            "pystream_barrier_inputs_blocked",
            "Maximum currently observed blocked barrier inputs.",
            registry=self.registry,
        )
        self.checkpoint_decisions = Counter(
            "pystream_checkpoint_decisions_total",
            "Durable checkpoint decisions.",
            registry=self.registry,
        )
        self.checkpoint_finalize_retries = Counter(
            "pystream_checkpoint_finalize_retries_total",
            "Checkpoint finalize retries.",
            registry=self.registry,
        )
        self.checkpoint_decided_age = Gauge(
            "pystream_checkpoint_decided_age_seconds",
            "Age of the oldest DECIDED but not FINALIZED checkpoint.",
            registry=self.registry,
        )
        self.checkpoint_consecutive_failures = Gauge(
            "pystream_checkpoint_consecutive_failures",
            "Maximum consecutive checkpoint failures across local jobs.",
            registry=self.registry,
        )
        self.sink_transactions = Gauge(
            "pystream_sink_transactions",
            "Current transaction counts by bounded state.",
            ("state",),
            registry=self.registry,
        )
        self.job_recovery_duration = Histogram(
            "pystream_job_recovery_duration_seconds",
            "Completed recovery duration.",
            ("component",),
            buckets=(0.1, 0.5, 1, 2.5, 5, 10, 20, 30, 60, 120),
            registry=self.registry,
        )
        self.job_recovery_last_duration = Gauge(
            "pystream_job_recovery_last_duration_seconds",
            "Most recently completed recovery duration.",
            ("component",),
            registry=self.registry,
        )
        self.object_store_requests = Counter(
            "pystream_object_store_requests_total",
            "Object store operations by bounded operation and status.",
            ("operation", "status"),
            registry=self.registry,
        )
        self.object_store_available = Gauge(
            "pystream_object_store_available",
            "Whether the last object store operation succeeded.",
            registry=self.registry,
        )
        self.tls_handshake_failures = Counter(
            "pystream_tls_handshake_failures_total",
            "Rejected TLS handshakes or peer certificate identities.",
            ("surface",),
            registry=self.registry,
        )
        self.auth_rejections = Counter(
            "pystream_auth_rejections_total",
            "Rejected authentication attempts.",
            ("surface", "reason"),
            registry=self.registry,
        )
        self.tls_certificate_expiry = Gauge(
            "pystream_tls_certificate_expiry_seconds",
            "Certificate not-valid-after time as a Unix timestamp.",
            ("identity",),
            registry=self.registry,
        )
        self._checkpoint_phase = "IDLE"
        self._last_alignment_ms: dict[str, int] = {}
        for role in _LEADER_ROLES:
            self.leader_role.labels(role=role).set(0)
        for phase in _CHECKPOINT_PHASES:
            self.checkpoint_phase.labels(phase=phase).set(phase == "IDLE")
        for state in _TRANSACTION_STATES:
            self.sink_transactions.labels(state=state).set(0)
        self.object_store_available.set(1)

    def render(self) -> bytes:
        return generate_latest(self.registry)

    def set_checkpoint_phase(self, phase: str) -> None:
        if phase not in _CHECKPOINT_PHASES:
            raise ValueError(f"未知 Checkpoint phase: {phase}")
        self._checkpoint_phase = phase
        for candidate in _CHECKPOINT_PHASES:
            self.checkpoint_phase.labels(phase=candidate).set(candidate == phase)

    def update_jobmanager(
        self,
        manager: JobManager,
        leadership: LeaderCoordinator | None = None,
    ) -> None:
        for role in _LEADER_ROLES:
            self.leader_role.labels(role=role).set(manager.role.value == role)
        self.leader_epoch.set(manager.coordinator_epoch)
        current = leadership.current if leadership is not None else None
        remaining = (
            max(0.0, current.lease.expires_at.timestamp() - time.time())
            if current is not None
            else 0.0
        )
        self.lease_remaining.set(remaining)
        runs = manager.runs
        self.checkpoint_consecutive_failures.set(
            max((run.consecutive_checkpoint_failures for run in runs), default=0)
        )
        decided_ages = [
            max(0.0, time.time() - run.job.updated_at.timestamp())
            for run in runs
            if run.last_decided_checkpoint_id is not None
            and run.last_decided_checkpoint_id != run.last_finalized_checkpoint_id
        ]
        self.checkpoint_decided_age.set(max(decided_ages, default=0.0))

    def update_worker(self, snapshots: Iterable[RuntimeSnapshot]) -> None:
        totals = dict.fromkeys(_TRANSACTION_STATES, 0)
        blocked = 0
        for snapshot in snapshots:
            blocked = max(blocked, snapshot.barrier_blocked_inputs)
            previous = self._last_alignment_ms.get(snapshot.task_id)
            if (
                snapshot.barrier_alignment_duration_ms > 0
                and snapshot.barrier_alignment_duration_ms != previous
            ):
                self.barrier_alignment.observe(snapshot.barrier_alignment_duration_ms / 1_000)
                self._last_alignment_ms[snapshot.task_id] = snapshot.barrier_alignment_duration_ms
            for state in _TRANSACTION_STATES:
                totals[state] += snapshot.operator_metrics.get(f"transactions_{state}", 0)
        self.barrier_inputs_blocked.set(blocked)
        for state, value in totals.items():
            self.sink_transactions.labels(state=state).set(value)

    def record_object_store(self, operation: str, status: str) -> None:
        self.object_store_requests.labels(operation=operation, status=status).inc()
        self.object_store_available.set(status != "error")

    def record_auth_rejection(self, surface: str, reason: str) -> None:
        self.auth_rejections.labels(surface=surface, reason=reason).inc()

    def record_tls_failure(self, surface: str) -> None:
        self.tls_handshake_failures.labels(surface=surface).inc()

    def record_recovery(self, component: str, duration: float) -> None:
        value = max(0.0, duration)
        self.job_recovery_duration.labels(component=component).observe(value)
        self.job_recovery_last_duration.labels(component=component).set(value)

    def observe_certificate(self, identity: str, cert_file: str | Path) -> None:
        self.tls_certificate_expiry.labels(identity=identity).set(
            certificate_expiry_timestamp(cert_file)
        )


__all__ = ["PyStreamMetrics"]
