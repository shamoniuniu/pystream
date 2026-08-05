"""Prometheus metric names, labels and alert threshold contracts."""

from __future__ import annotations

from pathlib import Path

import yaml

from pystream.observability import PyStreamMetrics

ROOT = Path(__file__).resolve().parents[2]


def test_prometheus_registry_exports_advanced_metric_contract() -> None:
    metrics = PyStreamMetrics()
    metrics.lease_renew_failures.inc()
    metrics.checkpoint_decisions.inc()
    metrics.checkpoint_finalize_retries.inc()
    metrics.record_object_store("get", "ok")
    metrics.record_tls_failure("worker_http")
    metrics.record_auth_rejection("external_api", "invalid_token")
    metrics.record_recovery("job", 12.5)
    rendered = metrics.render().decode("utf-8")

    expected = {
        "pystream_leader_role",
        "pystream_leader_epoch",
        "pystream_lease_renew_failures_total",
        "pystream_lease_remaining_seconds",
        "pystream_checkpoint_phase",
        "pystream_checkpoint_duration_seconds",
        "pystream_barrier_alignment_seconds",
        "pystream_barrier_inputs_blocked",
        "pystream_checkpoint_decisions_total",
        "pystream_checkpoint_finalize_retries_total",
        "pystream_sink_transactions",
        "pystream_job_recovery_duration_seconds",
        "pystream_object_store_requests_total",
        "pystream_tls_handshake_failures_total",
        "pystream_auth_rejections_total",
        "pystream_tls_certificate_expiry_seconds",
    }
    assert all(name in rendered for name in expected)
    assert metrics.leader_role._labelnames == ("role",)
    assert metrics.sink_transactions._labelnames == ("state",)
    assert metrics.object_store_requests._labelnames == ("operation", "status")
    assert "job_id" not in rendered
    assert "task_id" not in rendered


def test_prometheus_rules_cover_security_and_recovery_slos() -> None:
    document = yaml.safe_load(
        (ROOT / "deploy" / "prometheus" / "rules.yml").read_text(encoding="utf-8")
    )
    rules = {rule["alert"]: rule for group in document["groups"] for rule in group["rules"]}

    assert rules["PyStreamActiveJobManagerCount"]["for"] == "15s"
    assert "!= 1" in rules["PyStreamActiveJobManagerCount"]["expr"]
    assert "< 6" in rules["PyStreamLeaderLeaseLow"]["expr"]
    assert "> 30" in rules["PyStreamCheckpointFinalizeStalled"]["expr"]
    assert 'component="job"' in rules["PyStreamJobManagerRecoverySLOBreach"]["expr"]
    assert "> 30" in rules["PyStreamJobManagerRecoverySLOBreach"]["expr"]
    assert 'component="worker"' in rules["PyStreamWorkerRecoverySLOBreach"]["expr"]
    assert "> 60" in rules["PyStreamWorkerRecoverySLOBreach"]["expr"]
    assert "604800" in rules["PyStreamCertificateExpiring"]["expr"]
