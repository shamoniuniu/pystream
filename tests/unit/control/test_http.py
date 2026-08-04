"""JobManager HTTP 路由与真实作业制品提交测试。"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer

from pystream.artifact import build_job_bundle
from pystream.checkpoint import CheckpointManifest
from pystream.control import (
    CoordinatorRole,
    JobManager,
    JobManagerHttpService,
    LocalArtifactRepository,
    TaskDeployment,
    WorkerNode,
)


class RecordingGateway:
    """不启动真实 Worker，只记录 HTTP 服务触发的部署和停止。"""

    def __init__(self) -> None:
        self.deployments: list[tuple[str, TaskDeployment]] = []
        self.stops: list[tuple[str, str]] = []

    async def deploy_task(self, worker: WorkerNode, deployment: TaskDeployment) -> None:
        self.deployments.append((worker.worker_id, deployment))

    async def stop_task(
        self,
        worker: WorkerNode,
        task_id: str,
        attempt_id: int,
        coordinator_epoch: int = 0,
    ) -> None:
        del attempt_id, coordinator_epoch
        self.stops.append((worker.worker_id, task_id))


class RecordingLeadership:
    def __init__(self) -> None:
        self.started = False
        self.closed = False

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_http_区分active与ready并将standby写入映射为503(tmp_path: Path) -> None:
    manager = JobManager(
        LocalArtifactRepository(tmp_path / "store"),
        RecordingGateway(),
        role=CoordinatorRole.STANDBY,
    )
    leadership = RecordingLeadership()
    client = TestClient(
        TestServer(
            JobManagerHttpService(
                manager,
                leadership=leadership,
            ).create_app()
        )
    )
    await client.start_server()
    try:
        assert leadership.started
        assert (await client.get("/health")).status == 200
        active = await client.get("/health/active")
        leader = await client.get("/health/leader")
        assert active.status == 503
        assert active.headers["Retry-After"] == "1"
        assert leader.status == 503

        rejected = await client.post(
            "/workers/register",
            json={
                "worker_id": "worker-1",
                "incarnation_id": "process-1",
                "control_address": "http://worker-1:8081",
                "data_host": "worker-1",
                "data_port": 9000,
                "total_slots": 1,
            },
        )
        assert rejected.status == 503
        assert rejected.headers["Retry-After"] == "1"

        await manager.activate(1)
        assert (await client.get("/health/active")).status == 200
        assert (await client.get("/health/leader")).status == 200
    finally:
        await client.close()
    assert leadership.closed


@pytest.mark.asyncio
async def test_http_手动触发checkpoint返回manifest(tmp_path: Path, monkeypatch) -> None:
    manager = JobManager(LocalArtifactRepository(tmp_path / "store"), RecordingGateway())

    async def trigger_checkpoint(job_id: str) -> CheckpointManifest:
        assert job_id == "job-1"
        return CheckpointManifest(
            job_id=job_id,
            checkpoint_id=3,
            attempt_id=1,
            created_at=datetime.now(UTC),
            snapshots=(),
        )

    monkeypatch.setattr(manager, "trigger_checkpoint", trigger_checkpoint)
    client = TestClient(TestServer(JobManagerHttpService(manager).create_app()))
    await client.start_server()
    try:
        response = await client.post("/v1/jobs/job-1/checkpoint")
        document = await response.json()

        assert response.status == 200
        assert document["job_id"] == "job-1"
        assert document["checkpoint_id"] == 3
        assert document["attempt_id"] == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_http_注册三worker_提交查询下载和取消(tmp_path: Path) -> None:
    gateway = RecordingGateway()
    manager = JobManager(
        LocalArtifactRepository(tmp_path / "store"),
        gateway,
        coordinator_epoch=4,
    )
    service = JobManagerHttpService(manager, reconcile_interval=60)
    client = TestClient(TestServer(service.create_app()))
    await client.start_server()
    try:
        for index in range(1, 4):
            response = await client.post(
                "/workers/register",
                json={
                    "worker_id": f"worker-{index}",
                    "incarnation_id": f"worker-{index}-process-1",
                    "control_address": f"http://worker-{index}:8081",
                    "data_host": f"worker-{index}",
                    "data_port": 9000,
                    "total_slots": 4,
                },
            )
            assert response.status == 201
            registration = await response.json()
            assert registration["restarted"] is False
            assert registration["coordinator_epoch"] == 4

        heartbeat = await client.post("/workers/worker-1/heartbeat", json={})
        assert heartbeat.status == 200
        assert (await heartbeat.json())["coordinator_epoch"] == 4

        health = await (await client.get("/health")).json()
        workers = await (await client.get("/v1/workers")).json()
        assert health["healthy_workers"] == 3
        assert len(workers["workers"]) == 3
        assert workers["workers"][0]["incarnation_id"] == "worker-1-process-1"

        bundle = build_job_bundle(
            Path("examples") / "wordcount",
            tmp_path / "bundles",
        )
        content = Path(bundle.path).read_bytes()
        submitted = await client.post(
            "/v1/jobs",
            data=content,
            headers={
                "Content-Type": "application/zip",
                "X-PyStream-SHA256": bundle.sha256,
                "X-PyStream-Filename": Path(bundle.path).name,
            },
        )
        assert submitted.status == 201
        submission = await submitted.json()
        assert submission["status"] == "RUNNING"
        job_id = submission["job_id"]

        status = await (await client.get(f"/v1/jobs/{job_id}")).json()
        assert status["status"] == "RUNNING"
        assert len(status["tasks"]) == 10
        assert len({item["worker_id"] for item in status["tasks"]}) >= 2
        assert len(gateway.deployments) == 10

        task = status["tasks"][0]
        missing_epoch = await client.post(
            f"/jobs/{job_id}/tasks/{task['task_id']}/status",
            json={
                "status": "FAILED",
                "attempt_id": task["attempt_id"],
                "error": "missing epoch",
            },
        )
        assert missing_epoch.status == 400

        stale_report = await client.post(
            f"/jobs/{job_id}/tasks/{task['task_id']}/status",
            json={
                "status": "FAILED",
                "attempt_id": task["attempt_id"],
                "coordinator_epoch": 3,
                "error": "stale leader",
            },
        )
        assert stale_report.status == 200
        assert (await stale_report.json())["status"] == "RUNNING"

        artifact = await client.get(f"/jobs/{job_id}/artifacts/{bundle.sha256}")
        assert artifact.status == 200
        assert await artifact.read() == content

        cancelled = await client.post(f"/v1/jobs/{job_id}/cancel")
        cancel_view = await cancelled.json()
        assert cancelled.status == 200
        assert cancel_view["status"] == "CANCELLED"
        assert cancel_view["released_slots"] == 10
        assert len(gateway.stops) == 10
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_http_拒绝非法制品和未知worker(tmp_path: Path) -> None:
    manager = JobManager(LocalArtifactRepository(tmp_path / "store"), RecordingGateway())
    client = TestClient(TestServer(JobManagerHttpService(manager).create_app()))
    await client.start_server()
    try:
        invalid_type = await client.post("/v1/jobs", data=b"zip")
        assert invalid_type.status == 415

        invalid_digest = await client.post(
            "/v1/jobs",
            data=b"zip",
            headers={
                "Content-Type": "application/zip",
                "X-PyStream-SHA256": "bad",
            },
        )
        assert invalid_digest.status == 400

        heartbeat = await client.post("/workers/missing/heartbeat", json={})
        assert heartbeat.status == 404
        assert "尚未注册" in (await heartbeat.json())["error"]
    finally:
        await client.close()
