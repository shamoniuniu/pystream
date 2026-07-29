"""Worker aiohttp 服务、注册心跳和 Gateway 测试。"""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest
from aiohttp.test_utils import TestClient, TestServer

from pystream.checkpoint import TaskSnapshotDescriptor
from pystream.control import TaskDeployment, WorkerNode
from pystream.runtime import DataPlaneServer, RuntimeSnapshot, TaskRuntimeState
from pystream.worker import (
    HttpWorkerGateway,
    WorkerHttpService,
    WorkerServiceConfig,
)
from tests.unit.worker.test_worker_models import sample_deployment


class StubRegistration:
    def __init__(self) -> None:
        self.registrations: list[dict[str, object]] = []
        self.heartbeats: list[str] = []
        self.failures: list[tuple[str, str, int, str]] = []

    async def register(self, **values) -> None:
        self.registrations.append(values)

    async def heartbeat(self, worker_id: str) -> None:
        self.heartbeats.append(worker_id)

    async def report_task_failed(
        self,
        job_id: str,
        task_id: str,
        attempt_id: int,
        error: str,
    ) -> None:
        self.failures.append((job_id, task_id, attempt_id, error))


class StubManager:
    def __init__(self) -> None:
        self.deployed: list[TaskDeployment] = []
        self.stopped: list[str] = []
        self.checkpoint_actions: list[tuple[str, str, int]] = []
        self.closed = False
        self._snapshots: dict[str, RuntimeSnapshot] = {}

    @property
    def task_count(self) -> int:
        return len(self._snapshots)

    async def deploy(self, deployment: TaskDeployment) -> RuntimeSnapshot:
        self.deployed.append(deployment)
        task = deployment.task
        snapshot = RuntimeSnapshot(
            task_id=task.task_id,
            job_id=task.job_id,
            operator_id=task.operator_id,
            subtask_index=task.subtask_index,
            state=TaskRuntimeState.RUNNING,
            error=None,
            input_channels=len(deployment.incoming_channels),
            closed_input_channels=0,
            output_channels=len(deployment.outgoing_channels),
            records_in=0,
            records_out=0,
        )
        self._snapshots[task.task_id] = snapshot
        return snapshot

    async def stop(
        self,
        task_id: str,
        attempt_id: int | None = None,
    ) -> RuntimeSnapshot | None:
        del attempt_id
        self.stopped.append(task_id)
        snapshot = self._snapshots.get(task_id)
        if snapshot is None:
            return None
        stopped = replace(snapshot, state=TaskRuntimeState.STOPPED)
        self._snapshots[task_id] = stopped
        return stopped

    async def arm_checkpoint(
        self,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
    ) -> None:
        del attempt_id
        self.checkpoint_actions.append(("arm", task_id, checkpoint_id))

    async def trigger_checkpoint(
        self,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
    ) -> TaskSnapshotDescriptor:
        del attempt_id
        self.checkpoint_actions.append(("trigger", task_id, checkpoint_id))
        return self._descriptor(task_id, checkpoint_id)

    async def wait_checkpoint(
        self,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
    ) -> TaskSnapshotDescriptor:
        del attempt_id
        self.checkpoint_actions.append(("wait", task_id, checkpoint_id))
        return self._descriptor(task_id, checkpoint_id)

    async def complete_checkpoint(
        self,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
    ) -> None:
        del attempt_id
        self.checkpoint_actions.append(("complete", task_id, checkpoint_id))

    async def abort_checkpoint(
        self,
        task_id: str,
        attempt_id: int,
        checkpoint_id: int,
    ) -> None:
        del attempt_id
        self.checkpoint_actions.append(("abort", task_id, checkpoint_id))

    @staticmethod
    def _descriptor(task_id: str, checkpoint_id: int) -> TaskSnapshotDescriptor:
        return TaskSnapshotDescriptor(
            job_id="job-1",
            checkpoint_id=checkpoint_id,
            attempt_id=0,
            task_id=task_id,
            operator_id="words",
            relative_path=f"job-1/checkpoint-{checkpoint_id}/task.json",
            sha256="0" * 64,
            size=100,
        )

    def get(self, task_id: str) -> RuntimeSnapshot:
        from pystream.worker import WorkerTaskError

        try:
            return self._snapshots[task_id]
        except KeyError as exc:
            raise WorkerTaskError("unknown") from exc

    def snapshots(self) -> tuple[RuntimeSnapshot, ...]:
        return tuple(self._snapshots.values())

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_worker_http_注册心跳_部署查询停止() -> None:
    manager = StubManager()
    registration = StubRegistration()
    data_server = DataPlaneServer("127.0.0.1", 0)
    service = WorkerHttpService(
        WorkerServiceConfig(
            worker_id="worker-1",
            control_address="http://worker-1:8081",
            data_host="worker-1",
            total_slots=4,
            heartbeat_interval=0.01,
        ),
        manager,  # type: ignore[arg-type]
        data_server,
        registration,
    )
    client = TestClient(TestServer(service.create_app()))
    await client.start_server()
    try:
        assert registration.registrations[0]["data_port"] == data_server.bound_port
        health = await (await client.get("/health")).json()
        assert health["status"] == "ok"
        assert health["worker_id"] == "worker-1"
        assert health["data_plane"]["registered_channels"] == 0
        assert health["data_plane"]["active_connections"] == 0
        assert health["runtime"] == {
            "records_in": 0,
            "records_out": 0,
            "input_queue_depth": 0,
            "output_queue_depth": 0,
            "errors": 0,
        }
        await asyncio.sleep(0.03)
        assert registration.heartbeats

        bad = await client.post("/tasks/deploy", json={"bad": True})
        assert bad.status == 400

        from pystream.worker import deployment_to_dict

        response = await client.post(
            "/tasks/deploy",
            json=deployment_to_dict(sample_deployment()),
        )
        assert response.status == 201
        task_id = sample_deployment().task.task_id
        assert (await response.json())["state"] == "RUNNING"
        assert (await (await client.get("/tasks")).json())["tasks"][0]["task_id"] == task_id
        assert (await client.get(f"/tasks/{task_id}")).status == 200
        assert (await client.get("/tasks/missing")).status == 404

        arm = await client.post(f"/tasks/{task_id}/checkpoints/1/arm?attempt_id=0")
        assert (await arm.json())["status"] == "armed"
        trigger = await client.post(f"/tasks/{task_id}/checkpoints/1/trigger?attempt_id=0")
        assert (await trigger.json())["checkpoint_id"] == 1
        wait = await client.get(f"/tasks/{task_id}/checkpoints/1?attempt_id=0")
        assert (await wait.json())["task_id"] == task_id
        assert (
            await client.post(f"/tasks/{task_id}/checkpoints/1/complete?attempt_id=0")
        ).status == 204
        assert (
            await client.post(f"/tasks/{task_id}/checkpoints/2/abort?attempt_id=0")
        ).status == 204
        assert (
            await client.post(f"/tasks/{task_id}/checkpoints/not-int/arm?attempt_id=0")
        ).status == 400

        stopped = await client.delete(f"/tasks/{task_id}?attempt_id=0")
        assert (await stopped.json())["state"] == "STOPPED"
        assert (await client.delete("/tasks/missing?attempt_id=0")).status == 204
    finally:
        await client.close()
    assert manager.closed
    assert not data_server.running


@pytest.mark.asyncio
async def test_http_worker_gateway_调用真实worker路由() -> None:
    manager = StubManager()
    registration = StubRegistration()
    data_server = DataPlaneServer("127.0.0.1", 0)
    service = WorkerHttpService(
        WorkerServiceConfig(
            worker_id="worker-1",
            control_address="http://placeholder",
            data_host="127.0.0.1",
        ),
        manager,  # type: ignore[arg-type]
        data_server,
        registration,
    )
    server = TestServer(service.create_app())
    await server.start_server()
    gateway = HttpWorkerGateway()
    worker = WorkerNode.create(
        "worker-1",
        str(server.make_url("")).rstrip("/"),
        "127.0.0.1",
        data_server.bound_port,
        4,
    )
    deployment = sample_deployment()
    try:
        await gateway.deploy_task(worker, deployment)
        await gateway.arm_checkpoint(worker, deployment.task.task_id, 0, 3)
        triggered = await gateway.trigger_checkpoint(
            worker,
            deployment.task.task_id,
            0,
            3,
        )
        waited = await gateway.wait_checkpoint(
            worker,
            deployment.task.task_id,
            0,
            3,
        )
        await gateway.complete_checkpoint(worker, deployment.task.task_id, 0, 3)
        await gateway.abort_checkpoint(worker, deployment.task.task_id, 0, 4)
        await gateway.stop_task(worker, deployment.task.task_id, 0)
    finally:
        await gateway.close()
        await server.close()

    assert manager.deployed == [deployment]
    assert triggered == waited
    assert manager.checkpoint_actions == [
        ("arm", deployment.task.task_id, 3),
        ("trigger", deployment.task.task_id, 3),
        ("wait", deployment.task.task_id, 3),
        ("complete", deployment.task.task_id, 3),
        ("abort", deployment.task.task_id, 4),
    ]
    assert manager.stopped == [deployment.task.task_id]
