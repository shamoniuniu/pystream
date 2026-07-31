"""WorkerTaskManager 的制品复用、部署和失败上报测试。"""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import pytest
import yaml

from pystream.api import API_VERSION, OperatorType
from pystream.artifact import build_job_bundle
from pystream.control import ArtifactDescriptor, TaskDeployment, TaskInstance, TaskStatus
from pystream.runtime import DataPlaneServer, TaskRuntimeState
from pystream.worker import WorkerTaskError, WorkerTaskManager


class BlockingConsumer:
    """启动后阻塞，直到 TaskRuntime 取消。"""

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.started = False
        self.stopped = False
        self.paused = False
        self.commits: list[dict[object, object] | None] = []
        self._wait = asyncio.Event()

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True
        self._wait.set()

    async def commit(self, offsets: dict[object, object] | None = None) -> None:
        self.commits.append(offsets)

    def assignment(self) -> set[object]:
        return set()

    def pause(self, *partitions: object) -> None:
        del partitions
        self.paused = True

    def resume(self, *partitions: object) -> None:
        del partitions
        self.paused = False

    def seek(self, partition: object, offset: int) -> None:
        del partition, offset

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.fail:
            raise ConnectionError("broker disconnected")
        await self._wait.wait()
        raise StopAsyncIteration


class FiniteConsumer(BlockingConsumer):
    """返回固定消息后结束，用于验证 Source UDF 接线。"""

    def __init__(self, *values: bytes) -> None:
        super().__init__()
        self.values = values

    def __aiter__(self):
        async def iterate():
            for offset, value in enumerate(self.values):
                yield type(
                    "Message",
                    (),
                    {
                        "topic": "words",
                        "partition": 0,
                        "offset": offset,
                        "value": value,
                    },
                )()

        return iterate()


class MemoryFetcher:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.calls = 0

    async def fetch(self, descriptor: ArtifactDescriptor) -> bytes:
        del descriptor
        self.calls += 1
        return self.content


class RecordingReporter:
    def __init__(self) -> None:
        self.failures: list[tuple[str, str, int, int, str]] = []
        self.reported = asyncio.Event()

    async def report_task_failed(
        self,
        job_id: str,
        task_id: str,
        attempt_id: int,
        error: str,
        *,
        coordinator_epoch: int = 0,
    ) -> None:
        self.failures.append((job_id, task_id, attempt_id, coordinator_epoch, error))
        self.reported.set()


def build_source_bundle(
    tmp_path: Path,
    *,
    validator: str | None = None,
    bad_record_policy: str = "fail",
    checkpoint: bool = False,
) -> tuple[bytes, ArtifactDescriptor]:
    source = tmp_path / "source"
    source.mkdir()
    source_config = {
        "connector": "kafka",
        "topic": "words",
        "bootstrap_servers": "unused:9092",
        "bad_record_policy": bad_record_policy,
    }
    if validator is not None:
        source_config["validator"] = validator
    document = {
        "api_version": API_VERSION,
        "job": {"name": "worker-test"},
        "operators": [
            {
                "id": "words",
                "type": "source",
                "parallelism": 1,
                "config": source_config,
            },
            {
                "id": "output",
                "type": "sink",
                "parallelism": 1,
                "config": {
                    "connector": "file",
                    "format": "csv",
                    "output_path": str(tmp_path / "output"),
                },
            },
        ],
        "edges": [{"from": "words", "to": "output"}],
    }
    if checkpoint:
        document["execution"] = {
            "checkpoint": {
                "interval": "10s",
                "timeout": "30s",
                "max_consecutive_failures": 3,
            },
            "restart": {"max_attempts": 3, "delay": "2s"},
        }
    (source / "job.yaml").write_text(
        yaml.safe_dump(document, sort_keys=False),
        encoding="utf-8",
    )
    (source / "wordcount_udfs.py").write_text(
        "def validate_input(value):\n"
        "    if not isinstance(value, dict) or not isinstance(value.get('word'), str):\n"
        "        raise ValueError('word required')\n"
        "    if isinstance(value.get('count'), bool) or not isinstance(value.get('count'), int):\n"
        "        raise ValueError('integer count required')\n",
        encoding="utf-8",
    )
    bundle = build_job_bundle(source, tmp_path / "bundles")
    content = Path(bundle.path).read_bytes()
    return content, ArtifactDescriptor("job-1", bundle.sha256, len(content))


def source_deployment(
    descriptor: ArtifactDescriptor,
    *,
    worker_id: str = "worker-1",
    attempt_id: int = 0,
    coordinator_epoch: int = 0,
) -> TaskDeployment:
    task = TaskInstance(
        task_id="job-1:words:0",
        job_id="job-1",
        operator_id="words",
        operator_type=OperatorType.SOURCE,
        subtask_index=0,
        parallelism=1,
        attempt_id=attempt_id,
        status=TaskStatus.DEPLOYING,
        worker_id=worker_id,
        slot_index=0,
    )
    return TaskDeployment(
        task,
        descriptor,
        (),
        (),
        coordinator_epoch=coordinator_epoch,
    )


@pytest.mark.asyncio
async def test_manager_下载解压一次_启动停止并隔离工作目录(tmp_path: Path) -> None:
    content, descriptor = build_source_bundle(tmp_path)
    fetcher = MemoryFetcher(content)
    consumer = BlockingConsumer()
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    manager = WorkerTaskManager(
        "worker-1",
        tmp_path / "work",
        server,
        fetcher,
        consumer_factory=lambda *args, **kwargs: consumer,
    )
    deployment = source_deployment(descriptor)

    first = await manager.deploy(deployment)
    duplicate = await manager.deploy(deployment)

    assert first.state is duplicate.state is TaskRuntimeState.RUNNING
    assert fetcher.calls == 1
    assert consumer.started
    assert (
        tmp_path / "work" / "jobs" / descriptor.job_id / descriptor.sha256 / "job.yaml"
    ).is_file()
    stopped = await manager.stop(deployment.task.task_id)
    assert stopped is not None and stopped.state is TaskRuntimeState.STOPPED
    assert consumer.stopped
    assert await manager.stop("unknown") is None

    await manager.close()
    await server.close()


@pytest.mark.asyncio
async def test_manager_转发checkpoint生命周期并复用共享store(tmp_path: Path) -> None:
    content, descriptor = build_source_bundle(tmp_path, checkpoint=True)
    consumer = BlockingConsumer()
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    manager = WorkerTaskManager(
        "worker-1",
        tmp_path / "work",
        server,
        MemoryFetcher(content),
        consumer_factory=lambda *args, **kwargs: consumer,
        checkpoint_root=tmp_path / "shared-checkpoints",
    )
    deployment = source_deployment(descriptor)
    task_id = deployment.task.task_id
    try:
        await manager.deploy(deployment)
        await manager.arm_checkpoint(task_id, 0, 1)
        descriptor = await manager.trigger_checkpoint(task_id, 0, 1)
        assert await manager.wait_checkpoint(task_id, 0, 1) == descriptor
        assert consumer.commits == []

        manager.checkpoint_store.complete_checkpoint(
            job_id="job-1",
            checkpoint_id=1,
            attempt_id=0,
            expected_task_ids={task_id},
            snapshots=(descriptor,),
        )
        await manager.complete_checkpoint(task_id, 0, 1)
        assert consumer.commits == [{}]

        await manager.arm_checkpoint(task_id, 0, 2)
        await manager.abort_checkpoint(task_id, 0, 2)
        assert consumer.commits == [{}]
    finally:
        await manager.close()
        await server.close()


@pytest.mark.asyncio
async def test_manager_高attempt替换旧runtime并拒绝低attempt(tmp_path: Path) -> None:
    content, descriptor = build_source_bundle(tmp_path)
    first_consumer = BlockingConsumer()
    second_consumer = BlockingConsumer()
    consumers = [first_consumer, second_consumer]
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    manager = WorkerTaskManager(
        "worker-1",
        tmp_path / "work",
        server,
        MemoryFetcher(content),
        consumer_factory=lambda *args, **kwargs: consumers.pop(0),
    )
    first = source_deployment(descriptor, attempt_id=0)
    second = source_deployment(descriptor, attempt_id=1)
    try:
        await manager.deploy(first)
        replaced = await manager.deploy(second)
        duplicate = await manager.deploy(second)

        assert first_consumer.stopped
        assert second_consumer.started
        assert replaced == duplicate
        assert manager._runtimes[first.task.task_id].deployment.task.attempt_id == 1
        stale_stop = await manager.stop(first.task.task_id, attempt_id=0)
        assert stale_stop is not None and stale_stop.state is TaskRuntimeState.RUNNING
        with pytest.raises(WorkerTaskError, match="attempt 不匹配"):
            await manager.arm_checkpoint(first.task.task_id, 0, 1)
        with pytest.raises(WorkerTaskError, match="旧 attempt"):
            await manager.deploy(source_deployment(descriptor, attempt_id=0))
    finally:
        await manager.close()
        await server.close()


@pytest.mark.asyncio
async def test_manager_见到高epoch后跨task拒绝旧leader控制(tmp_path: Path) -> None:
    content, descriptor = build_source_bundle(tmp_path)
    first_consumer = BlockingConsumer()
    second_consumer = BlockingConsumer()
    consumers = [first_consumer, second_consumer]
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    manager = WorkerTaskManager(
        "worker-1",
        tmp_path / "work",
        server,
        MemoryFetcher(content),
        consumer_factory=lambda *args, **kwargs: consumers.pop(0),
    )
    first = source_deployment(descriptor, attempt_id=0, coordinator_epoch=4)
    second = source_deployment(descriptor, attempt_id=0, coordinator_epoch=5)
    second.task.task_id = "job-1:words-shadow:0"
    try:
        await manager.deploy(first)
        await manager.deploy(second)

        assert not first_consumer.stopped
        assert manager.get(second.task.task_id).coordinator_epoch == 5
        with pytest.raises(WorkerTaskError, match="旧 coordinator epoch"):
            await manager.arm_checkpoint(first.task.task_id, 0, 1, 4)
        with pytest.raises(WorkerTaskError, match="旧 coordinator epoch"):
            await manager.deploy(
                source_deployment(
                    descriptor,
                    attempt_id=2,
                    coordinator_epoch=4,
                )
            )
    finally:
        await manager.close()
        await server.close()


@pytest.mark.asyncio
async def test_manager_拒绝错误worker和篡改制品(tmp_path: Path) -> None:
    content, descriptor = build_source_bundle(tmp_path)
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    manager = WorkerTaskManager(
        "worker-1",
        tmp_path / "work",
        server,
        MemoryFetcher(content),
        consumer_factory=lambda *args, **kwargs: BlockingConsumer(),
    )
    with pytest.raises(WorkerTaskError, match="不能部署"):
        await manager.deploy(source_deployment(descriptor, worker_id="worker-2"))

    bad_content = content + b"tampered"
    bad_descriptor = ArtifactDescriptor(
        "job-2",
        hashlib.sha256(content).hexdigest(),
        len(bad_content),
    )
    bad_manager = WorkerTaskManager(
        "worker-1",
        tmp_path / "bad-work",
        server,
        MemoryFetcher(bad_content),
        consumer_factory=lambda *args, **kwargs: BlockingConsumer(),
    )
    bad_deployment = source_deployment(bad_descriptor)
    bad_deployment.task.job_id = "job-2"
    bad_deployment.task.task_id = "job-2:words:0"
    with pytest.raises(WorkerTaskError, match="下载制品不匹配"):
        await bad_manager.deploy(bad_deployment)

    await server.close()


@pytest.mark.asyncio
async def test_manager_运行时异常上报_jobmanager(tmp_path: Path) -> None:
    content, descriptor = build_source_bundle(tmp_path)
    reporter = RecordingReporter()
    consumer = BlockingConsumer(fail=True)
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    manager = WorkerTaskManager(
        "worker-1",
        tmp_path / "work",
        server,
        MemoryFetcher(content),
        status_reporter=reporter,
        consumer_factory=lambda *args, **kwargs: consumer,
    )
    deployment = source_deployment(descriptor, coordinator_epoch=4)

    await manager.deploy(deployment)
    await asyncio.wait_for(reporter.reported.wait(), timeout=2)

    snapshot = manager.get(deployment.task.task_id)
    assert snapshot.state is TaskRuntimeState.FAILED
    assert reporter.failures[0][:2] == ("job-1", deployment.task.task_id)
    assert reporter.failures[0][2] == 0
    assert reporter.failures[0][3] == 4
    assert "broker disconnected" in reporter.failures[0][4]
    await manager.close()
    await server.close()


@pytest.mark.asyncio
async def test_manager_为_source_加载_payload_validator(tmp_path: Path) -> None:
    content, descriptor = build_source_bundle(
        tmp_path,
        validator="wordcount_udfs:validate_input",
        bad_record_policy="skip",
    )
    server = DataPlaneServer("127.0.0.1", 0)
    await server.start()
    manager = WorkerTaskManager(
        "worker-1",
        tmp_path / "work",
        server,
        MemoryFetcher(content),
        consumer_factory=lambda *args, **kwargs: FiniteConsumer(b"{}"),
    )
    deployment = source_deployment(descriptor)

    await manager.deploy(deployment)
    for _ in range(100):
        snapshot = manager.get(deployment.task.task_id)
        if snapshot.state is TaskRuntimeState.STOPPED:
            break
        await asyncio.sleep(0.01)

    assert snapshot.state is TaskRuntimeState.STOPPED
    assert snapshot.operator_metrics == {"records_read": 0, "bad_records": 1}
    await manager.close()
    await server.close()
