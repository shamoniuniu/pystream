"""Worker 注册、资源视图和 slot 调度测试。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from pystream.api import StreamGraph
from pystream.control import (
    InsufficientSlots,
    SlotScheduler,
    TaskStatus,
    WorkerNotFound,
    WorkerRegistry,
    build_execution_graph,
)


def register_workers(
    registry: WorkerRegistry,
    count: int = 3,
    slots: int = 4,
    heartbeat_at: datetime | None = None,
) -> None:
    """注册地址稳定的测试 Worker。"""
    for index in range(count):
        registry.register(
            worker_id=f"worker-{index}",
            control_address=f"http://worker-{index}:8081",
            data_host=f"worker-{index}",
            data_port=9000 + index,
            total_slots=slots,
            heartbeat_at=heartbeat_at,
        )


def test_registry_注册心跳和资源健康快照():
    heartbeat = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
    registry = WorkerRegistry(timedelta(seconds=10))
    register_workers(registry, count=2, slots=2, heartbeat_at=heartbeat)
    registry.get("worker-0").reserve("existing-task")

    views = registry.resource_view(heartbeat + timedelta(seconds=11))

    assert [view.worker_id for view in views] == ["worker-0", "worker-1"]
    assert views[0].used_slots == 1
    assert views[0].available_slots == 1
    assert not views[0].healthy
    registry.heartbeat("worker-0", heartbeat + timedelta(seconds=11))
    assert [worker.worker_id for worker in registry.healthy(heartbeat + timedelta(seconds=11))] == [
        "worker-0"
    ]


def test_registry_拒绝非法超时和未知_worker():
    with pytest.raises(ValueError, match="heartbeat_timeout"):
        WorkerRegistry(timedelta(0))

    registry = WorkerRegistry()
    with pytest.raises(WorkerNotFound, match="尚未注册"):
        registry.get("missing")
    with pytest.raises(WorkerNotFound, match="尚未注册"):
        registry.heartbeat("missing")


def test_registry_活动_worker_合法重注册保留slot():
    registry = WorkerRegistry()
    worker = registry.register(
        "worker-a",
        "http://old",
        "old-host",
        9000,
        2,
        incarnation_id="process-1",
    )
    worker.reserve("task-1")

    refreshed = registry.register(
        "worker-a",
        "http://new",
        "new-host",
        9001,
        2,
        incarnation_id="process-1",
    )

    assert refreshed is worker
    assert refreshed.incarnation_id == "process-1"
    assert refreshed.control_address == "http://new"
    assert refreshed.data_host == "new-host"
    assert refreshed.data_port == 9001
    assert refreshed.slots[0].task_id == "task-1"
    with pytest.raises(ValueError, match="不能修改"):
        registry.register("worker-a", "http://new", "new-host", 9001, 3)


def test_registry_新incarnation更新身份并暂时保留旧slot():
    registry = WorkerRegistry()
    worker = registry.register(
        "worker-a",
        "http://old",
        "old-host",
        9000,
        2,
        incarnation_id="process-1",
    )
    worker.reserve("task-1")

    restarted = registry.register(
        "worker-a",
        "http://new",
        "new-host",
        9001,
        2,
        incarnation_id="process-2",
    )

    assert restarted is worker
    assert restarted.incarnation_id == "process-2"
    assert restarted.slots[0].task_id == "task-1"


def test_registry_活动_worker_重注册仍拒绝非法端口():
    """[defect-probing] BUG_MAP-2：活动分支不得绕过注册参数校验。"""
    registry = WorkerRegistry()
    worker = registry.register("worker-a", "http://old", "old-host", 9000, 2)
    worker.reserve("task-1")

    with pytest.raises(ValueError, match="data_port"):
        registry.register("worker-a", "http://new", "new-host", 0, 2)

    assert worker.data_port == 9000


def test_scheduler_资源不足不产生部分分配(two_task_graph: StreamGraph):
    registry = WorkerRegistry()
    register_workers(registry, count=1, slots=1)
    execution = build_execution_graph("job-small", two_task_graph)

    with pytest.raises(InsufficientSlots) as exc_info:
        SlotScheduler().schedule(execution, registry)

    assert exc_info.value.required == 2
    assert exc_info.value.available == 1
    assert registry.get("worker-0").available_slots == 1
    assert all(task.status is TaskStatus.CREATED for task in execution.tasks.values())


def test_scheduler_确定性均衡分配并绑定目标端点(linear_graph: StreamGraph):
    registry = WorkerRegistry()
    register_workers(registry)
    execution = build_execution_graph("job-1", linear_graph)

    SlotScheduler().schedule(execution, registry)

    assignments = {
        task.task_id: (task.worker_id, task.slot_index) for task in execution.tasks.values()
    }
    assert len({worker_id for worker_id, _ in assignments.values()}) == 3
    used = [worker.used_slots for worker in registry.all()]
    assert max(used) - min(used) <= 1
    assert all(channel.target_endpoint is not None for channel in execution.channels)
    assert all(
        channel.target_endpoint.host
        == registry.get(execution.tasks[channel.target_task_id].worker_id or "").data_host
        for channel in execution.channels
    )

    second_registry = WorkerRegistry()
    register_workers(second_registry)
    second_execution = build_execution_graph("job-1", linear_graph)
    SlotScheduler().schedule(second_execution, second_registry)
    assert {
        task.task_id: (task.worker_id, task.slot_index) for task in second_execution.tasks.values()
    } == assignments


def test_scheduler_有第二个空闲worker时保证跨节点放置(two_task_graph: StreamGraph):
    """[defect-probing] BUG_MAP-1：已有负载不应取消新作业的跨节点证明。"""
    registry = WorkerRegistry()
    register_workers(registry, count=2, slots=4)
    busy = registry.get("worker-1")
    for index in range(3):
        busy.reserve(f"existing-{index}")
    execution = build_execution_graph("job-small", two_task_graph)

    SlotScheduler().schedule(execution, registry)

    assert len({task.worker_id for task in execution.tasks.values()}) >= 2


def test_scheduler_release_释放全部slot且幂等(linear_graph: StreamGraph):
    registry = WorkerRegistry()
    register_workers(registry)
    execution = build_execution_graph("job-1", linear_graph)
    scheduler = SlotScheduler()
    scheduler.schedule(execution, registry)

    scheduler.release(execution, registry)
    scheduler.release(execution, registry)

    assert all(worker.used_slots == 0 for worker in registry.all())
    assert all(
        task.worker_id is None and task.slot_index is None for task in execution.tasks.values()
    )
