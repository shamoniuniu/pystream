"""控制面状态模型与物理执行图测试。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from pystream.api import OperatorType, Partitioning, StreamGraph
from pystream.control import (
    CoordinatorRole,
    InvalidStateTransition,
    Job,
    JobStatus,
    TaskInstance,
    TaskStatus,
    WorkerNode,
    build_execution_graph,
)


def test_coordinator_role_公开active_standby和protective状态() -> None:
    assert {role.value for role in CoordinatorRole} == {
        "STANDBY",
        "ACTIVE",
        "PROTECTIVE",
    }


def test_job_只允许声明过的状态转换():
    job = Job(job_id="job-1", name="wordcount")

    job.transition(JobStatus.VALIDATING)
    job.transition(JobStatus.DEPLOYING)
    job.transition(JobStatus.RUNNING)
    job.transition(JobStatus.CANCELLING)
    job.transition(JobStatus.CANCELLED)

    assert job.status is JobStatus.CANCELLED
    with pytest.raises(InvalidStateTransition, match="不能从 CANCELLED"):
        job.transition(JobStatus.RUNNING)


def test_job_失败路径保留最后错误():
    job = Job(job_id="job-1", name="wordcount")
    job.transition(JobStatus.VALIDATING)
    job.transition(JobStatus.DEPLOYING)
    job.transition(JobStatus.FAILING, "worker lost")
    job.transition(JobStatus.FAILED, "worker lost")

    assert job.status is JobStatus.FAILED
    assert job.error == "worker lost"


def test_job_恢复状态允许重部署和取消():
    recovered = Job(job_id="job-1", name="wordcount")
    recovered.transition(JobStatus.VALIDATING)
    recovered.transition(JobStatus.DEPLOYING)
    recovered.transition(JobStatus.RUNNING)
    recovered.transition(JobStatus.RECOVERING, "worker lost")
    recovered.transition(JobStatus.DEPLOYING)
    recovered.transition(JobStatus.RUNNING)
    assert recovered.status is JobStatus.RUNNING

    cancelled = Job(job_id="job-2", name="wordcount")
    cancelled.transition(JobStatus.VALIDATING)
    cancelled.transition(JobStatus.DEPLOYING)
    cancelled.transition(JobStatus.RUNNING)
    cancelled.transition(JobStatus.RECOVERING, "worker lost")
    cancelled.transition(JobStatus.CANCELLING)
    cancelled.transition(JobStatus.CANCELLED)
    assert cancelled.status is JobStatus.CANCELLED


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"worker_id": ""}, "worker_id"),
        ({"control_address": ""}, "地址"),
        ({"data_host": ""}, "地址"),
        ({"data_port": 0}, "data_port"),
        ({"data_port": 65536}, "data_port"),
        ({"total_slots": 0}, "total_slots"),
    ],
)
def test_worker_create_拒绝非法注册参数(overrides, message):
    values = {
        "worker_id": "worker-a",
        "control_address": "http://worker-a:8081",
        "data_host": "worker-a",
        "data_port": 9000,
        "total_slots": 2,
    }
    values.update(overrides)

    with pytest.raises(ValueError, match=message):
        WorkerNode.create(**values)


def test_worker_slot_按编号预留_释放幂等并计算健康():
    heartbeat = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
    worker = WorkerNode.create(
        "worker-a",
        "http://worker-a:8081",
        "worker-a",
        9000,
        2,
        heartbeat,
    )

    assert worker.reserve("task-1") == 0
    assert worker.reserve("task-2") == 1
    assert worker.used_slots == 2
    assert worker.available_slots == 0
    with pytest.raises(InvalidStateTransition, match="没有可用"):
        worker.reserve("task-3")

    worker.release("task-1")
    worker.release("task-1")
    assert worker.available_slots == 1
    assert worker.is_healthy(heartbeat + timedelta(seconds=15), timedelta(seconds=15))
    assert not worker.is_healthy(heartbeat + timedelta(seconds=16), timedelta(seconds=15))


def test_task_assignment_状态转换和清理位置():
    task = TaskInstance(
        task_id="job:map:0",
        job_id="job",
        operator_id="map",
        operator_type=OperatorType.MAP,
        subtask_index=0,
        parallelism=2,
    )

    task.assign("worker-a", 1)
    task.transition(TaskStatus.DEPLOYING)
    task.transition(TaskStatus.RUNNING)
    task.transition(TaskStatus.CANCELLING)
    task.transition(TaskStatus.CANCELLED)
    task.clear_assignment()

    assert task.status is TaskStatus.CANCELLED
    assert task.worker_id is None
    assert task.slot_index is None
    with pytest.raises(InvalidStateTransition, match="只有 CREATED"):
        task.assign("worker-b", 0)


def test_task_reset_for_attempt_要求释放资源并严格递增():
    task = TaskInstance(
        task_id="job:map:0",
        job_id="job",
        operator_id="map",
        operator_type=OperatorType.MAP,
        subtask_index=0,
        parallelism=1,
    )
    task.assign("worker-a", 0)
    task.transition(TaskStatus.DEPLOYING)
    task.transition(TaskStatus.FAILED, "lost")

    with pytest.raises(InvalidStateTransition, match="释放"):
        task.reset_for_attempt(1, 3)

    task.clear_assignment()
    task.reset_for_attempt(1, 3)

    assert task.status is TaskStatus.CREATED
    assert task.attempt_id == 1
    assert task.restored_checkpoint_id == 3
    assert task.error is None
    with pytest.raises(InvalidStateTransition, match="严格递增"):
        task.reset_for_attempt(1, 3)


def test_execution_graph_展开并发度_通道和下游优先顺序(linear_graph: StreamGraph):
    execution = build_execution_graph("job-1", linear_graph)

    assert execution.total_tasks == 10
    assert [task.subtask_index for task in execution.operator_tasks("totals")] == [0, 1, 2]
    assert [task.operator_id for task in execution.deployment_order()] == [
        "output",
        "totals",
        "totals",
        "totals",
        "by_word",
        "by_word",
        "normalize",
        "normalize",
        "words",
        "words",
    ]

    channels_by_partition = {
        partitioning: [
            channel for channel in execution.channels if channel.partitioning is partitioning
        ]
        for partitioning in Partitioning
    }
    assert len(channels_by_partition[Partitioning.FORWARD]) == 4
    assert len(channels_by_partition[Partitioning.REBALANCE]) == 0
    assert len(channels_by_partition[Partitioning.HASH]) == 9
    assert all(
        channel.source_task_id.rsplit(":", 1)[-1] == channel.target_task_id.rsplit(":", 1)[-1]
        for channel in channels_by_partition[Partitioning.FORWARD]
    )


def test_execution_graph_查询通道并拒绝未调度端点(linear_graph: StreamGraph):
    execution = build_execution_graph("job-1", linear_graph)
    reduce_task = execution.operator_tasks("totals")[0]

    assert len(execution.incoming_channels(reduce_task.task_id)) == 2
    assert len(execution.outgoing_channels(reduce_task.task_id)) == 1
    with pytest.raises(ValueError, match="尚未完成调度"):
        execution.bind_endpoints({})
    with pytest.raises(KeyError):
        execution.operator_tasks("missing")
