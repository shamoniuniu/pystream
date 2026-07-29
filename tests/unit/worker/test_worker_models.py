"""Worker HTTP 部署 DTO 的严格往返测试。"""

from __future__ import annotations

import pytest

from pystream.api import OperatorType, Partitioning
from pystream.checkpoint import TaskSnapshotDescriptor
from pystream.control import (
    ArtifactDescriptor,
    PhysicalChannel,
    TaskDeployment,
    TaskEndpoint,
    TaskInstance,
    TaskStatus,
)
from pystream.worker import WorkerRequestError, deployment_from_dict, deployment_to_dict


def sample_deployment() -> TaskDeployment:
    task = TaskInstance(
        task_id="job-1:map:0",
        job_id="job-1",
        operator_id="map",
        operator_type=OperatorType.MAP,
        subtask_index=0,
        parallelism=1,
        status=TaskStatus.DEPLOYING,
        worker_id="worker-1",
        slot_index=2,
    )
    channel = PhysicalChannel(
        channel_id="job-1:map:0->job-1:sink:0",
        source_task_id=task.task_id,
        target_task_id="job-1:sink:0",
        partitioning=Partitioning.FORWARD,
        target_endpoint=TaskEndpoint("job-1:sink:0", "worker-2", 9000),
    )
    return TaskDeployment(
        task,
        ArtifactDescriptor("job-1", "a" * 64, 42),
        (),
        (channel,),
    )


def test_deployment_json_严格往返() -> None:
    original = sample_deployment()

    restored = deployment_from_dict(deployment_to_dict(original))

    assert restored == original


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.update({"unknown": True}), "未知 unknown"),
        (lambda value: value["task"].update({"subtask_index": -1}), "subtask_index"),
        (lambda value: value["artifact"].update({"job_id": "other"}), "必须与"),
        (
            lambda value: value["outgoing_channels"][0]["target_endpoint"].update(
                {"task_id": "wrong"}
            ),
            "target_task_id",
        ),
    ],
)
def test_deployment_json_拒绝未知和不一致字段(mutate, message: str) -> None:
    document = deployment_to_dict(sample_deployment())
    mutate(document)

    with pytest.raises(WorkerRequestError, match=message):
        deployment_from_dict(document)


def test_deployment_json_要求数组和合法枚举() -> None:
    document = deployment_to_dict(sample_deployment())
    document["incoming_channels"] = {}
    with pytest.raises(WorkerRequestError, match="array"):
        deployment_from_dict(document)

    document = deployment_to_dict(sample_deployment())
    document["task"]["operator_type"] = "join"
    with pytest.raises(WorkerRequestError, match="部署字段无效"):
        deployment_from_dict(document)


def test_deployment_json_恢复descriptor严格往返并拒绝当前attempt快照() -> None:
    base = sample_deployment()
    base.task.attempt_id = 2
    base.task.restored_checkpoint_id = 7
    descriptor = TaskSnapshotDescriptor(
        job_id=base.task.job_id,
        checkpoint_id=7,
        attempt_id=1,
        task_id=base.task.task_id,
        operator_id=base.task.operator_id,
        relative_path="job-1/checkpoint/tasks/map.json",
        sha256="b" * 64,
        size=100,
    )
    deployment = TaskDeployment(
        base.task,
        base.artifact,
        base.incoming_channels,
        base.outgoing_channels,
        (descriptor,),
    )

    assert deployment_from_dict(deployment_to_dict(deployment)) == deployment

    document = deployment_to_dict(deployment)
    document["restore_descriptors"][0]["attempt_id"] = 2
    with pytest.raises(WorkerRequestError, match="恢复身份"):
        deployment_from_dict(document)
