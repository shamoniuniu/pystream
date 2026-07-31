"""Worker HTTP 边界使用的严格部署序列化辅助函数。"""

from __future__ import annotations

from typing import Any

from pystream.api import OperatorType, Partitioning
from pystream.checkpoint import CheckpointError, TaskSnapshotDescriptor
from pystream.control import (
    ArtifactDescriptor,
    PhysicalChannel,
    TaskDeployment,
    TaskEndpoint,
    TaskInstance,
    TaskStatus,
)


class WorkerRequestError(ValueError):
    """Worker 控制请求缺失字段或类型不合法。"""


def deployment_to_dict(deployment: TaskDeployment) -> dict[str, Any]:
    """把控制面部署对象转换为 HTTP JSON 文档。"""
    task = deployment.task
    return {
        "coordinator_epoch": deployment.coordinator_epoch,
        "task": {
            "task_id": task.task_id,
            "job_id": task.job_id,
            "operator_id": task.operator_id,
            "operator_type": task.operator_type.value,
            "subtask_index": task.subtask_index,
            "parallelism": task.parallelism,
            "attempt_id": task.attempt_id,
            "restored_checkpoint_id": task.restored_checkpoint_id,
            "status": task.status.value,
            "worker_id": task.worker_id,
            "slot_index": task.slot_index,
        },
        "artifact": {
            "job_id": deployment.artifact.job_id,
            "sha256": deployment.artifact.sha256,
            "size": deployment.artifact.size,
        },
        "incoming_channels": [_channel_to_dict(item) for item in deployment.incoming_channels],
        "outgoing_channels": [_channel_to_dict(item) for item in deployment.outgoing_channels],
        "restore_descriptors": [
            descriptor.to_dict() for descriptor in deployment.restore_descriptors
        ],
    }


def deployment_from_dict(document: object) -> TaskDeployment:
    """从不可信 HTTP JSON 恢复完整 TaskDeployment。"""
    root = _strict_dict(
        document,
        {
            "task",
            "artifact",
            "incoming_channels",
            "outgoing_channels",
            "restore_descriptors",
            "coordinator_epoch",
        },
        "$",
    )
    task_data = _strict_dict(
        root["task"],
        {
            "task_id",
            "job_id",
            "operator_id",
            "operator_type",
            "subtask_index",
            "parallelism",
            "attempt_id",
            "restored_checkpoint_id",
            "status",
            "worker_id",
            "slot_index",
        },
        "$.task",
    )
    artifact_data = _strict_dict(
        root["artifact"],
        {"job_id", "sha256", "size"},
        "$.artifact",
    )
    incoming = _channel_list(root["incoming_channels"], "$.incoming_channels")
    outgoing = _channel_list(root["outgoing_channels"], "$.outgoing_channels")
    restore_descriptors = _descriptor_list(
        root["restore_descriptors"],
        "$.restore_descriptors",
    )
    try:
        task = TaskInstance(
            task_id=_string(task_data["task_id"], "$.task.task_id"),
            job_id=_string(task_data["job_id"], "$.task.job_id"),
            operator_id=_string(task_data["operator_id"], "$.task.operator_id"),
            operator_type=OperatorType(task_data["operator_type"]),
            subtask_index=_integer(task_data["subtask_index"], "$.task.subtask_index", minimum=0),
            parallelism=_integer(task_data["parallelism"], "$.task.parallelism", minimum=1),
            attempt_id=_integer(task_data["attempt_id"], "$.task.attempt_id", minimum=0),
            restored_checkpoint_id=_nullable_integer(
                task_data["restored_checkpoint_id"],
                "$.task.restored_checkpoint_id",
            ),
            status=TaskStatus(task_data["status"]),
            worker_id=_nullable_string(task_data["worker_id"], "$.task.worker_id"),
            slot_index=_nullable_integer(task_data["slot_index"], "$.task.slot_index"),
        )
        artifact = ArtifactDescriptor(
            job_id=_string(artifact_data["job_id"], "$.artifact.job_id"),
            sha256=_string(artifact_data["sha256"], "$.artifact.sha256"),
            size=_integer(artifact_data["size"], "$.artifact.size", minimum=0),
        )
    except (TypeError, ValueError) as exc:
        raise WorkerRequestError(f"部署字段无效: {exc}") from exc
    if task.job_id != artifact.job_id:
        raise WorkerRequestError("$.artifact.job_id 必须与 $.task.job_id 一致")
    if task.restored_checkpoint_id is None:
        if restore_descriptors:
            raise WorkerRequestError(
                "未设置 restored_checkpoint_id 时 restore_descriptors 必须为空"
            )
    else:
        if not restore_descriptors:
            raise WorkerRequestError("设置 restored_checkpoint_id 时必须提供 restore_descriptors")
        for descriptor in restore_descriptors:
            if (
                descriptor.job_id != task.job_id
                or descriptor.checkpoint_id != task.restored_checkpoint_id
                or descriptor.operator_id != task.operator_id
                or descriptor.attempt_id >= task.attempt_id
            ):
                raise WorkerRequestError("restore descriptor 与 Task 恢复身份不匹配")
        if task.operator_type is OperatorType.SOURCE:
            if not any(descriptor.task_id == task.task_id for descriptor in restore_descriptors):
                raise WorkerRequestError("Source restore descriptors 缺少当前 task_id")
        elif len(restore_descriptors) != 1 or restore_descriptors[0].task_id != task.task_id:
            raise WorkerRequestError("非 Source Task 必须只恢复自身 descriptor")
    return TaskDeployment(
        task=task,
        artifact=artifact,
        incoming_channels=incoming,
        outgoing_channels=outgoing,
        restore_descriptors=restore_descriptors,
        coordinator_epoch=_integer(
            root["coordinator_epoch"],
            "$.coordinator_epoch",
            minimum=0,
        ),
    )


def _channel_to_dict(channel: PhysicalChannel) -> dict[str, Any]:
    endpoint = channel.target_endpoint
    return {
        "channel_id": channel.channel_id,
        "source_task_id": channel.source_task_id,
        "target_task_id": channel.target_task_id,
        "partitioning": channel.partitioning.value,
        "target_endpoint": (
            None
            if endpoint is None
            else {
                "task_id": endpoint.task_id,
                "host": endpoint.host,
                "port": endpoint.port,
            }
        ),
    }


def _descriptor_list(
    value: object,
    path: str,
) -> tuple[TaskSnapshotDescriptor, ...]:
    if not isinstance(value, list):
        raise WorkerRequestError(f"{path} 必须是 array")
    try:
        return tuple(TaskSnapshotDescriptor.from_dict(item) for item in value)
    except CheckpointError as exc:
        raise WorkerRequestError(f"{path} 包含非法 descriptor: {exc}") from exc


def _channel_list(value: object, path: str) -> tuple[PhysicalChannel, ...]:
    if not isinstance(value, list):
        raise WorkerRequestError(f"{path} 必须是 array")
    result: list[PhysicalChannel] = []
    for index, item in enumerate(value):
        item_path = f"{path}[{index}]"
        data = _strict_dict(
            item,
            {
                "channel_id",
                "source_task_id",
                "target_task_id",
                "partitioning",
                "target_endpoint",
            },
            item_path,
        )
        endpoint_data = data["target_endpoint"]
        endpoint = None
        if endpoint_data is not None:
            endpoint_document = _strict_dict(
                endpoint_data,
                {"task_id", "host", "port"},
                f"{item_path}.target_endpoint",
            )
            endpoint = TaskEndpoint(
                task_id=_string(
                    endpoint_document["task_id"],
                    f"{item_path}.target_endpoint.task_id",
                ),
                host=_string(
                    endpoint_document["host"],
                    f"{item_path}.target_endpoint.host",
                ),
                port=_integer(
                    endpoint_document["port"],
                    f"{item_path}.target_endpoint.port",
                    minimum=1,
                    maximum=65535,
                ),
            )
        try:
            channel = PhysicalChannel(
                channel_id=_string(data["channel_id"], f"{item_path}.channel_id"),
                source_task_id=_string(
                    data["source_task_id"],
                    f"{item_path}.source_task_id",
                ),
                target_task_id=_string(
                    data["target_task_id"],
                    f"{item_path}.target_task_id",
                ),
                partitioning=Partitioning(data["partitioning"]),
                target_endpoint=endpoint,
            )
        except ValueError as exc:
            raise WorkerRequestError(f"{item_path} 字段无效: {exc}") from exc
        if endpoint is not None and endpoint.task_id != channel.target_task_id:
            raise WorkerRequestError(
                f"{item_path}.target_endpoint.task_id 必须与 target_task_id 一致"
            )
        result.append(channel)
    return tuple(result)


def _strict_dict(value: object, fields: set[str], path: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise WorkerRequestError(f"{path} 必须是 object")
    missing = fields - value.keys()
    extra = value.keys() - fields
    if missing or extra:
        details = []
        if missing:
            details.append("缺少 " + ", ".join(sorted(missing)))
        if extra:
            details.append("未知 " + ", ".join(sorted(extra)))
        raise WorkerRequestError(f"{path} 字段错误: {'; '.join(details)}")
    return value


def _string(value: object, path: str) -> str:
    if not isinstance(value, str) or not value:
        raise WorkerRequestError(f"{path} 必须是非空字符串")
    return value


def _nullable_string(value: object, path: str) -> str | None:
    return None if value is None else _string(value, path)


def _integer(
    value: object,
    path: str,
    *,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise WorkerRequestError(f"{path} 必须是大于等于 {minimum} 的整数")
    if maximum is not None and value > maximum:
        raise WorkerRequestError(f"{path} 必须小于等于 {maximum}")
    return value


def _nullable_integer(value: object, path: str) -> int | None:
    return None if value is None else _integer(value, path)


__all__ = [
    "WorkerRequestError",
    "deployment_from_dict",
    "deployment_to_dict",
]
