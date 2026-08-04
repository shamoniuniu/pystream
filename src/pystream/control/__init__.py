"""JobManager 与 Worker 控制面。

该模块负责物理执行图、slot 调度、Worker 心跳、制品下载边界和任务部署编排。
第一阶段遇到 Worker 或任务故障时停止整条作业并释放资源，不执行自动恢复。
"""

from pystream.control.artifacts import LocalArtifactRepository, S3ArtifactRepository
from pystream.control.checkpoint import (
    CheckpointCoordinationError,
    CheckpointCoordinator,
)
from pystream.control.errors import (
    ArtifactError,
    ControlPlaneError,
    DeploymentError,
    InsufficientSlots,
    InvalidStateTransition,
    NotLeaderError,
    WorkerNotFound,
)
from pystream.control.execution import ExecutionGraph, build_execution_graph
from pystream.control.http import JobManagerHttpService
from pystream.control.leader import (
    DEFAULT_LEASE_TTL,
    DEFAULT_RENEW_INTERVAL,
    DEFAULT_STANDBY_POLL_INTERVAL,
    LEADER_LEASE_SCHEMA_VERSION,
    LeaderCoordinator,
    LeaderElectionError,
    LeaderLease,
    LeaderLeaseLost,
    LeaderLeaseRepository,
    S3LeaderLeaseRepository,
    StoredLeaderLease,
)
from pystream.control.manager import JobManager, JobRun
from pystream.control.metadata import (
    JOB_METADATA_SCHEMA_VERSION,
    JobMetadataConflict,
    JobMetadataError,
    JobMetadataRepository,
    JobMetadataRevision,
    S3JobMetadataRepository,
    StoredJobMetadata,
)
from pystream.control.models import (
    ArtifactDescriptor,
    CoordinatorRole,
    Job,
    JobStatus,
    PhysicalChannel,
    ResourceView,
    TaskEndpoint,
    TaskInstance,
    TaskStatus,
    WorkerNode,
    WorkerSlot,
)
from pystream.control.ports import ArtifactRepository, TaskDeployment, WorkerGateway
from pystream.control.scheduler import SlotScheduler, WorkerRegistry

__all__ = [
    "DEFAULT_LEASE_TTL",
    "DEFAULT_RENEW_INTERVAL",
    "DEFAULT_STANDBY_POLL_INTERVAL",
    "JOB_METADATA_SCHEMA_VERSION",
    "LEADER_LEASE_SCHEMA_VERSION",
    "ArtifactDescriptor",
    "ArtifactError",
    "ArtifactRepository",
    "CheckpointCoordinationError",
    "CheckpointCoordinator",
    "ControlPlaneError",
    "CoordinatorRole",
    "DeploymentError",
    "ExecutionGraph",
    "InsufficientSlots",
    "InvalidStateTransition",
    "Job",
    "JobManager",
    "JobManagerHttpService",
    "JobMetadataConflict",
    "JobMetadataError",
    "JobMetadataRepository",
    "JobMetadataRevision",
    "JobRun",
    "JobStatus",
    "LeaderCoordinator",
    "LeaderElectionError",
    "LeaderLease",
    "LeaderLeaseLost",
    "LeaderLeaseRepository",
    "LocalArtifactRepository",
    "NotLeaderError",
    "PhysicalChannel",
    "ResourceView",
    "S3ArtifactRepository",
    "S3JobMetadataRepository",
    "S3LeaderLeaseRepository",
    "SlotScheduler",
    "StoredJobMetadata",
    "StoredLeaderLease",
    "TaskDeployment",
    "TaskEndpoint",
    "TaskInstance",
    "TaskStatus",
    "WorkerGateway",
    "WorkerNode",
    "WorkerNotFound",
    "WorkerRegistry",
    "WorkerSlot",
    "build_execution_graph",
]
