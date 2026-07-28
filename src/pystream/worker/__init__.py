"""Worker 控制服务与本地任务管理。

该模块负责 Worker 注册/心跳、HTTP 部署接口、作业制品下载与隔离 UDF 加载，
并把物理任务交给 TaskRuntime。第一阶段任务或数据连接失败会直接上报 JobManager。
"""

from pystream.worker.http import (
    HttpArtifactFetcher,
    HttpJobManagerClient,
    HttpWorkerGateway,
    RegistrationClient,
    WorkerHttpService,
    WorkerServiceConfig,
)
from pystream.worker.manager import (
    ArtifactFetcher,
    StatusReporter,
    WorkerTaskError,
    WorkerTaskManager,
)
from pystream.worker.models import (
    WorkerRequestError,
    deployment_from_dict,
    deployment_to_dict,
)

__all__ = [
    "ArtifactFetcher",
    "HttpArtifactFetcher",
    "HttpJobManagerClient",
    "HttpWorkerGateway",
    "RegistrationClient",
    "StatusReporter",
    "WorkerHttpService",
    "WorkerRequestError",
    "WorkerServiceConfig",
    "WorkerTaskError",
    "WorkerTaskManager",
    "deployment_from_dict",
    "deployment_to_dict",
]
