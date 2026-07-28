"""控制面领域错误。

错误类型保持独立于 HTTP 传输，JobManager 服务可以在后续适配层中稳定地
将资源不足、状态冲突和部署失败转换为对外响应。
"""


class ControlPlaneError(RuntimeError):
    """所有控制面可预期错误的基类。"""


class InvalidStateTransition(ControlPlaneError):
    """状态机收到未声明的状态转换。"""


class WorkerNotFound(ControlPlaneError):
    """引用了尚未注册或已被移除的 Worker。"""


class InsufficientSlots(ControlPlaneError):
    """可用 slot 不能容纳完整作业，调度未产生部分分配。"""

    def __init__(self, required: int, available: int) -> None:
        self.required = required
        self.available = available
        super().__init__(f"资源不足: 需要 {required} 个 slots, 当前可用 {available} 个")


class DeploymentError(ControlPlaneError):
    """任务部署、停止或运行状态汇总失败。"""


class ArtifactError(ControlPlaneError):
    """作业制品不存在、摘要不匹配或不可读。"""


__all__ = [
    "ArtifactError",
    "ControlPlaneError",
    "DeploymentError",
    "InsufficientSlots",
    "InvalidStateTransition",
    "WorkerNotFound",
]
