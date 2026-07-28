"""TaskRuntime 与数据端口服务器的运行时错误。"""


class RuntimeErrorBase(RuntimeError):
    """运行时错误基类。"""


class RuntimeLifecycleError(RuntimeErrorBase):
    """任务或服务器处于不允许当前操作的生命周期状态。"""


class RuntimeConnectionError(RuntimeErrorBase):
    """任务间数据连接建立、握手或传输失败。"""


class RuntimeTaskError(RuntimeErrorBase):
    """算子、Source 或 Sink 执行失败。"""


__all__ = [
    "RuntimeConnectionError",
    "RuntimeErrorBase",
    "RuntimeLifecycleError",
    "RuntimeTaskError",
]
