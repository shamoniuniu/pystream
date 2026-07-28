"""作业制品与 UDF 加载错误类型。"""

from __future__ import annotations


class ArtifactError(Exception):
    """作业制品处理失败的基类。"""


class ArtifactValidationError(ArtifactError):
    """制品结构、摘要或文件边界不合法。"""


class UDFLoadError(ArtifactError):
    """UDF 引用无法在当前作业目录中安全加载。"""


class UDFContractError(ArtifactError):
    """UDF 签名或返回值不满足算子契约。"""


__all__ = [
    "ArtifactError",
    "ArtifactValidationError",
    "UDFContractError",
    "UDFLoadError",
]
