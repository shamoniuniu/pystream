"""作业制品管理。

该模块负责 ZIP 作业包的构建、摘要验证、安全解压和 UDF 隔离加载。
任何制品校验失败都必须阻止任务部署，且不得写入作业目录之外的位置。
"""

from pystream.artifact.bundle import (
    DEFAULT_MAX_FILE_SIZE,
    DEFAULT_MAX_FILES,
    DEFAULT_MAX_TOTAL_SIZE,
    artifact_filename,
    build_job_bundle,
    extract_job_bundle,
    sha256_file,
    verify_job_bundle,
)
from pystream.artifact.errors import (
    ArtifactError,
    ArtifactValidationError,
    UDFContractError,
    UDFLoadError,
)
from pystream.artifact.models import (
    MANIFEST_NAME,
    MANIFEST_VERSION,
    ArtifactManifest,
    JobBundle,
    ManifestEntry,
)
from pystream.artifact.udf import LoadedUDF, UDFKind, UDFLoader

__all__ = [
    "DEFAULT_MAX_FILES",
    "DEFAULT_MAX_FILE_SIZE",
    "DEFAULT_MAX_TOTAL_SIZE",
    "MANIFEST_NAME",
    "MANIFEST_VERSION",
    "ArtifactError",
    "ArtifactManifest",
    "ArtifactValidationError",
    "JobBundle",
    "LoadedUDF",
    "ManifestEntry",
    "UDFContractError",
    "UDFKind",
    "UDFLoadError",
    "UDFLoader",
    "artifact_filename",
    "build_job_bundle",
    "extract_job_bundle",
    "sha256_file",
    "verify_job_bundle",
]
