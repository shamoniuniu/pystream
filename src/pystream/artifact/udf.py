"""按作业命名空间隔离加载可信 Python UDF，并校验调用契约。"""

from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import re
import sys
import types
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from pystream.artifact.errors import UDFContractError, UDFLoadError

_UDF_REFERENCE = re.compile(
    r"^(?P<module>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)"
    r":(?P<function>[A-Za-z_][A-Za-z0-9_]*)$"
)


class UDFKind(StrEnum):
    """第一阶段用户函数类型及其位置参数契约。"""

    MAP = "map"
    KEY_SELECTOR = "key_selector"
    REDUCE = "reduce"
    VALIDATOR = "validator"

    @property
    def positional_arguments(self) -> int:
        """返回调用所需的位置参数数量。"""
        return 2 if self is UDFKind.REDUCE else 1


def _assert_json_serializable(value: Any, *, reference: str) -> None:
    try:
        json.dumps(value, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError, OverflowError) as exc:
        raise UDFContractError(f"UDF {reference!r} 返回值必须可严格 JSON 序列化: {exc}") from exc


def _validate_callable_contract(
    function: Callable[..., Any],
    *,
    reference: str,
    kind: UDFKind,
) -> None:
    if inspect.iscoroutinefunction(function):
        raise UDFContractError(f"UDF {reference!r} 必须是同步函数")
    try:
        signature = inspect.signature(function)
        signature.bind(*([object()] * kind.positional_arguments))
    except (TypeError, ValueError) as exc:
        raise UDFContractError(
            f"UDF {reference!r} 不满足 {kind.value} 的 "
            f"{kind.positional_arguments} 个位置参数契约: {exc}"
        ) from exc


@dataclass(frozen=True, slots=True)
class LoadedUDF:
    """经过加载和签名校验、可直接调用的 UDF。"""

    reference: str
    kind: UDFKind
    function: Callable[..., Any]

    def __call__(self, *arguments: Any) -> Any:
        if len(arguments) != self.kind.positional_arguments:
            raise UDFContractError(
                f"UDF {self.reference!r} 调用参数数量错误: "
                f"expected={self.kind.positional_arguments}, actual={len(arguments)}"
            )
        result = self.function(*arguments)
        _assert_json_serializable(result, reference=self.reference)
        return result


class UDFLoader:
    """把一个作业目录映射到独立 Python 命名空间。

    作业 UDF 被视为可信代码；本类只保证来源目录约束和作业间模块缓存隔离，
    不提供权限沙箱。包内依赖应使用相对导入。
    """

    def __init__(self, job_root: str | Path, *, job_id: str) -> None:
        root = Path(job_root)
        if root.is_symlink() or not root.is_dir():
            raise UDFLoadError(f"作业目录不存在或不是普通目录: {root}")
        self._root = root.resolve()
        identity = hashlib.sha256(
            f"{job_id}\0{self._root}\0{uuid.uuid4().hex}".encode()
        ).hexdigest()
        self._namespace = f"_pystream_job_{identity}"
        self._closed = False
        namespace = types.ModuleType(self._namespace)
        namespace.__package__ = self._namespace
        namespace.__path__ = [str(self._root)]  # type: ignore[attr-defined]
        namespace.__file__ = None
        sys.modules[self._namespace] = namespace

    @property
    def namespace(self) -> str:
        """返回该作业专属的内部模块名前缀。"""
        return self._namespace

    def _module_source(self, module_name: str) -> Path:
        parts = module_name.split(".")
        current = self._root
        for part in parts[:-1]:
            current /= part
            if current.is_symlink() or not current.is_dir():
                raise UDFLoadError(f"UDF 模块包不存在或不是普通目录: {module_name!r}")
        leaf = parts[-1]
        module_file = current / f"{leaf}.py"
        package_file = current / leaf / "__init__.py"
        candidates = [candidate for candidate in (module_file, package_file) if candidate.exists()]
        if len(candidates) != 1:
            if not candidates:
                raise UDFLoadError(f"UDF 模块不在当前作业目录: {module_name!r}")
            raise UDFLoadError(f"UDF 模块文件与同名包冲突: {module_name!r}")
        source = candidates[0]
        if source.is_symlink() or not source.is_file():
            raise UDFLoadError(f"UDF 模块必须是普通 Python 源文件: {module_name!r}")
        try:
            source.resolve().relative_to(self._root)
        except ValueError as exc:  # pragma: no cover - 前置链接检查的纵深防御
            raise UDFLoadError(f"UDF 模块逃逸作业目录: {module_name!r}") from exc
        return source

    def load(self, reference: str, kind: UDFKind | str) -> LoadedUDF:
        """加载 `module:function` 并校验对应算子调用契约。"""
        if self._closed:
            raise UDFLoadError("UDFLoader 已关闭")
        match = _UDF_REFERENCE.fullmatch(reference)
        if match is None:
            raise UDFLoadError(f"非法 UDF 引用 {reference!r}, 必须使用 module:function 格式")
        try:
            normalized_kind = UDFKind(kind)
        except ValueError as exc:
            raise UDFContractError(f"不支持的 UDF 类型: {kind!r}") from exc
        module_name = match.group("module")
        function_name = match.group("function")
        self._module_source(module_name)
        qualified_module = f"{self._namespace}.{module_name}"
        try:
            module = importlib.import_module(qualified_module)
        except Exception as exc:
            self._remove_modules(include_namespace=False)
            raise UDFLoadError(f"加载 UDF 模块 {module_name!r} 失败: {exc}") from exc

        function = getattr(module, function_name, None)
        if not callable(function):
            raise UDFLoadError(f"UDF {reference!r} 不存在或不可调用")
        _validate_callable_contract(
            function,
            reference=reference,
            kind=normalized_kind,
        )
        return LoadedUDF(
            reference=reference,
            kind=normalized_kind,
            function=function,
        )

    def _remove_modules(self, *, include_namespace: bool = True) -> None:
        prefix = f"{self._namespace}."
        names = [
            name
            for name in sys.modules
            if name.startswith(prefix) or (include_namespace and name == self._namespace)
        ]
        for name in sorted(names, key=len, reverse=True):
            sys.modules.pop(name, None)

    def close(self) -> None:
        """清理该作业在 `sys.modules` 中的全部隔离模块。"""
        if not self._closed:
            self._remove_modules()
            self._closed = True

    def __enter__(self) -> UDFLoader:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


__all__ = ["LoadedUDF", "UDFKind", "UDFLoader"]
