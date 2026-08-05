"""仅从文件加载 Secret，并提供不含敏感值的稳定错误。"""

from __future__ import annotations

from pathlib import Path


class SecretFileError(ValueError):
    """A required Secret file cannot be read safely."""


def read_secret_file(path: str | Path, name: str) -> str:
    """Read a non-empty UTF-8 Secret without including its value in errors."""
    resolved = Path(path)
    try:
        value = resolved.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as exc:
        raise SecretFileError(f"无法读取 {name} 文件 {resolved}: {type(exc).__name__}") from exc
    if not value:
        raise SecretFileError(f"{name} 文件不能为空")
    return value


__all__ = ["SecretFileError", "read_secret_file"]
