"""中文文档入口、模块覆盖和相对链接契约测试。"""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REQUIRED_DOCS = {
    "README.md",
    "docs/README.md",
    "docs/api.md",
    "docs/architecture.md",
    "docs/deployment.md",
    "docs/modules.md",
    "docs/roadmap.md",
    "docs/testing.md",
    "docs/troubleshooting.md",
}
LINK_PATTERN = re.compile(r"\[[^\]]+\]\((?!https?://|#)([^)]+)\)")
CHINESE_PATTERN = re.compile(r"[\u4e00-\u9fff]")


def test_关键中文文档齐全且根readme可发现() -> None:
    for relative in REQUIRED_DOCS:
        path = ROOT / relative
        assert path.is_file(), relative
        content = path.read_text(encoding="utf-8")
        assert CHINESE_PATTERN.search(content), f"{relative} 缺少中文说明"

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    for relative in REQUIRED_DOCS - {"README.md"}:
        assert relative in readme


def test_文档相对链接全部存在() -> None:
    for relative in REQUIRED_DOCS:
        path = ROOT / relative
        for target in LINK_PATTERN.findall(path.read_text(encoding="utf-8")):
            target_path = target.split("#", 1)[0]
            if not target_path:
                continue
            assert (path.parent / target_path).resolve().exists(), (
                f"{relative} 包含失效链接 {target}"
            )


def test_每个python模块都有中文模块docstring() -> None:
    for path in sorted((ROOT / "src" / "pystream").rglob("*.py")):
        module = ast.parse(path.read_text(encoding="utf-8"))
        docstring = ast.get_docstring(module)
        assert docstring, f"{path.relative_to(ROOT)} 缺少模块 docstring"
        assert CHINESE_PATTERN.search(docstring), (
            f"{path.relative_to(ROOT)} 模块 docstring 缺少中文"
        )


def test_文档明确第一阶段失败语义和docker验证状态() -> None:
    combined = "\n".join(
        (ROOT / relative).read_text(encoding="utf-8")
        for relative in ("README.md", "docs/deployment.md", "docs/roadmap.md")
    )

    assert "不自动恢复" in combined
    assert "At-least-once" in combined
    assert "Exactly-once" in combined
    assert "没有 `docker` 命令" in combined
    assert "多容器运行结果仍待" in combined


def test_文档生命周期包含权威来源和新鲜度控制() -> None:
    content = (ROOT / "docs" / "README.md").read_text(encoding="utf-8")

    assert "Diátaxis quadrant" in content
    assert "Responsibility path" in content
    assert "Source of truth" in content
    assert "Last verified" in content
    assert "Verification cadence" in content
    assert "Staleness signal" in content
    assert "每类事实只能有一个权威位置" in content
    assert "test_documentation.py" in content
