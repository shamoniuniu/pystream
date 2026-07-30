"""中级事件时间与恢复演示共享的路径、预期结果和输出读取函数。"""

from __future__ import annotations

import csv
import os
from collections import Counter
from pathlib import Path

from _demo import DEFAULT_JOBMANAGER_URL, DEFAULT_OUTPUT_ROOT

DEFAULT_BOOTSTRAP_SERVERS = os.getenv(
    "PYSTREAM_KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
DEFAULT_TOPIC = "intermediate-words"
JOB_ID_MARKER = ".last_intermediate_job_id"

BASELINE_ROWS = Counter(
    {
        ("2026/07/29T00:00:05", 1, 1): 1,
        ("2026/07/29T00:00:05", 2, 1): 1,
    }
)
RECOVERY_PHASE_ROWS = Counter(
    {
        ("2026/07/29T00:00:10", 1, 1): 1,
        ("2026/07/29T00:00:10", 2, 2): 1,
    }
)
EXPECTED_ROWS = BASELINE_ROWS + RECOVERY_PHASE_ROWS


def marker_path(output_root: Path) -> Path:
    """返回共享卷中的最近一次中级演示作业标记。"""
    return output_root / JOB_ID_MARKER


def save_job_id(output_root: Path, job_id: str) -> None:
    """原子保存中级演示 job_id。"""
    output_root.mkdir(parents=True, exist_ok=True)
    marker = marker_path(output_root)
    temporary = marker.with_suffix(".tmp")
    temporary.write_text(f"{job_id}\n", encoding="utf-8")
    temporary.replace(marker)


def load_job_id(output_root: Path, explicit: str | None = None) -> str:
    """返回显式 job_id，或读取最近一次中级演示标记。"""
    if explicit:
        return explicit
    marker = marker_path(output_root)
    try:
        job_id = marker.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise RuntimeError(f"无法读取中级 job_id 标记 {marker}: {exc}") from exc
    if not job_id:
        raise RuntimeError(f"中级 job_id 标记为空: {marker}")
    return job_id


def output_files(output_root: Path, job_id: str) -> tuple[Path, ...]:
    """按稳定顺序返回中级 File Sink 分片。"""
    return tuple(sorted((output_root / job_id / "output").glob("part-*.csv")))


def read_rows(files: tuple[Path, ...]) -> Counter[tuple[str, int, int]]:
    """读取中级 CSV，返回窗口结束、count 桶和 word 数的多重集。"""
    rows: Counter[tuple[str, int, int]] = Counter()
    for path in files:
        with path.open("r", encoding="utf-8", newline="") as stream:
            for row_number, row in enumerate(csv.reader(stream), start=1):
                if len(row) != 3:
                    raise RuntimeError(f"{path}:{row_number} 应为 3 列; 实际 {len(row)}")
                window_end, raw_count, raw_word_count = row
                try:
                    count = int(raw_count)
                    word_count = int(raw_word_count)
                except ValueError as exc:
                    raise RuntimeError(f"{path}:{row_number} 聚合值不是整数") from exc
                rows[(window_end, count, word_count)] += 1
    return rows


__all__ = [
    "BASELINE_ROWS",
    "DEFAULT_BOOTSTRAP_SERVERS",
    "DEFAULT_JOBMANAGER_URL",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_TOPIC",
    "EXPECTED_ROWS",
    "RECOVERY_PHASE_ROWS",
    "load_job_id",
    "marker_path",
    "output_files",
    "read_rows",
    "save_job_id",
]
