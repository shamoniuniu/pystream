"""WordCount 演示脚本共享的环境、job_id 和输出读取函数。"""

from __future__ import annotations

import csv
import os
import time
from collections import Counter
from pathlib import Path

from pystream.client import JobManagerClient
from pystream.security import TlsFiles, create_client_ssl_context

DEFAULT_JOBMANAGER_URL = os.getenv("PYSTREAM_JOBMANAGER_URL", "http://localhost:8080")
DEFAULT_OUTPUT_ROOT = Path(os.getenv("PYSTREAM_OUTPUT_ROOT", "output"))
JOB_ID_MARKER = ".last_wordcount_job_id"
EXPECTED_WORDCOUNT = Counter({"apple": 2, "pie": 1})


def kafka_client_options() -> dict[str, object]:
    """Build aiokafka SSL options from file-only environment settings."""
    ca_file = os.getenv("PYSTREAM_KAFKA_CA_FILE")
    cert_file = os.getenv("PYSTREAM_KAFKA_CERT_FILE")
    key_file = os.getenv("PYSTREAM_KAFKA_KEY_FILE")
    configured = (ca_file, cert_file, key_file)
    if all(value is None for value in configured):
        return {}
    if any(value is None for value in configured):
        raise ValueError("Kafka SSL 要求同时配置 CA、certificate 和 private key 文件")
    files = TlsFiles(Path(ca_file), Path(cert_file), Path(key_file))
    return {
        "security_protocol": "SSL",
        "ssl_context": create_client_ssl_context(files),
    }


def marker_path(output_root: Path) -> Path:
    """返回共享卷中的最近一次演示作业标记。"""
    return output_root / JOB_ID_MARKER


def save_job_id(output_root: Path, job_id: str) -> None:
    """原子保存演示 job_id，供后续脚本读取。"""
    output_root.mkdir(parents=True, exist_ok=True)
    marker = marker_path(output_root)
    temporary = marker.with_suffix(".tmp")
    temporary.write_text(f"{job_id}\n", encoding="utf-8")
    temporary.replace(marker)


def load_job_id(output_root: Path, explicit: str | None = None) -> str:
    """返回显式 job_id，或读取最近一次演示标记。"""
    if explicit:
        return explicit
    marker = marker_path(output_root)
    try:
        job_id = marker.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise RuntimeError(f"无法读取 job_id 标记 {marker}: {exc}") from exc
    if not job_id:
        raise RuntimeError(f"job_id 标记为空: {marker}")
    return job_id


def wait_for_workers(
    jobmanager_url: str,
    *,
    expected: int,
    timeout: float,
) -> None:
    """等待控制面看到足够数量的健康 Worker。"""
    deadline = time.monotonic() + timeout
    last_count = 0
    client = JobManagerClient(jobmanager_url, timeout=3)
    while time.monotonic() < deadline:
        try:
            document = client.workers()
            workers = document.get("workers", [])
            last_count = sum(
                isinstance(item, dict) and item.get("healthy") is True for item in workers
            )
            if last_count >= expected:
                return
        except (OSError, ValueError):
            pass
        time.sleep(0.5)
    raise TimeoutError(f"等待 {expected} 个健康 Worker 超时; 最后观察到 {last_count} 个")


def output_files(output_root: Path, job_id: str) -> tuple[Path, ...]:
    """按稳定顺序返回 WordCount Sink 分片。"""
    return tuple(sorted((output_root / job_id / "output").glob("part-*.csv")))


def read_counts(files: tuple[Path, ...]) -> tuple[Counter[str], list[tuple[str, str, int]]]:
    """读取无表头 CSV，返回跨窗口计数汇总和原始行。"""
    counts: Counter[str] = Counter()
    rows: list[tuple[str, str, int]] = []
    for path in files:
        with path.open("r", encoding="utf-8", newline="") as stream:
            for row_number, row in enumerate(csv.reader(stream), start=1):
                if len(row) != 3:
                    raise RuntimeError(f"{path}:{row_number} 应为 3 列; 实际 {len(row)}")
                window_end, word, raw_count = row
                try:
                    count = int(raw_count)
                except ValueError as exc:
                    raise RuntimeError(f"{path}:{row_number} count 不是整数") from exc
                rows.append((window_end, word, count))
                counts[word] += count
    return counts, rows
