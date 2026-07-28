"""等待 WordCount 的 10 秒处理时间窗口产生可见 CSV 输出。"""

from __future__ import annotations

import argparse
import time
from collections.abc import Sequence
from pathlib import Path

from _demo import (
    DEFAULT_JOBMANAGER_URL,
    DEFAULT_OUTPUT_ROOT,
    EXPECTED_WORDCOUNT,
    load_job_id,
    output_files,
    read_counts,
)

from pystream.client import JobManagerClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="等待 WordCount 窗口输出")
    parser.add_argument("--job-id")
    parser.add_argument("--jobmanager-url", default=DEFAULT_JOBMANAGER_URL)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--timeout", type=float, default=45.0)
    return parser


def wait_for_complete_output(
    client: JobManagerClient,
    *,
    job_id: str,
    output_root: Path,
    timeout: float,
    poll_interval: float = 0.5,
) -> tuple[Path, ...]:
    """等待全部期望 key 写完，避免把首个非空分片误判为完整结果。"""
    deadline = time.monotonic() + timeout
    last_status = "UNKNOWN"
    last_observation = "尚未发现输出文件"
    while time.monotonic() < deadline:
        status = client.status(job_id)
        last_status = str(status.get("status", "UNKNOWN"))
        if last_status in {"FAILED", "REJECTED", "CANCELLED"}:
            raise RuntimeError(f"作业在窗口输出前进入 {last_status}: {status.get('error')}")

        files = output_files(output_root, job_id)
        if files:
            try:
                counts, _ = read_counts(files)
            except (OSError, RuntimeError) as exc:
                # Sink 刷新分片时可能暂时读到不完整行; 下一轮重新读取。
                last_observation = f"输出暂不可完整读取: {exc}"
            else:
                last_observation = f"当前计数 {dict(sorted(counts.items()))}"
                if counts == EXPECTED_WORDCOUNT:
                    return files
        time.sleep(poll_interval)
    raise TimeoutError(
        f"等待完整 WordCount 窗口输出超时; 最后状态 {last_status}; {last_observation}"
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.timeout <= 0:
        raise ValueError("--timeout 必须大于 0")
    job_id = load_job_id(args.output_root, args.job_id)
    client = JobManagerClient(args.jobmanager_url)
    files = wait_for_complete_output(
        client,
        job_id=job_id,
        output_root=args.output_root,
        timeout=args.timeout,
    )
    print(f"job_id={job_id}")
    print("output_files=" + ",".join(str(path) for path in files))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
