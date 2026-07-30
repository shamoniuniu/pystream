"""等待中级演示的 baseline 或故障前恢复阶段输出。"""

from __future__ import annotations

import argparse
import time
from collections import Counter
from collections.abc import Sequence
from pathlib import Path

from _intermediate_demo import (
    BASELINE_ROWS,
    DEFAULT_JOBMANAGER_URL,
    DEFAULT_OUTPUT_ROOT,
    EXPECTED_ROWS,
    load_job_id,
    output_files,
    read_rows,
)

from pystream.client import JobManagerClient

PHASE_EXPECTATIONS = {
    "baseline": BASELINE_ROWS,
    "recovery": EXPECTED_ROWS,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="等待中级演示窗口输出")
    parser.add_argument("--job-id")
    parser.add_argument("--jobmanager-url", default=DEFAULT_JOBMANAGER_URL)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--phase", choices=tuple(PHASE_EXPECTATIONS), required=True)
    parser.add_argument("--timeout", type=float, default=45.0)
    return parser


def wait_for_rows(
    client: JobManagerClient,
    *,
    job_id: str,
    output_root: Path,
    expected: Counter[tuple[str, int, int]],
    timeout: float,
    poll_interval: float = 0.25,
) -> tuple[tuple[Path, ...], Counter[tuple[str, int, int]]]:
    """等待输出多重集包含全部预期行，重复行不阻止阶段完成。"""
    deadline = time.monotonic() + timeout
    last_status = "UNKNOWN"
    observed: Counter[tuple[str, int, int]] = Counter()
    while time.monotonic() < deadline:
        status = client.status(job_id)
        last_status = str(status.get("status", "UNKNOWN"))
        if last_status in {"FAILED", "REJECTED", "CANCELLED"}:
            raise RuntimeError(f"作业在输出完成前进入 {last_status}: {status.get('error')}")
        files = output_files(output_root, job_id)
        if files:
            try:
                observed = read_rows(files)
            except (OSError, RuntimeError):
                pass
            else:
                if not expected - observed:
                    return files, observed
        time.sleep(poll_interval)
    missing = expected - observed
    raise TimeoutError(
        f"等待中级输出超时; 最后状态 {last_status}; "
        f"missing={dict(missing)}; observed={dict(observed)}"
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.timeout <= 0:
        raise ValueError("--timeout 必须大于 0")
    job_id = load_job_id(args.output_root, args.job_id)
    files, observed = wait_for_rows(
        JobManagerClient(args.jobmanager_url),
        job_id=job_id,
        output_root=args.output_root,
        expected=PHASE_EXPECTATIONS[args.phase],
        timeout=args.timeout,
    )
    print(f"job_id={job_id}")
    print(f"phase={args.phase}")
    print("output_files=" + ",".join(str(path) for path in files))
    print(f"observed_rows={dict(observed)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
