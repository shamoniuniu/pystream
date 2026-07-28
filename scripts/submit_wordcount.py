"""打包 WordCount 作业并提交到已经注册 3 个 Worker 的 JobManager。"""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from pathlib import Path

from _demo import (
    DEFAULT_JOBMANAGER_URL,
    DEFAULT_OUTPUT_ROOT,
    save_job_id,
    wait_for_workers,
)

from pystream.artifact import build_job_bundle
from pystream.client import JobManagerClient

DEFAULT_JOB_DIR = Path(
    os.getenv(
        "PYSTREAM_WORDCOUNT_JOB_DIR",
        str(Path(__file__).resolve().parents[1] / "examples" / "wordcount"),
    )
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="提交 PyStream WordCount 作业")
    parser.add_argument(
        "--job-dir",
        type=Path,
        default=DEFAULT_JOB_DIR,
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path("/tmp/pystream-artifacts"),
    )
    parser.add_argument("--jobmanager-url", default=DEFAULT_JOBMANAGER_URL)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--worker-timeout", type=float, default=60.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    wait_for_workers(
        args.jobmanager_url,
        expected=3,
        timeout=args.worker_timeout,
    )
    bundle = build_job_bundle(args.job_dir, args.artifact_dir)
    client = JobManagerClient(args.jobmanager_url, timeout=30.0)
    response = client.submit(
        Path(bundle.path).read_bytes(),
        bundle.sha256,
        Path(bundle.path).name,
    )
    job_id = response.get("job_id")
    status = response.get("status")
    if not isinstance(job_id, str) or not job_id:
        raise RuntimeError(f"JobManager 未返回有效 job_id: {response}")
    if status != "RUNNING":
        raise RuntimeError(f"WordCount 未进入 RUNNING: {response}")
    save_job_id(args.output_root, job_id)
    print(f"job_id={job_id}")
    print(f"status={status}")
    print(f"artifact_sha256={bundle.sha256}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
