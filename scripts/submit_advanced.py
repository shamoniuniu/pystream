"""Bundle and submit the advanced exactly-once acceptance job."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from pathlib import Path

from _advanced_demo import DEFAULT_JOBMANAGER_URL, DEFAULT_OUTPUT_ROOT, save_job_id
from _demo import wait_for_workers

from pystream.artifact import build_job_bundle
from pystream.client import JobManagerClient

DEFAULT_JOB_DIR = Path(
    os.getenv(
        "PYSTREAM_ADVANCED_JOB_DIR",
        str(Path(__file__).resolve().parents[1] / "examples" / "advanced"),
    )
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Submit advanced exactly-once job")
    parser.add_argument("--job-dir", type=Path, default=DEFAULT_JOB_DIR)
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path("/tmp/pystream-advanced-artifacts"),
    )
    parser.add_argument("--jobmanager-url", default=DEFAULT_JOBMANAGER_URL)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--worker-timeout", type=float, default=90.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    wait_for_workers(
        args.jobmanager_url,
        expected=3,
        timeout=args.worker_timeout,
    )
    bundle = build_job_bundle(args.job_dir, args.artifact_dir)
    response = JobManagerClient(args.jobmanager_url, timeout=30.0).submit(
        Path(bundle.path).read_bytes(),
        bundle.sha256,
        Path(bundle.path).name,
    )
    job_id = response.get("job_id")
    if not isinstance(job_id, str) or not job_id:
        raise RuntimeError(f"JobManager returned an invalid job_id: {response}")
    if response.get("status") != "RUNNING":
        raise RuntimeError(f"Advanced job did not enter RUNNING: {response}")
    save_job_id(args.output_root, job_id)
    print(f"job_id={job_id}")
    print("status=RUNNING")
    print(f"artifact_sha256={bundle.sha256}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
