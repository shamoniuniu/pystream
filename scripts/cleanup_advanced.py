"""Cancel an advanced acceptance job and optionally remove its output."""

from __future__ import annotations

import argparse
import shutil
from collections.abc import Sequence
from pathlib import Path

from _advanced_demo import (
    DEFAULT_JOBMANAGER_URL,
    DEFAULT_OUTPUT_ROOT,
    load_job_id,
    marker_path,
)

from pystream.client import JobManagerClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Clean up an advanced acceptance job")
    parser.add_argument("--job-id")
    parser.add_argument("--jobmanager-url", default=DEFAULT_JOBMANAGER_URL)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--keep-output", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    job_id = load_job_id(args.output_root, args.job_id)
    client = JobManagerClient(args.jobmanager_url, timeout=30.0)
    status = client.status(job_id)
    if status.get("status") in {"RUNNING", "RECOVERING", "DEPLOYING"}:
        status = client.cancel(job_id)
    if status.get("status") not in {"CANCELLED", "FAILED", "REJECTED"}:
        raise RuntimeError(f"Advanced job did not enter a terminal state: {status}")
    if not args.keep_output:
        shutil.rmtree(args.output_root / job_id, ignore_errors=True)
    marker = marker_path(args.output_root)
    if marker.exists() and marker.read_text(encoding="utf-8").strip() == job_id:
        marker.unlink()
    print(f"job_id={job_id}")
    print(f"status={status.get('status')}")
    print(f"output_removed={not args.keep_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
