"""打包并提交中级事件时间、Retract 和 Checkpoint 演示作业。"""

from __future__ import annotations

import argparse
import os
import shutil
import tempfile
from collections.abc import Sequence
from pathlib import Path

import yaml
from _demo import wait_for_workers
from _intermediate_demo import (
    DEFAULT_JOBMANAGER_URL,
    DEFAULT_OUTPUT_ROOT,
    save_job_id,
)

from pystream.artifact import build_job_bundle
from pystream.client import JobManagerClient

DEFAULT_JOB_DIR = Path(
    os.getenv(
        "PYSTREAM_INTERMEDIATE_JOB_DIR",
        str(Path(__file__).resolve().parents[1] / "examples" / "intermediate"),
    )
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="提交 PyStream 中级演示作业")
    parser.add_argument("--job-dir", type=Path, default=DEFAULT_JOB_DIR)
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path("/tmp/pystream-intermediate-artifacts"),
    )
    parser.add_argument("--jobmanager-url", default=DEFAULT_JOBMANAGER_URL)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--worker-timeout", type=float, default=60.0)
    parser.add_argument(
        "--checkpoint-interval",
        help="仅为本次提交覆盖 Checkpoint 周期, 例如 60s",
    )
    return parser


def stage_job_with_checkpoint_interval(
    job_dir: Path,
    staging_root: Path,
    checkpoint_interval: str,
) -> Path:
    """复制演示作业并仅在临时副本中覆盖 Checkpoint 周期。"""
    staged_job_dir = staging_root / "job"
    shutil.copytree(job_dir, staged_job_dir)
    job_path = staged_job_dir / "job.yaml"
    document = yaml.safe_load(job_path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise RuntimeError("中级 job.yaml 顶层必须是映射")
    execution = document.get("execution")
    checkpoint = execution.get("checkpoint") if isinstance(execution, dict) else None
    if not isinstance(checkpoint, dict):
        raise RuntimeError("中级 job.yaml 缺少 execution.checkpoint")
    checkpoint["interval"] = checkpoint_interval
    job_path.write_text(
        yaml.safe_dump(document, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return staged_job_dir


def build_submission(
    job_dir: Path,
    artifact_dir: Path,
    checkpoint_interval: str | None,
) -> tuple[bytes, str, str]:
    """构建提交载荷，必要时使用临时作业副本覆盖 Checkpoint 周期。"""
    if checkpoint_interval is None:
        bundle = build_job_bundle(job_dir, artifact_dir)
        return (
            Path(bundle.path).read_bytes(),
            bundle.sha256,
            Path(bundle.path).name,
        )

    with tempfile.TemporaryDirectory(prefix="pystream-intermediate-") as temporary:
        staged_job_dir = stage_job_with_checkpoint_interval(
            job_dir,
            Path(temporary),
            checkpoint_interval,
        )
        bundle = build_job_bundle(staged_job_dir, artifact_dir)
        return (
            Path(bundle.path).read_bytes(),
            bundle.sha256,
            Path(bundle.path).name,
        )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    wait_for_workers(
        args.jobmanager_url,
        expected=3,
        timeout=args.worker_timeout,
    )
    payload, artifact_sha256, artifact_name = build_submission(
        args.job_dir,
        args.artifact_dir,
        args.checkpoint_interval,
    )
    client = JobManagerClient(args.jobmanager_url, timeout=30.0)
    response = client.submit(
        payload,
        artifact_sha256,
        artifact_name,
    )
    job_id = response.get("job_id")
    status = response.get("status")
    if not isinstance(job_id, str) or not job_id:
        raise RuntimeError(f"JobManager 未返回有效 job_id: {response}")
    if status != "RUNNING":
        raise RuntimeError(f"中级作业未进入 RUNNING: {response}")
    save_job_id(args.output_root, job_id)
    print(f"job_id={job_id}")
    print(f"status={status}")
    print(f"artifact_sha256={artifact_sha256}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
