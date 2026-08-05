"""Shared deterministic data and manifest readers for advanced acceptance."""

from __future__ import annotations

import csv
import hashlib
import json
import os
from collections import Counter
from pathlib import Path

from _demo import DEFAULT_JOBMANAGER_URL, DEFAULT_OUTPUT_ROOT

DEFAULT_BOOTSTRAP_SERVERS = os.getenv(
    "PYSTREAM_KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
DEFAULT_TOPIC = "advanced-words"
JOB_ID_MARKER = ".last_advanced_job_id"
EXPECTED_ROWS = Counter(
    {
        ("2026/07/29T00:00:05", 1, 1): 1,
        ("2026/07/29T00:00:05", 2, 1): 1,
    }
)


def marker_path(output_root: Path) -> Path:
    return output_root / JOB_ID_MARKER


def save_job_id(output_root: Path, job_id: str) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    marker = marker_path(output_root)
    temporary = marker.with_suffix(".tmp")
    temporary.write_text(f"{job_id}\n", encoding="utf-8")
    temporary.replace(marker)


def load_job_id(output_root: Path, explicit: str | None = None) -> str:
    if explicit:
        return explicit
    marker = marker_path(output_root)
    try:
        job_id = marker.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise RuntimeError(f"Cannot read advanced job marker {marker}: {exc}") from exc
    if not job_id:
        raise RuntimeError(f"Advanced job marker is empty: {marker}")
    return job_id


def visible_rows(
    output_root: Path,
    job_id: str,
) -> tuple[Counter[tuple[str, int, int]], tuple[dict[str, object], ...]]:
    """Read only fragments referenced by immutable output manifests."""
    operator_root = output_root / job_id / "output"
    manifest_paths = tuple(sorted((operator_root / "manifests").glob("checkpoint-*.json")))
    if not manifest_paths:
        raise RuntimeError(f"No output manifests found for job {job_id}")
    rows: Counter[tuple[str, int, int]] = Counter()
    manifests: list[dict[str, object]] = []
    seen_fragments: set[str] = set()
    for manifest_path in manifest_paths:
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(document, dict):
            raise RuntimeError(f"Output manifest must be an object: {manifest_path}")
        if document.get("job_id") != job_id or document.get("operator_id") != "output":
            raise RuntimeError(f"Output manifest identity mismatch: {manifest_path}")
        raw_fragments = document.get("fragments")
        if not isinstance(raw_fragments, list) or not raw_fragments:
            raise RuntimeError(f"Output manifest has no fragments: {manifest_path}")
        for fragment in raw_fragments:
            if not isinstance(fragment, dict):
                raise RuntimeError(f"Output fragment entry must be an object: {manifest_path}")
            relative_path = fragment.get("relative_path")
            expected_sha = fragment.get("sha256")
            expected_size = fragment.get("size")
            if (
                not isinstance(relative_path, str)
                or relative_path in seen_fragments
                or not isinstance(expected_sha, str)
                or isinstance(expected_size, bool)
                or not isinstance(expected_size, int)
            ):
                raise RuntimeError(f"Invalid or duplicate fragment entry: {fragment!r}")
            seen_fragments.add(relative_path)
            path = (output_root / relative_path).resolve()
            if not path.is_relative_to(output_root.resolve()) or not path.is_file():
                raise RuntimeError(f"Manifest fragment is unavailable: {relative_path}")
            content = path.read_bytes()
            if len(content) != expected_size or hashlib.sha256(content).hexdigest() != expected_sha:
                raise RuntimeError(f"Manifest fragment identity mismatch: {relative_path}")
            with path.open("r", encoding="utf-8", newline="") as stream:
                for row_number, row in enumerate(csv.reader(stream), start=1):
                    if len(row) != 3:
                        raise RuntimeError(f"{path}:{row_number} must contain 3 columns")
                    window_end, raw_count, raw_word_count = row
                    try:
                        count = int(raw_count)
                        word_count = int(raw_word_count)
                    except ValueError as exc:
                        raise RuntimeError(
                            f"{path}:{row_number} contains non-integer output"
                        ) from exc
                    rows[(window_end, count, word_count)] += 1
        manifests.append(document)
    return rows, tuple(manifests)


def pending_transaction_count(output_root: Path, job_id: str) -> int:
    pending = output_root / job_id / "output" / "pending"
    return sum(1 for path in pending.glob("attempt-*/tx-*") if path.is_dir())


def canonical_rows(rows: Counter[tuple[str, int, int]]) -> list[dict[str, object]]:
    return [
        {
            "window_end": row[0],
            "count": row[1],
            "word_count": row[2],
            "occurrences": occurrences,
        }
        for row, occurrences in sorted(rows.items())
    ]


__all__ = [
    "DEFAULT_BOOTSTRAP_SERVERS",
    "DEFAULT_JOBMANAGER_URL",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_TOPIC",
    "EXPECTED_ROWS",
    "canonical_rows",
    "load_job_id",
    "marker_path",
    "pending_transaction_count",
    "save_job_id",
    "visible_rows",
]
