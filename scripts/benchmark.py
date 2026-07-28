"""Run a configurable offline WordCount benchmark with real PyStream operators.

The benchmark exercises Map, KeyBy, HASH Shuffle, keyed tumbling-window Reduce,
REBALANCE, and File Sink without requiring Kafka or Docker. It reports both
throughput and window-result latency, but intentionally has no pass/fail speed
threshold because results depend on the host.
"""

from __future__ import annotations

import argparse
import csv
import ctypes
import json
import os
import platform
import runpy
import shutil
import subprocess
import tempfile
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pystream.api import FileSinkConfig, Partitioning
from pystream.common import RecordEnvelope
from pystream.operators import (
    FileSinkOperator,
    KeyByOperator,
    ManualClock,
    MapOperator,
    OperatorContext,
    ReduceWindowOperator,
)
from pystream.runtime import ShuffleRouter

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = PROJECT_ROOT / "reports" / "offline-benchmark.json"
WINDOW_START = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)


def positive_int(value: str) -> int:
    """Parse a strictly positive command-line integer."""
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    """Build the benchmark command-line parser."""
    parser = argparse.ArgumentParser(description="Run the offline PyStream benchmark")
    parser.add_argument("--records", type=positive_int, default=50_000)
    parser.add_argument("--partitions", type=positive_int, default=2)
    parser.add_argument("--map-parallelism", type=positive_int, default=2)
    parser.add_argument("--key-parallelism", type=positive_int, default=2)
    parser.add_argument("--reduce-parallelism", type=positive_int, default=3)
    parser.add_argument("--sink-parallelism", type=positive_int, default=1)
    parser.add_argument("--window-seconds", type=positive_int, default=300)
    parser.add_argument("--word-cardinality", type=positive_int, default=100)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    return parser


def percentile(values: list[float], probability: float) -> float:
    """Return a linearly interpolated percentile for a non-empty sample."""
    if not values:
        raise ValueError("percentile sample cannot be empty")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def command_version(*command: str) -> str | None:
    """Return one-line tool version output, or None when unavailable."""
    if shutil.which(command[0]) is None:
        return None
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return (result.stdout or result.stderr).strip().splitlines()[0]


def total_memory_bytes() -> int | None:
    """Return host physical memory using only the standard library."""
    if os.name == "nt":

        class MemoryStatus(ctypes.Structure):
            _fields_ = [
                ("length", ctypes.c_ulong),
                ("memory_load", ctypes.c_ulong),
                ("total_physical", ctypes.c_ulonglong),
                ("available_physical", ctypes.c_ulonglong),
                ("total_page_file", ctypes.c_ulonglong),
                ("available_page_file", ctypes.c_ulonglong),
                ("total_virtual", ctypes.c_ulonglong),
                ("available_virtual", ctypes.c_ulonglong),
                ("available_extended_virtual", ctypes.c_ulonglong),
            ]

        status = MemoryStatus()
        status.length = ctypes.sizeof(status)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return int(status.total_physical)
        return None
    try:
        return int(os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE"))
    except (AttributeError, OSError, ValueError):
        return None


def load_wordcount_udfs() -> tuple[Callable[..., Any], Callable[..., Any], Callable[..., Any]]:
    """Load the delivered example UDFs without duplicating benchmark logic."""
    namespace = runpy.run_path(str(PROJECT_ROOT / "examples" / "wordcount" / "wordcount_udfs.py"))
    return namespace["normalize"], namespace["word_key"], namespace["add_counts"]


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    """Execute the offline pipeline and return a structured benchmark report."""
    if args.word_cardinality > args.records:
        raise ValueError("word-cardinality cannot exceed records")

    normalize, word_key, add_counts = load_wordcount_udfs()
    map_operators = [
        MapOperator(OperatorContext("normalize", index), normalize)
        for index in range(args.map_parallelism)
    ]
    key_operators = [
        KeyByOperator(OperatorContext("by_word", index), word_key)
        for index in range(args.key_parallelism)
    ]
    clocks = [ManualClock(WINDOW_START) for _ in range(args.reduce_parallelism)]
    reduce_operators = [
        ReduceWindowOperator(
            OperatorContext("totals", index, clocks[index]),
            add_counts,
            window_size_seconds=args.window_seconds,
        )
        for index in range(args.reduce_parallelism)
    ]
    map_to_key = [
        ShuffleRouter(Partitioning.REBALANCE, args.key_parallelism, index)
        for index in range(args.map_parallelism)
    ]
    key_to_reduce = [
        ShuffleRouter(Partitioning.HASH, args.reduce_parallelism, index)
        for index in range(args.key_parallelism)
    ]
    reduce_to_sink = [
        ShuffleRouter(Partitioning.REBALANCE, args.sink_parallelism, index)
        for index in range(args.reduce_parallelism)
    ]
    started_by_word: dict[str, list[int]] = defaultdict(list)
    reducer_distribution: Counter[int] = Counter()
    latencies_ms: list[float] = []

    with tempfile.TemporaryDirectory(prefix="pystream-benchmark-") as temporary:
        output_root = Path(temporary)
        sink_operators = [
            FileSinkOperator(
                OperatorContext("output", index),
                job_id="benchmark",
                config=FileSinkConfig(
                    connector="file",
                    format="csv",
                    output_path=str(output_root),
                ),
            )
            for index in range(args.sink_parallelism)
        ]
        operators = [*map_operators, *key_operators, *reduce_operators, *sink_operators]
        for operator in operators:
            operator.open()

        benchmark_started = time.perf_counter_ns()
        for index in range(args.records):
            record_started = time.perf_counter_ns()
            source_partition = index % args.partitions
            map_index = source_partition % args.map_parallelism
            raw_word = f"word-{index % args.word_cardinality}"
            word = raw_word.upper() if index % 2 == 0 else raw_word
            record = RecordEnvelope(
                record_id=f"benchmark:{source_partition}:{index}",
                payload={"word": word, "count": 1},
                processing_time=WINDOW_START,
            )
            mapped = map_operators[map_index].process(record)[0]
            key_index = map_to_key[map_index].route(mapped)
            keyed = key_operators[key_index].process(mapped)[0]
            reduce_index = key_to_reduce[key_index].route(keyed)
            reducer_distribution[reduce_index] += 1
            reduce_operators[reduce_index].process(keyed)
            started_by_word[str(keyed.key)].append(record_started)

        window_end = WINDOW_START + timedelta(seconds=args.window_seconds)
        completed_records = 0
        output_rows = 0
        for reduce_index, operator in enumerate(reduce_operators):
            clocks[reduce_index].set(window_end)
            for output in operator.on_timer():
                sink_index = reduce_to_sink[reduce_index].route(output)
                sink_operators[sink_index].process(output)
                completed_records += int(output.payload["count"])
                output_rows += 1
                completed_at = time.perf_counter_ns()
                latencies_ms.extend(
                    (completed_at - started_at) / 1_000_000
                    for started_at in started_by_word[str(output.key)]
                )
        benchmark_finished = time.perf_counter_ns()

        for operator in reversed(operators):
            operator.close()

        csv_completed_records = 0
        csv_rows = 0
        for path in sorted((output_root / "benchmark" / "output").glob("part-*.csv")):
            with path.open(encoding="utf-8", newline="") as stream:
                for row in csv.reader(stream):
                    if len(row) != 3:
                        raise RuntimeError(f"invalid benchmark CSV row in {path}: {row!r}")
                    csv_completed_records += int(row[2])
                    csv_rows += 1

    duration_seconds = (benchmark_finished - benchmark_started) / 1_000_000_000
    count_match = completed_records == csv_completed_records == args.records
    row_match = output_rows == csv_rows == args.word_cardinality
    if not count_match or not row_match or len(latencies_ms) != args.records:
        raise RuntimeError(
            "benchmark correctness check failed: "
            f"input={args.records}, completed={completed_records}, csv={csv_completed_records}, "
            f"rows={output_rows}, csv_rows={csv_rows}, latencies={len(latencies_ms)}"
        )

    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "benchmark": "offline_in_process_wordcount",
        "latency_scope": (
            "record creation through synthetic window trigger and flushed CSV result; "
            "excludes real window wait, Kafka, TCP, Docker, and scheduling"
        ),
        "environment": {
            "os": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor() or None,
            "cpu_count": os.cpu_count(),
            "memory_bytes": total_memory_bytes(),
            "python": platform.python_version(),
            "docker": command_version("docker", "--version"),
            "compose": command_version("docker", "compose", "version"),
        },
        "parameters": {
            "input_records": args.records,
            "kafka_partitions": args.partitions,
            "parallelism": {
                "source": args.partitions,
                "map": args.map_parallelism,
                "key_by": args.key_parallelism,
                "reduce": args.reduce_parallelism,
                "sink": args.sink_parallelism,
            },
            "window_size_seconds": args.window_seconds,
            "word_cardinality": args.word_cardinality,
        },
        "results": {
            "completed_records": completed_records,
            "output_rows": output_rows,
            "duration_seconds": round(duration_seconds, 6),
            "throughput_records_per_second": round(args.records / duration_seconds, 2),
            "p50_latency_ms": round(percentile(latencies_ms, 0.50), 3),
            "p95_latency_ms": round(percentile(latencies_ms, 0.95), 3),
            "error_count": 0,
            "dropped_records": 0,
            "input_output_count_match": count_match,
            "output_row_count_match": row_match,
            "reducer_record_distribution": {
                str(index): reducer_distribution[index] for index in range(args.reduce_parallelism)
            },
        },
        "threshold": None,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Run the benchmark, persist JSON, and print the same report."""
    args = build_parser().parse_args(argv)
    report = run_benchmark(args)
    report_path = args.report.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
