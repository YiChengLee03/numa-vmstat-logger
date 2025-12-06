#!/usr/bin/env python3

from __future__ import annotations

import argparse
import math
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Dict, List, Sequence, Tuple
from venv import logger

POLICY_COMMANDS: Dict[str, List[str]] = {
    "default": [],
    "interleave_all": ["numactl", "--interleave=all"],
    "preferred_node0": ["numactl", "--preferred=0"],
}

LOGGER_OUTPUT = "rocksdb_numa_stat_log.csv"
ROCKS_DB_FUNCTIONS = (
    "fillrandom", "readseq", "readrandom", "readtocache", "readwhilescanning"
)
ROCKS_DB_ROW_RE = re.compile(
    r"^\s*(?P<function>\w+)\s*:\s*"
    r"(?P<avg>[\d\.]+)\s+micros/op\s+"
    r"(?P<rate>[\d\.]+)\s+ops/sec.*"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run rocksdb/db_bench repeatedly while collecting NUMA stats."
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=100,
        help="How many benchmark runs to execute (default: 100).",
    )
    parser.add_argument(
        "--start-run",
        type=int,
        default=1,
        help="Run index to start from when labeling runs (default: 1).",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.1,
        help="Sampling interval passed to numa_stat_logger (seconds).",
    )
    parser.add_argument(
        "--policies",
        nargs="+",
        default=["default"],
        type=str,
        help=(
            "Sequence of NUMA policies to cycle through. Provide one or more names "
            "(default: 'default')."
        ),
    )
    parser.add_argument(
        "--db-bench-binary",
        type=Path,
        default=Path("~/rocksdb/db_bench"),
        help="Path to the compiled db_bench binary.",
    )
    parser.add_argument(
        "--db-bench-helper-script",
        type=Path,
        default=Path(
            "~/numa-vmstat-logger/data_collection/rocksdb/benchmark_script.sh"),
        help="Path to the helper benchmark script.",
    )
    parser.add_argument(
        "--logger-binary",
        type=Path,
        default=Path("~/numa-vmstat-logger/src/numa_stat_logger"),
        help="Path to the compiled numa_stat_logger binary.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("rocksdb_logs"),
        help="Directory where logs are stored.",
    )
    parser.add_argument(
        "--raw-file",
        type=Path,
        default=Path("rocksdb_logs/raw.csv"),
        help="Destination logs with mem_policy and run index columns.",
    )
    parser.add_argument(
        "--preprocessed-file",
        type=Path,
        default=Path("rocksdb_logs/features.csv"),
        help="CSV file that collects STREAM benchmark summaries per run.",
    )
    return parser.parse_args()


def flatten_policies(raw_policies: Sequence[str]) -> List[str]:
    policies: List[str] = []
    for entry in raw_policies:
        for token in entry.split(","):
            token = token.strip()
            if token:
                policies.append(token)
    return policies or ["default"]


def build_output_file_suffix(start_run: int, policies: Sequence[str]) -> str:
    def sanitize_label(value: str) -> str:
        sanitized = re.sub(r"[^A-Za-z0-9_-]+", "_", value).strip("_")
        return sanitized or "policy"

    label = "_".join(sanitize_label(policy) for policy in policies)
    return f"_start_{start_run}_policy_{label}"


def append_suffix_to_path(path: Path, suffix: str) -> Path:
    suffix_str = "".join(path.suffixes)
    base_name = path.name[: -len(suffix_str)] if suffix_str else path.name
    new_name = f"{base_name}{suffix}{suffix_str}"
    return path.with_name(new_name)


def detect_numa_nodes() -> int:
    command = "ls -d /sys/devices/system/node/node* | wc -l"

    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )
        count_str = result.stdout.strip()
        if not count_str:
            return 0
        return int(count_str)

    except subprocess.CalledProcessError as e:
        print(
            f"Error running NUMA detection command (Exit Status {e.returncode}):")
        print(f"Stderr: {e.stderr.strip()}")
        return None
    except FileNotFoundError:
        print("System shell or necessary command ('ls', 'wc') not found.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


def ensure_binaries(db_bench_path: Path, logger_path: Path) -> None:
    expanded_db_bench_path = db_bench_path.expanduser()
    expanded_logger_path = logger_path.expanduser()

    if not expanded_db_bench_path.is_file():
        raise FileNotFoundError(
            f"rocksdb/db_bench binary not found at {db_bench_path}. "
            "Build it first (e.g. make db_bench -j$(nproc))."
        )
    if not expanded_logger_path.is_file():
        raise FileNotFoundError(
            f"numa_stat_logger binary not found at {logger_path}. "
            "Run `make -C src` or `make` from the project root to build it."
        )

    if not os.access(expanded_db_bench_path, os.X_OK):
        raise PermissionError(
            f"db_bench found, but lacks execute permission: {expanded_db_bench_path}.")
    if not os.access(expanded_logger_path, os.X_OK):
        raise PermissionError(
            f"logger found, but lacks execute permission: {expanded_logger_path}.")


def generate_num_intervals(
    num_intervals: int = 8,
    start: int = 10_000,
    end: int = 5_000_000,
) -> List[int]:
    if start <= 0 or end <= 0 or start >= end or num_intervals < 2:
        return [start, end]

    # Calculate the step size in logarithmic space
    log_start = math.log(start)
    log_end = math.log(end)
    log_step = (log_end - log_start) / (num_intervals - 1)

    num_list: List[int] = []
    for i in range(num_intervals):
        log_value = log_start + i * log_step
        # Convert back to linear space and round to the nearest integer
        value = int(round(math.exp(log_value)))
        num_list.append(value)

    num_list[0] = start
    num_list[-1] = end

    return num_list


def run_benchmark_and_logger(
    run_index: int,
    policy_name: str,
    interval: float,
    numa_count: int,
    db_bench_helper_path: Path,
    db_bench_num_iter: int,
    logger_path: Path,
    run_dir: Path,
) -> Tuple[Path, str]:
    run_dir.mkdir(parents=True, exist_ok=True)
    db_bench_helper_script = [
        str(db_bench_helper_path.resolve()), str(db_bench_num_iter)]

    logger_cmd = [
        str(logger_path),
        str(numa_count),
        str(interval),
        "-r",
        *db_bench_helper_script
    ]

    print(
        f"[run {run_index}] policy={policy_name} -> collecting NUMA stats ...",
        flush=True,
    )

    try:
        result = subprocess.run(
            logger_cmd,
            shell=True,
            check=True,
            cwd=run_dir,
            capture_output=True,
            text=True,
        )

        return run_dir / LOGGER_OUTPUT, result.stdout

    except FileNotFoundError:
        print("One of the commands ('ls' or 'wc') was not found.")
    except Exception as e:
        print(e, file=sys.stderr, end="")

    return run_dir / LOGGER_OUTPUT, ""


def append_with_metadata(
    run_file: Path,
    raw_file: Path,
    policy_name: str,
    run_index: int,
) -> None:
    if not run_file.is_file():
        raise FileNotFoundError(f"Expected logger output at {run_file}")

    raw_lines = run_file.read_text(encoding="utf-8").strip().splitlines()
    if not raw_lines:
        print(f"[run {run_index}] warning: {run_file} is empty, skipping.")
        return

    base_header = raw_lines[0]
    rows = raw_lines[1:]
    augmented_header = f"{base_header},mem_policy,run_index"

    raw_file.parent.mkdir(parents=True, exist_ok=True)
    if not raw_file.exists():
        raw_file.write_text(augmented_header + "\n", encoding="utf-8")
    else:
        with raw_file.open("r", encoding="utf-8") as existing:
            existing_header = existing.readline().rstrip("\n")
        if existing_header != augmented_header:
            raise RuntimeError(
                f"Aggregate header mismatch.\n"
                f"Existing: {existing_header}\nExpected: {augmented_header}"
            )

    with raw_file.open("a", encoding="utf-8") as dest:
        for row in rows:
            dest.write(f"{row},{policy_name},{run_index}\n")


def parse_benchmark_results(db_bench_output: str) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []

    lines = [line.strip() for line in db_bench_output.splitlines()]

    for line in lines:
        # Skip empty lines, separators, and non-data lines (like DB path)
        if not line or line.startswith("-") or line.startswith("DB path:"):
            continue

        match = ROCKS_DB_ROW_RE.match(line)
        if not match:
            continue

        # Append the structured dictionary, mapping log fields to required keys
        rows.append(
            {
                "function": match.group("function"),
                "rate": float(match.group("rate")),
                "avg": float(match.group("avg")),
            }
        )

    return rows


def append_benchmark_results(
    results: Sequence[Dict[str, float]],
    csv_path: Path,
    policy_name: str,
    run_index: int,
) -> None:
    if not results:
        print(
            f"[run {run_index}] warning: DB_BENCH output missing summary table.")
        return

    header_parts = ["run_index", "mem_policy"]
    for fn in ROCKS_DB_FUNCTIONS:
        prefix = fn.lower()
        header_parts.extend(
            [
                f"{prefix}_rate_ops_s",
                f"{prefix}_avg_ms_ops",
            ]
        )
    header = ",".join(header_parts)

    row_by_function = {row["function"]: row for row in results}
    missing = [fn for fn in ROCKS_DB_FUNCTIONS if fn not in row_by_function]
    if missing:
        print(
            f"[run {run_index}] warning: STREAM output missing entries for: "
            f"{', '.join(missing)}"
        )

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if not csv_path.exists():
        csv_path.write_text(header + "\n", encoding="utf-8")
    else:
        with csv_path.open("r", encoding="utf-8") as fh:
            existing_header = fh.readline().rstrip("\n")
        if existing_header != header:
            raise RuntimeError(
                f"STREAM metrics header mismatch.\n"
                f"Existing: {existing_header}\nExpected: {header}"
            )

    row_values: List[str] = [str(run_index), policy_name]
    for fn in ROCKS_DB_FUNCTIONS:
        row = row_by_function.get(fn)
        if not row:
            row_values.extend(["", "", "", ""])
            continue
        row_values.extend(
            [
                f"{row['rate']:.6f}",
                f"{row['avg']:.9f}",
                f"{row['min']:.9f}",
                f"{row['max']:.9f}",
            ]
        )

    with csv_path.open("a", encoding="utf-8") as fh:
        fh.write(",".join(row_values) + "\n")


def main() -> int:
    args = parse_args()

    ensure_binaries(args.db_bench_binary, args.logger_binary)
    policies = flatten_policies(args.policies)
    suffix = build_output_file_suffix(args.start_run, policies)

    raw_file = append_suffix_to_path(args.raw_file, suffix)
    preprocessed_file = append_suffix_to_path(
        args.preprocessed_file, suffix)

    try:
        numa_nodes = detect_numa_nodes()
    except RuntimeError as e:
        print(f"Failed to detect NUMA topology: {e}", file=sys.stderr)
        return 1

    print(
        f"Detected {numa_nodes} NUMA nodes. "
        f"Running {args.runs} STREAM iterations starting at run {args.start_run}.",
        flush=True,
    )

    db_bench_num_iter = generate_num_intervals(args.runs)

    for offset in range(args.runs):
        run_index = args.start_run + offset
        policy_name = policies[(run_index - 1) % len(policies)]
        run_dir = args.output_dir / f"run_{run_index:02d}"

        run_csv_path, db_bench_stdout = run_benchmark_and_logger(
            run_index,
            policy_name,
            args.interval,
            numa_nodes,
            args.db_bench_helper_script.expanduser(),
            db_bench_num_iter[offset],
            args.logger_binary.expanduser(),
            run_dir,
        )
        append_with_metadata(run_csv_path, raw_file, policy_name, run_index)
        benchmark_results = parse_benchmark_results(db_bench_stdout)
        append_benchmark_results(
            benchmark_results, preprocessed_file, policy_name, run_index)

    print(f"All runs complete. Raw CSV: {raw_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
