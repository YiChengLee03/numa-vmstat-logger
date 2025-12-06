#!/usr/bin/env python3
"""
Run the NUMA-STREAM benchmark multiple times while logging NUMA statistics.

For each run we invoke numa_stat_logger in "-r" mode so it collects data while
the benchmark executes. The logger is started in an isolated working directory,
which keeps each raw CSV separate. After a run finishes we append its samples
to an aggregate CSV that includes two extra columns:
    - mem_policy: textual description of the NUMA policy used for this run
    - stream_run: 1-based run index so samples can be grouped later
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Sequence, Tuple
import re


# Map human friendly policy names to the command prefix that enforces them.
# The default entry means "no numactl policy, inherit the current one".
POLICY_COMMANDS: Dict[str, List[str]] = {
    "default": [],
    "interleave_all": ["numactl", "--interleave=all"],
    "preferred_node0": ["numactl", "--preferred=0"],
}

LOGGER_OUTPUT = "stream_numa_stat_log.csv"
STREAM_HEADER = "Function      Rate (MB/s)   Avg time     Min time     Max time"
STREAM_ROW_RE = re.compile(
    r"^(?P<function>[A-Za-z]+):\s+"
    r"(?P<rate>[0-9.eE+-]+)\s+"
    r"(?P<avg>[0-9.eE+-]+)\s+"
    r"(?P<min>[0-9.eE+-]+)\s+"
    r"(?P<max>[0-9.eE+-]+)"
)
STREAM_FUNCTIONS = ("Copy", "Scale", "Add", "Triad")


def normalize_policies(raw: Sequence[str]) -> List[str]:
    policies: List[str] = []
    for entry in raw:
        for token in entry.split(","):
            token = token.strip()
            if token:
                policies.append(token)
    return policies or ["default"]


def sanitize_label(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_-]+", "_", value).strip("_")
    return sanitized or "policy"


def append_suffix_to_path(path: Path, suffix: str) -> Path:
    suffix_str = "".join(path.suffixes)
    base_name = path.name[: -len(suffix_str)] if suffix_str else path.name
    new_name = f"{base_name}{suffix}{suffix_str}"
    return path.with_name(new_name)


def build_output_suffix(start_run: int, policies: Sequence[str]) -> str:
    label = "-".join(sanitize_label(policy) for policy in policies)
    return f"_start_{start_run}_policy_{label}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run NUMA-STREAM repeatedly while collecting NUMA stats."
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=10,
        help="How many benchmark runs to execute (default: 10).",
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
        "--stream-binary",
        type=Path,
        default=Path("NUMA-STREAM/stream-gcc"),
        help="Path to the compiled NUMA-STREAM binary.",
    )
    parser.add_argument(
        "--logger-binary",
        type=Path,
        default=Path("../src/numa_stat_logger"),
        help="Path to the compiled numa_stat_logger binary.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("stream_run_outputs"),
        help="Directory where per-run logs and the aggregate CSV are stored.",
    )
    parser.add_argument(
        "--aggregate-file",
        type=Path,
        default=Path("stream_run_outputs/stream_raw_log.csv"),
        help="Destination CSV with appended mem_policy and stream_run columns.",
    )
    parser.add_argument(
        "--stream-results-file",
        type=Path,
        default=Path("stream_run_outputs/stream_feature_log.csv"),
        help="CSV file that collects STREAM benchmark summaries per run.",
    )
    return parser.parse_args()


def detect_numa_nodes() -> int:
    node_root = Path("/sys/devices/system/node")
    nodes = sorted(node_root.glob("node[0-9]*"))
    if not nodes:
        raise RuntimeError(f"No NUMA nodes detected under {node_root}")
    return len(nodes)


def ensure_binaries(stream_path: Path, logger_path: Path) -> None:
    if not stream_path.is_file():
        raise FileNotFoundError(
            f"NUMA-STREAM binary not found at {stream_path}. "
            "Build it first (e.g. gcc ... NUMA-STREAM/stream.c -o stream-gcc -lnuma)."
        )
    if not logger_path.is_file():
        raise FileNotFoundError(
            f"numa_stat_logger binary not found at {logger_path}. "
            "Run `make -C src` or `make` from the project root to build it."
        )


def run_once(
    run_index: int,
    policy_name: str,
    interval: float,
    numa_count: int,
    stream_path: Path,
    logger_path: Path,
    run_dir: Path,
) -> Tuple[Path, str]:
    run_dir.mkdir(parents=True, exist_ok=True)
    stream_cmd = [str(stream_path)]

    logger_cmd = [
        str(logger_path),
        str(numa_count),
        str(interval),
        "-r",
        *stream_cmd,
    ]

    print(
        f"[run {run_index}] policy={policy_name} -> collecting NUMA stats ...",
        flush=True,
    )
    result = subprocess.run(
        logger_cmd,
        check=True,
        cwd=run_dir,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, file=sys.stderr, end="")
    return run_dir / LOGGER_OUTPUT, result.stdout or ""


def append_with_metadata(
    run_csv: Path,
    aggregate_csv: Path,
    policy_name: str,
    run_index: int,
) -> None:
    if not run_csv.is_file():
        raise FileNotFoundError(f"Expected logger output at {run_csv}")

    raw_lines = run_csv.read_text(encoding="utf-8").strip().splitlines()
    if not raw_lines:
        print(f"[run {run_index}] warning: {run_csv} is empty, skipping.")
        return

    base_header = raw_lines[0]
    rows = raw_lines[1:]
    augmented_header = f"{base_header},mem_policy,stream_run"

    aggregate_csv.parent.mkdir(parents=True, exist_ok=True)
    if not aggregate_csv.exists():
        aggregate_csv.write_text(augmented_header + "\n", encoding="utf-8")
    else:
        with aggregate_csv.open("r", encoding="utf-8") as existing:
            existing_header = existing.readline().rstrip("\n")
        if existing_header != augmented_header:
            raise RuntimeError(
                f"Aggregate header mismatch.\n"
                f"Existing: {existing_header}\nExpected: {augmented_header}"
            )

    with aggregate_csv.open("a", encoding="utf-8") as dest:
        for row in rows:
            dest.write(f"{row},{policy_name},{run_index}\n")


def parse_stream_results(stream_output: str) -> List[Dict[str, float]]:
    lines = [line.strip() for line in stream_output.splitlines()]
    try:
        start_idx = lines.index(STREAM_HEADER) + 1
    except ValueError:
        return []

    rows: List[Dict[str, float]] = []
    for line in lines[start_idx:]:
        if not line or line.startswith("-"):
            break
        match = STREAM_ROW_RE.match(line)
        if not match:
            continue
        rows.append(
            {
                "function": match.group("function"),
                "rate": float(match.group("rate")),
                "avg": float(match.group("avg")),
                "min": float(match.group("min")),
                "max": float(match.group("max")),
            }
        )
    return rows


def append_stream_results(
    results: Sequence[Dict[str, float]],
    csv_path: Path,
    policy_name: str,
    run_index: int,
) -> None:
    if not results:
        print(f"[run {run_index}] warning: STREAM output missing summary table.")
        return

    header_parts = ["stream_run", "mem_policy"]
    for fn in STREAM_FUNCTIONS:
        prefix = fn.lower()
        header_parts.extend(
            [
                f"{prefix}_rate_mb_s",
                f"{prefix}_avg_time_s",
                f"{prefix}_min_time_s",
                f"{prefix}_max_time_s",
            ]
        )
    header = ",".join(header_parts)

    row_by_function = {row["function"]: row for row in results}
    missing = [fn for fn in STREAM_FUNCTIONS if fn not in row_by_function]
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
    for fn in STREAM_FUNCTIONS:
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
    ensure_binaries(args.stream_binary, args.logger_binary)
    policies = normalize_policies(args.policies)
    suffix = build_output_suffix(args.start_run, policies)
    aggregate_file = append_suffix_to_path(args.aggregate_file, suffix)
    stream_results_file = append_suffix_to_path(
        args.stream_results_file, suffix)

    try:
        numa_nodes = detect_numa_nodes()
    except RuntimeError as exc:
        print(f"Failed to detect NUMA topology: {exc}", file=sys.stderr)
        return 1

    print(
        f"Detected {numa_nodes} NUMA nodes. "
        f"Running {args.runs} STREAM iterations starting at run {args.start_run}.",
        flush=True,
    )

    for offset in range(args.runs):
        run_index = args.start_run + offset
        policy_name = policies[(run_index - 1) % len(policies)]
        run_dir = args.output_dir / f"run_{run_index:02d}"
        run_csv_path, stream_stdout = run_once(
            run_index,
            policy_name,
            args.interval,
            numa_nodes,
            args.stream_binary.resolve(),
            args.logger_binary.resolve(),
            run_dir,
        )
        append_with_metadata(run_csv_path, aggregate_file,
                             policy_name, run_index)
        stream_results = parse_stream_results(stream_stdout)
        append_stream_results(
            stream_results, stream_results_file, policy_name, run_index
        )

    print(f"All runs complete. Aggregated CSV: {aggregate_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
