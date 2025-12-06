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
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run NUMA-STREAM repeatedly while collecting NUMA stats."
    )
    parser.add_argument(
        "--input_csv",
        type=Path,
        required=True,
        help="Path to the csv input file for the NUMA-STREAM benchmark."
    )
    parser.add_argument(
        "--output_csv",
        type=Path,
        required=True,
        help="Path to store the processed output CSV from the benchmark."
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    df = pd.read_csv(args.input_csv)
    new_df = pd.DataFrame()
    output_csv_path = args.output_csv

    if 'stream_run' not in df.columns:
        raise ValueError("Input CSV is missing required 'stream_run' column")

    new_df['node_0_usage'] = df['node_0_mem_used'] / df['node_0_mem_total']
    new_df['node_1_usage'] = df['node_1_mem_used'] / df['node_1_mem_total']

    if 'mem_policy' not in df.columns:
        raise ValueError("Input CSV is missing required 'mem_policy' column")

    policy = df['mem_policy'].astype(str).str.strip().str.lower()

    first_touch_aliases = {"default",
                           "first_touch", "first-touch", "firsttouch"}
    interleave_aliases = {"interleave", "interleave_all", "interleave-all"}
    preferred0_aliases = {"preferred_node0",
                          "preferred_0", "preferred0", "preferred-0"}
    preferred1_aliases = {"preferred_node1",
                          "preferred_1", "preferred1", "preferred-1"}

    new_df['first-touch'] = policy.isin(first_touch_aliases).astype(int)
    new_df['interleave'] = policy.isin(interleave_aliases).astype(int)
    new_df['preferred_0'] = policy.isin(preferred0_aliases).astype(int)
    new_df['preferred_1'] = policy.isin(preferred1_aliases).astype(int)
    new_df['runs'] = df['stream_run']

    # Compute usage trend for each run (end usage minus beginning usage) and
    # broadcast that per-row so downstream consumers can tell how memory moved.
    runs = df['stream_run']
    if 'timestamp' not in df.columns:
        raise ValueError("Input CSV is missing required 'timestamp' column")
    run_start_time = df.groupby('stream_run')['timestamp'].transform('min')
    new_df['run_timestep'] = df['timestamp'] - run_start_time

    run_usage = new_df.assign(stream_run=runs)
    free_pages = df.groupby('stream_run').agg(
        node_0_free_start=('node_0_nr_free_pages', 'first'),
        node_0_free_end=('node_0_nr_free_pages', 'last'),
        node_1_free_start=('node_1_nr_free_pages', 'first'),
        node_1_free_end=('node_1_nr_free_pages', 'last'),
    )
    free_pages['node_0_free_pages_change'] = free_pages['node_0_free_end'] - \
        free_pages['node_0_free_start']
    free_pages['node_1_free_pages_change'] = free_pages['node_1_free_end'] - \
        free_pages['node_1_free_start']

    if 'numa_pages_migrated' not in df.columns:
        raise ValueError(
            "Input CSV is missing required 'numa_pages_migrated' column")
    migration = df.groupby('stream_run').agg(
        migration_start=('numa_pages_migrated', 'first'),
        migration_end=('numa_pages_migrated', 'last'),
    )
    migration['total_page_migrations'] = (
        migration['migration_end'] - migration['migration_start']
    )

    trends = run_usage.groupby('stream_run').agg(
        node_0_start=('node_0_usage', 'first'),
        node_0_end=('node_0_usage', 'last'),
        node_0_min_usage=('node_0_usage', 'min'),
        node_0_max_usage=('node_0_usage', 'max'),
        node_0_volatility=('node_0_usage', 'std'),
        node_1_start=('node_1_usage', 'first'),
        node_1_end=('node_1_usage', 'last'),
        node_1_min_usage=('node_1_usage', 'min'),
        node_1_max_usage=('node_1_usage', 'max'),
        node_1_volatility=('node_1_usage', 'std'),
    )
    trends['node_0_trend'] = trends['node_0_end'] - trends['node_0_start']
    trends['node_1_trend'] = trends['node_1_end'] - trends['node_1_start']
    new_df['node_0_trend'] = runs.map(trends['node_0_trend'])
    new_df['node_1_trend'] = runs.map(trends['node_1_trend'])

    new_df['node_0_min_usage'] = runs.map(trends['node_0_min_usage'])
    new_df['node_1_min_usage'] = runs.map(trends['node_1_min_usage'])
    new_df['node_0_max_usage'] = runs.map(trends['node_0_max_usage'])
    new_df['node_1_max_usage'] = runs.map(trends['node_1_max_usage'])

    new_df['node_0_volatility'] = runs.map(trends['node_0_volatility'])
    new_df['node_1_volatility'] = runs.map(trends['node_1_volatility'])

    new_df['node_0_free_pages_change'] = runs.map(
        free_pages['node_0_free_pages_change'])
    new_df['node_1_free_pages_change'] = runs.map(
        free_pages['node_1_free_pages_change'])

    new_df['total_page_migrations'] = runs.map(
        migration['total_page_migrations'])

    # Only keep the last row from each run (final sample within each stream run).
    new_df = (
        new_df.groupby('runs', as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )

    column_order = [
        'runs',
        'run_timestep',
        'node_0_usage',
        'node_1_usage',
        'node_0_trend',
        'node_1_trend',
        'node_0_min_usage',
        'node_1_min_usage',
        'node_0_max_usage',
        'node_1_max_usage',
        'node_0_volatility',
        'node_1_volatility',
        'node_0_free_pages_change',
        'node_1_free_pages_change',
        'total_page_migrations',
        'first-touch',
        'interleave',
        'preferred_0',
        'preferred_1',
    ]
    existing_columns = [col for col in column_order if col in new_df.columns]
    remaining_columns = [
        col for col in new_df.columns if col not in existing_columns]
    new_df = new_df[existing_columns + remaining_columns]

    print(new_df.head())

    new_df.to_csv(output_csv_path, index=False)

    return 0


if __name__ == "__main__":
    sys.exit(main())
