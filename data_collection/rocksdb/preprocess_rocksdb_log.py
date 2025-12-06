#!/usr/bin/env python3

from __future__ import annotations

import argparse
import pandas as pd
from pathlib import Path
import re
import subprocess
import sys
from typing import Dict, List, Sequence, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run RocksDB/db_bench repeatedly while collecting NUMA stats."
    )
    parser.add_argument(
        "--input_csv",
        type=Path,
        required=True,
        help="Path to raw input csv file to be preprocessed into features."
    )
    parser.add_argument(
        "--output_csv",
        type=Path,
        required=True,
        help="Path to store the processed output CSV."
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # Read input
    df = pd.read_csv(args.input_csv)

    # Validate required columns early
    required = ['run_index', 'mem_policy', 'timestamp']
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Input CSV is missing required '{col}' column")

    # Normalize policy names
    policy = df['mem_policy'].astype(str).str.strip().str.lower()

    # --- One-hot encode memory policies ---
    first_touch_aliases = {"default",
                           "first_touch", "first-touch", "firsttouch"}
    interleave_aliases = {"interleave", "interleave_all", "interleave-all"}
    preferred0_aliases = {"preferred_node0",
                          "preferred_0", "preferred0", "preferred-0"}
    preferred1_aliases = {"preferred_node1",
                          "preferred_1", "preferred1", "preferred-1"}

    # Prepare 1-hot columns at full df resolution
    # Take only the last row per run later.
    df['first-touch'] = policy.isin(first_touch_aliases).astype(int)
    df['interleave'] = policy.isin(interleave_aliases).astype(int)
    df['preferred_0'] = policy.isin(preferred0_aliases).astype(int)
    df['preferred_1'] = policy.isin(preferred1_aliases).astype(int)

    # --- Compute per-run aggregates once ---
    # Usage trends and statistics
    usage_temp = pd.DataFrame()
    usage_temp['node_0_usage'] = df['node_0_mem_used'] / df['node_0_mem_total']
    usage_temp['node_1_usage'] = df['node_1_mem_used'] / df['node_1_mem_total']
    usage_temp['run_index'] = df['run_index']

    trends = usage_temp.groupby('run_index').agg(
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

    # Free pages change
    free_pages = df.groupby('run_index').agg(
        node_0_free_start=('node_0_nr_free_pages', 'first'),
        node_0_free_end=('node_0_nr_free_pages', 'last'),
        node_1_free_start=('node_1_nr_free_pages', 'first'),
        node_1_free_end=('node_1_nr_free_pages', 'last'),
    )

    free_pages['node_0_free_pages_change'] = (
        free_pages['node_0_free_end'] - free_pages['node_0_free_start']
    )
    free_pages['node_1_free_pages_change'] = (
        free_pages['node_1_free_end'] - free_pages['node_1_free_start']
    )

    # Total migration
    if 'numa_pages_migrated' not in df.columns:
        raise ValueError(
            "Input CSV is missing required 'numa_pages_migrated' column")

    migration = df.groupby('run_index').agg(
        migration_start=('numa_pages_migrated', 'first'),
        migration_end=('numa_pages_migrated', 'last'),
    )
    migration['total_page_migrations'] = (
        migration['migration_end'] - migration['migration_start']
    )

    # --- Extract only the last row per run ---
    last = df.groupby('run_index').tail(1).reset_index(drop=True)

    # Compute last-sample usage
    last['node_0_usage'] = last['node_0_mem_used'] / last['node_0_mem_total']
    last['node_1_usage'] = last['node_1_mem_used'] / last['node_1_mem_total']

    # Compute run_timestep: last timestamp minus start timestamp
    run_start = df.groupby('run_index')['timestamp'].min()
    last['run_timestep'] = last['timestamp'] - last['run_index'].map(run_start)

    # --- Merge in the per-run aggregates ---
    out = last.merge(trends, on='run_index', how='left')
    out = out.merge(
        free_pages[['node_0_free_pages_change', 'node_1_free_pages_change']],
        on='run_index',
        how='left'
    )
    out = out.merge(
        migration[['total_page_migrations']],
        on='run_index',
        how='left'
    )

    # --- Select and order columns ---
    column_order = [
        'run_index',
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

    out = out[column_order]

    print(out.head())

    out.to_csv(args.output_csv, index=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
