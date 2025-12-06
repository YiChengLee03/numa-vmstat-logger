#!/bin/bash

# --- Configuration ---

# 1. Number of Key/Value pairs (defaults to 1000)
# Use $1 if provided, otherwise default to 1000.
NUM=${1:-1000}

# 2. Number of threads (defaults to the number of processor cores)
# Use $2 if provided, otherwise default to the output of nproc.
THREADS=${2:-$(nproc)}

# Check if nproc returned 0 or failed; ensure threads is at least 1.
if [ "$THREADS" -lt 1 ]; then
    THREADS=1
fi

# Define the path to the db_bench executable
DB_BENCH_EXEC=~/rocksdb/db_bench

echo "--- Running RocksDB Benchmarks ---"
echo "NUM Operations (--num): $NUM"
echo "Threads (--threads): $THREADS"
echo "----------------------------------"

# Execute the db_bench command with dynamic parameters
$DB_BENCH_EXEC \
    --num_levels=6 --key_size=20 \
    --prefix_size=20 --keys_per_prefix=0 --value_size=100 \
    --cache_size=17179869184 --cache_numshardbits=6 \
    --compression_type=none --compression_ratio=1 \
    --min_level_to_compress=-1 --disable_seek_compaction=1 \
    --write_buffer_size=134217728 --max_write_buffer_number=2 --level0_file_num_compaction_trigger=8 \
    --target_file_size_base=134217728 --max_bytes_for_level_base=1073741824 \
    --disable_wal=1 --sync=0 --verify_checksum=1 --delete_obsolete_files_period_micros=314572800 \
    --max_background_compactions=4 --max_background_flushes=0 --level0_slowdown_writes_trigger=16 \
    --level0_stop_writes_trigger=24 \
    --statistics=0 --histogram=0 --stats_per_interval=0 --stats_interval=0\
    --use_plain_table=1 --open_files=-1 --mmap_read=1 --mmap_write=0 --memtablerep=prefix_hash \
    --bloom_bits=10 --bloom_locality=1 \
    --benchmarks="fillrandom,readseq,readrandom,readtocache,readwhilescanning" --use_existing_db=0 \
    --num="$NUM" \
    --threads="$THREADS" --allow_concurrent_memtable_write=false