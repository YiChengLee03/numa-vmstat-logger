# RocksDB/db_bench benchmarking and logging

This library provides numa-stat logging, db_bench benchmarking and data preprocessing into features for model training

---

## Files

- **`benchmark_script.sh`** - Bash wrapper to execute db_bench command with dynamic parameters
- **`Makefile`** - Build and run targets for RocksDB benchmarking, logging and data preprocessing
- **`preprocess_rocksdb_log.py`** - Python script to preprocess logs into aggregated feature set
- **`rocksdb_logger.py`** - Python wrapper to run and parse db_bench/numa-stat-logger 

---

## Building

Run:

```bash
make
```

This will run benchmark + logging + data preprocessing for all memory policies (e.g. first-touch/default, interleave, preferred_0 and preferred_1)