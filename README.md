# NUMA VM-Stat Logger

This project provides a lightweight C-based NUMA statistics logger for Linux systems. It can log NUMA node memory and VM statistics either for a **fixed duration** or **while a benchmark or other executable is running**.

---

## Files

- **`numa_stat_logger.c`** – C source code for logging NUMA stats to a CSV file.  
- **`Makefile`** – Build and run targets for the logger.  
- **`script.sh`** – Bash wrapper script to detect NUMA nodes and invoke the logger.  
- **`dummy_executable_benchmark.sh`** – Example benchmark that sleeps for testing `-r` mode.

---

## Building

Run:

```bash
make
```

This will compile the C logger:

Usage
1. Fixed Duration Logging
Logs NUMA stats for a fixed duration.

```bash
./script.sh
```

By default, this logs every 100 ms for 10 seconds (you can edit the script for a different duration).
Direct C invocation:

```bash
./numa_stat_logger <numa_count> <interval_sec> -d <duration_sec>
```

Example:

```
./numa_stat_logger 2 0.1 -d 30
```

* 2 → Number of NUMA nodes

* 0.1 → Interval in seconds (100 ms)

* -d 30 → Log for 30 seconds

2. Run Executable While Logging
Logs NUMA stats while an external executable runs.

```
./script.sh ./dummy_executable_benchmark.sh
```

Or directly:

```
./numa_stat_logger 4 0.1 -r ./my_benchmark --arg1 val1
```

* The logger will check every interval_sec and continue logging until the benchmark finishes.

* This works for any executable, including long-running workloads.

CSV Output
Logs are saved to:

```
numa_stat_log.csv
```

Columns include:

* Timestamp

* Node memory info (node_0_mem_total, node_0_mem_used, etc.)

* Node VM stats (nr_free_pages, numa_hit, etc.)

* System-wide VM stats (numa_pte_updates, numa_huge_pte_updates, etc.)

The CSV header is automatically written if the file does not exist.
Makefile Targets

* make – Builds numa_stat_logger.

* make run – Runs fixed-duration logging via script.sh.

* make run_executable – Runs script.sh with the dummy benchmark (or any executable).

Example Test

1. Build the logger:

```
make
```

2. Test fixed duration logging:

```
make run
```

3. Test logging while a benchmark runs:

```
make run_executable
```

Check numa_stat_log.csv to see rows being appended every 100 ms.
Notes

* Requires Linux with /sys/devices/system/node available.

* Works with multi-node NUMA systems.

* Use the INTERVAL variable in script.sh to adjust logging frequency.

* The logger is lightweight: uses a single process, flushes CSV after every row, and sleeps between iterations.