LOGGER_DIR := ../../src
BENCH_SCRIPT := benchmark_script.sh
LOGS_DIR := rocksdb_logs/
LOGGER_BINARY := $(LOGGER_DIR)/numa_stat_logger

all: permissions logger

# 1. Clean Logs and Logger Binaries
clean:
	@echo "--- Cleaning logs and source directories ---"
	rm -rf $(LOGS_DIR)
	$(MAKE) -C $(LOGGER_DIR) clean

# 2. Build the numa_stat_logger
logger:
	@echo "--- Building numa_stat_logger in $(LOGGER_DIR) ---"
	$(MAKE) -C $(LOGGER_DIR)
	chmod +x $(LOGGER_BINARY)

# 3. Set Executable Permissions
permissions:
	@echo "--- Setting executable permissions ---"
	chmod +x $(BENCH_SCRIPT)

run: BENCH_SCRIPT LOGGER_BINARY
	python3 rocksdb_logger.py