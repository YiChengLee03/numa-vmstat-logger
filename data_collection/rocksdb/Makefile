LOGGER_DIR := ../../src
BENCH_SCRIPT := benchmark_script.sh
LOGS_DIR := rocksdb_logs/
LOGGER_BINARY := $(LOGGER_DIR)/numa_stat_logger
RAW_FILE ?= $(LOGS_DIR)/raw_start_1_policy_default.csv
PREPROCESSED_FILE ?= $(LOGS_DIR)/features_start_1_policy_default.csv

all: logger permissions

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
	chmod +x $(LOGGER_BINARY)

run: logger permissions
	python3 rocksdb_logger.py

preprocess:
	@echo "Using RAW=$(RAW)"
	@echo "Using OUT=$(OUT)"
	@test -d .venv || python3 -m venv .venv
	@bash -c " \
		source .venv/bin/activate && \
		pip install --upgrade pip && \
		pip install pandas && \
		python3 preprocess_rocksdb_log.py $(RAW_FILE) $(PREPROCESSED_FILE) \
	"