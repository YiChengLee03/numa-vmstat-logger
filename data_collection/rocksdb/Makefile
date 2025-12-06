LOGGER_DIR := ../../src
BENCH_SCRIPT := benchmark_script.sh
LOGS_DIR := rocksdb_logs/
LOGGER_BINARY := $(LOGGER_DIR)/numa_stat_logger

RUNS ?= 20
START_RUN ?= 1
INTERVAL ?= 0.1
POLICIES ?= default

RAW_FILE ?= $(LOGS_DIR)/raw_start_1_policy_default.csv
PREPROCESSED_FILE ?= $(LOGS_DIR)/features_start_1_policy_default.csv

all: run preprocess

# Clean Logs and Logger Binaries
clean:
	@echo "--- Cleaning logs and source directories ---"
	rm -rf $(LOGS_DIR)
	$(MAKE) -C $(LOGGER_DIR) clean

# Build the numa_stat_logger
logger:
	@echo "--- Building numa_stat_logger in $(LOGGER_DIR) ---"
	$(MAKE) -C $(LOGGER_DIR)

# Set Executable Permissions
permissions:
	@echo "--- Setting executable permissions ---"
	chmod +x $(BENCH_SCRIPT)
	chmod +x $(LOGGER_BINARY)

run: logger permissions
	python3 rocksdb_logger.py --runs $(RUNS) --start_run $(START_RUN) --interval $(INTERVAL) --policies $(POLICIES)

preprocess:
	@echo "Using RAW=$(RAW)"
	@echo "Using OUT=$(OUT)"
	@test -d .venv || python3 -m venv .venv
	@bash -c " \
		source .venv/bin/activate && \
		pip install --upgrade pip && \
		pip install pandas && \
		python3 preprocess_rocksdb_log.py --input_csv $(RAW_FILE) --output_csv $(PREPROCESSED_FILE) \
	"