LOGGER_DIR := ../../src
BENCH_SCRIPT := benchmark_script.sh
LOGS_DIR := rocksdb_logs/
LOGGER_BINARY := $(LOGGER_DIR)/numa_stat_logger

RUNS ?= 20
START_RUN ?= 1
INTERVAL ?= 0.1
POLICIES ?= default

RAW_FILE ?= $(LOGS_DIR)/raw_start_$(START_RUN)_policy_default.csv
PREPROCESSED_FILE ?= $(LOGS_DIR)/features_start_$(START_RUN)_policy_default.csv

.PHONY: clean logger permissions run preprocess \
        interleave preferred_0 preferred_1 default_policy all_policies

########################################
# Run all policies
########################################

all_policies:
	@echo "=== Running default ==="
	make default_policy

	@echo "=== Running interleave ==="
	make interleave

	@echo "=== Running preferred_0 ==="
	make preferred_0

	@echo "=== Running preferred_1 ==="
	make preferred_1

########################################
# Core build + run rules
########################################

# Clean logs and binaries
clean:
	@echo "--- Cleaning logs and source directories ---"
	rm -rf $(LOGS_DIR)
	$(MAKE) -C $(LOGGER_DIR) clean

# Build numa_stat_logger
logger:
	@echo "--- Building numa_stat_logger in $(LOGGER_DIR) ---"
	$(MAKE) -C $(LOGGER_DIR)

# Set executable permissions
permissions:
	@echo "--- Setting executable permissions ---"
	chmod +x $(BENCH_SCRIPT)
	chmod +x $(LOGGER_BINARY)

# Run benchmark + logging
run: logger permissions
	python3 rocksdb_logger.py \
		--runs $(RUNS) \
		--start-run $(START_RUN) \
		--interval $(INTERVAL) \
		--policies $(POLICIES)

# Preprocess logged CSV
preprocess:
	@echo "Using RAW=$(RAW_FILE)"
	@echo "Using OUT=$(PREPROCESSED_FILE)"
	@test -d .venv || python3 -m venv .venv
	@bash -c " \
		source .venv/bin/activate && \
		pip install --upgrade pip && \
		pip install pandas && \
		python3 preprocess_rocksdb_log.py --input_csv $(RAW_FILE) --output_csv $(PREPROCESSED_FILE) \
	"

########################################
# NUMA policy convenience targets
########################################

default_policy:
	make run POLICIES=default \
	     RAW_FILE=$(LOGS_DIR)/raw_start_$(START_RUN)_policy_default.csv
	make preprocess PREPROCESSED_FILE=$(LOGS_DIR)/features_start_$(START_RUN)_policy_default.csv \
	                RAW_FILE=$(LOGS_DIR)/raw_start_$(START_RUN)_policy_default.csv

interleave:
	numactl --interleave=all \
	make run POLICIES=interleave \
	     RAW_FILE=$(LOGS_DIR)/raw_start_$(START_RUN)_policy_interleave.csv
	make preprocess PREPROCESSED_FILE=$(LOGS_DIR)/features_start_$(START_RUN)_policy_interleave.csv \
	                RAW_FILE=$(LOGS_DIR)/raw_start_$(START_RUN)_policy_interleave.csv

preferred_0:
	numactl --preferred=0 \
	make run POLICIES=preferred_0 \
	     RAW_FILE=$(LOGS_DIR)/raw_start_$(START_RUN)_policy_preferred_0.csv
	make preprocess PREPROCESSED_FILE=$(LOGS_DIR)/features_start_$(START_RUN)_policy_preferred_0.csv \
	                RAW_FILE=$(LOGS_DIR)/raw_start_$(START_RUN)_policy_preferred_0.csv

preferred_1:
	numactl --preferred=1 \
	make run POLICIES=preferred_1 \
	     RAW_FILE=$(LOGS_DIR)/raw_start_$(START_RUN)_policy_preferred_1.csv
	make preprocess PREPROCESSED_FILE=$(LOGS_DIR)/features_start_$(START_RUN)_policy_preferred_1.csv \
	                RAW_FILE=$(LOGS_DIR)/raw_start_$(START_RUN)_policy_preferred_1.csv
