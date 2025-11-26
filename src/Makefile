all: numa_stat_logger

numa_stat_logger: numa_stat_logger.c
	gcc numa_stat_logger.c -o numa_stat_logger

run: numa_stat_logger
	./script.sh

run_executable: numa_stat_logger
	./script.sh ./dummy_executable_benchmark.sh 