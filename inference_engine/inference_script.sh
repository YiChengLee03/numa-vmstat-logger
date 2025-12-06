#!/bin/bash

NUMA_COUNT=$(ls -d /sys/devices/system/node/node* | wc -l)

./dummy_executable_benchmark.sh &
DUMMY_PID=$!

echo "Started dummy_executable_benchmark.sh with PID $DUMMY_PID"
echo "NUMA nodes: $NUMA_COUNT"

while kill -0 "$DUMMY_PID" 2>/dev/null; do
    mode=$(./dummy_model.sh)
    ./change_policy "$DUMMY_PID" "$mode" "$NUMA_COUNT"
    sleep 0.5
done

echo "dummy_executable_benchmark.sh has finished. Exiting."
