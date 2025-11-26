#!/bin/bash

NUMA_COUNT=$(ls -d /sys/devices/system/node/node* | wc -l)
INTERVAL=0.1

if [ "$#" -ge 1 ]; then
    # Run benchmark while logging
    ./numa_stat_logger "$NUMA_COUNT" "$INTERVAL" -r "$@"
else
    # Fixed duration example: 30s
    ./numa_stat_logger "$NUMA_COUNT" "$INTERVAL" -d 10
fi
