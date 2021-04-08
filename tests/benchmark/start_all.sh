#!/bin/bash

set -euo pipefail

hostname=$(cat /proc/sys/kernel/hostname)

for pq in capq nbmq dbmq idmq lqmq mmq namq nammq;do
        for scenario in default;do
                for setting in experiments_${hostname}/${scenario}/*/;do
                        ./benchmark/evaluate.sh ${setting} ${pq} ${pq}_throughput_benchmark ${pq}_quality_benchmark
                done
        done
done
