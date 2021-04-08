#!/bin/bash

set -euo pipefail

scenario_dir=${1}

buffer_sizes=(2 8 16 64 128 1024)

for ins in "${buffer_sizes[@]}"; do
	for del in "${buffer_sizes[@]}"; do
		./benchmark/evaluate.sh "${scenario_dir}" "idmq_buf_size_${ins}_${del}" "idmq_throughput_benchmark_4_4_${ins}_${del}"
	done
done
