#!/bin/bash

set -euo pipefail

scenario_dir=${1}

mq_c=(2 4 8 16 32)

for c in "${mq_c[@]}"; do
	./benchmark/evaluate.sh ${scenario_dir} "idmq_c_${mq_c}" "idmq_throughput_benchmark_${mq_c}_4_16_16"
done
