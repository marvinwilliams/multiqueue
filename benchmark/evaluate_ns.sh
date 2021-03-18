#!/bin/bash

set -euo pipefail

scenario_dir=${1}

m_ns=(2 4 8 16)

for ns in "${m_ns[@]}"; do
	./benchmark/evaluate.sh ${scenario_dir} "mmq_nodesize_${ns}" "mmq_throughput_benchmark_${ns}"
done
