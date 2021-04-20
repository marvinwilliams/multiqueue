#!/bin/bash

set -uo pipefail

hostname=$(cat /proc/sys/kernel/hostname)

bin_dir="./build_${hostname}"

#for pq in ${bin_dir}/tests/*_performance; do
for pq in ${bin_dir}/benchmarks/*_numa_throughput; do
  pq=${pq##*/}
  pq=${pq%%_throughput}
  ./scripts/benchmark_single.sh ${pq}
done

for pq in ${bin_dir}/benchmarks/wrapper_*_throughput; do
  pq=${pq##*/}
  pq=${pq%%_throughput}
  ./scripts/benchmark_single.sh ${pq}
done

for pq in wrapper_capq wrapper_klsm256 wrapper_ksm1024 fullbufferingmq_c_4_k_1_ibs_16_dbs_16_numa fullbufferingmq_c_4_k_8_ibs_16_dbs_16_numa fullbufferingmq_c_8_k_8_ibs_16_dbs_16_numa mergingmq_c_4_k_1_ns_128 mergingmq_c_4_k_8_ns_128_numa; do
	./scripts/benchmark_variants.sh ${pq}
	./scripts/benchmark_sssp.sh ${pq}
done
