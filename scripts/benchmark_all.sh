#!/bin/bash

set -euo pipefail

hostname=$(cat /proc/sys/kernel/hostname)

bin_dir="./build_${hostname}"

for pq in ${bin_dir}/tests/*_performance; do
  pq=${pq##*/}
  pq=${pq%%_performance}
  ./scripts/benchmark_single.sh ${pq}
done
