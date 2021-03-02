#!/bin/bash

set -euo pipefail

build_dir="./build"
threads=(1 2 4 8 16)
buffer_sizes=(2 8 16 64)
reps=3

scenario_dir=${1:- "./experiments/current/default"}

prefill=$(awk 'NR == 2 {print $1}' ${scenario_dir}/config)
dist=$(awk 'NR == 2 {print $2}' ${scenario_dir}/config)
threads_quality=$(awk 'NR == 2 {print $3}' ${scenario_dir}/config)
if [[ -z ${prefill} || -z ${dist} || -z ${threads_quality} ]]; then
  echo Could not read config
  exit 1
fi

echo Prefill: ${prefill}
echo Distribution: ${dist}
echo Threads in quality benchmark ${threads_quality}

echo Building benchmarks
for i in "${buffer_sizes[@]}"; do
  for j in "${buffer_sizes[@]}"; do
    cmake --build build --target idmq_throughput_benchmark_4_4_${i}_${j}
  done
done
cmake --build build --target evaluate_quality

echo Starting throughput benchmarks
for ins in "${buffer_sizes[@]}"; do
  for del in "${buffer_sizes[@]}"; do
    {
      echo "threads rep throughput"
      for j in "${threads[@]}"; do
        for r in $(seq 1 $reps);do
          echo "sudo ${build_dir}/benchmark/idmq_throughput_benchmark_4_4_${ins}_${del} -j ${j} -n ${prefill} -d ${dist}" >&2
          echo $j $r $(sudo ${build_dir}/benchmark/idmq_throughput_benchmark_4_4_${ins}_${del} -j ${j} -n ${prefill} -d ${dist} 2> /dev/null | tail -n 1 | cut -d' ' -f2)
        done
      done
    } > "${scenario_dir}/throughput/idmq_${ins}_${del}.txt"
  done
done
