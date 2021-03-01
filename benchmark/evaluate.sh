#!/bin/bash

set -euo pipefail

build_dir="./build"
threads=(1 2 4 8 16)
reps=5

scenario_dir=${1:- "./experiments/current/default"}
pq="$2"
name=${3:-$pq}

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

echo Building consistency test
cmake --build build --target ${pq}_consistency_test
cmake --build build --target verify_consistency

echo Verifying consistency of pq
echo [1]
${build_dir}/tests/${pq}_consistency_test -j 1 -n 0 -o 100 2> /dev/null | ${build_dir}/tests/verify_consistency
echo [2]
${build_dir}/tests/${pq}_consistency_test -j 1 2> /dev/null | ${build_dir}/tests/verify_consistency
echo [3]
${build_dir}/tests/${pq}_consistency_test -j 8 2> /dev/null | ${build_dir}/tests/verify_consistency

echo Building benchmarks
cmake --build build --target ${pq}_quality_benchmark
cmake --build build --target ${pq}_throughput_benchmark
cmake --build build --target evaluate_quality

echo Starting throughput benchmarks
{
echo "threads rep throughput"
for j in "${threads[@]}"; do
  for r in $(seq 1 $reps);do
    echo "sudo ${build_dir}/benchmark/${pq}_throughput_benchmark -j ${j} -n ${prefill} -d ${dist}" >&2
    echo $j $r $(sudo ${build_dir}/benchmark/${pq}_throughput_benchmark -j ${j} -n ${prefill} -d ${dist} 2> /dev/null | tail -n 1 | cut -d' ' -f2)
  done
done
} > "${scenario_dir}/throughput/${name}.txt"

echo Starting quality benchmark
${build_dir}/benchmark/${pq}_quality_benchmark -j ${threads_quality} -n ${prefill} -d ${dist} 2> /dev/null | tee "${scenario_dir}/logs/${name}.txt" | ${build_dir}/benchmark/evaluate_quality -r "${scenario_dir}/rank/${name}.txt" -d "${scenario_dir}/delay/${name}.txt" -t "${scenario_dir}/top_delay/${name}.txt"
