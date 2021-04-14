#!/bin/bash

set -euo pipefail

hostname=$(cat /proc/sys/kernel/hostname)
bin_dir="./build_${hostname}"
reps=5
timeout=3

name="$1"

experiment_dir="${HOME}/experiments/${hostname}/multiqueue"
log_dir="${experiment_dir}/logs/${name}"
result_dir="${experiment_dir}/results/${name}"
quality_bin="${bin_dir}/tests/${name}_performance"
throughput_bin="${bin_dir}/tests/benchmark/${name}_throughput"

mkdir -p ${log_dir}
mkdir -p ${result_dir}

if [[ -x ${quality_bin} ]]; then
  for ((j=1;j<=$(nproc);j=2*j)); do
    echo Starting quality benchmark >&2
    echo "${quality_bin} -j ${j} -t 200" >&2
    ${quality_bin} -j ${j} -t 200 > "${log_dir}/performance_${j}_stdout.txt" 2>  "${log_dir}/performance_${j}_stderr.txt"
    ${bin_dir}/tests/evaluate_quality -r "${result_dir}/rank_${j}.txt" -d "${result_dir}/delay_${j}.txt" -t "${result_dir}/top_delay_${j}.txt" < "${log_dir}/performance_${j}_stdout.txt" >  "${log_dir}/evaluate_quality_${j}_stdout.txt" 2> "${log_dir}/evaluate_quality_${j}_stderr.txt"
  done
else
  echo Quality binary not executable, skipping >&2
fi

if [[ -x ${throughput_bin} ]]; then
  echo Starting throughput benchmarks >&2
  echo "threads rep throughput"
  {
    for ((j=1;j<=$(nproc);j=2*j)); do
      for r in $(seq 1 $reps);do
        echo "${throughput_bin} -j ${j} -t ${timeout}000" >&2
        ${throughput_bin} -j ${j} -t ${timeout}000 >  "${log_dir}/throughput_${j}_${r}_stdout.txt" 2> "${log_dir}/throughput_${j}_${r}_stderr.txt"
        ops=$(grep "Ops/s:" "${log_dir}/throughput_${j}_${r}_stderr.txt" | cut -d' ' -f2)
        echo $j $r $ops
      done
    done
  } > "${result_dir}/throughput.txt"
else
  echo Performance binary not executable, skipping >&2
fi
