#!/bin/bash

set -uo pipefail

hostname=$(cat /proc/sys/kernel/hostname)
bin_dir="./build_${hostname}"
reps=5
timeout=3

name="$1"

experiment_dir="${HOME}/experiments/${hostname}/multiqueue"
log_dir="${experiment_dir}/logs/${name}"
result_dir="${experiment_dir}/results/${name}"
sssp_bin="${bin_dir}/benchmarks/${name}_shortest_path"

echo Experiment dir: ${experiment_dir} >&2
echo Log dir: ${log_dir} >&2
echo Result dir: ${result_dir} >&2
echo SSSP bin: ${sssp_bin} >&2

mkdir -p ${log_dir}
mkdir -p ${result_dir}

if [[ -x ${sssp_bin} ]]; then
	echo Starting sssp benchmark >&2
	for ((j=1;j<=$(nproc);j=2*j)); do
		echo "${sssp_bin} -j ${j} -t 200" >&2
		timeout 120 ${sssp_bin} -j ${j} -f data/data/USA-road-t.NY.gr -c data/data/NY_solution.txt 2> "${log_dir}/sssp_ny_${j}_stderr.txt" > "${result_dir}/sssp_ny_${j}.txt"
		timeout 120 ${sssp_bin} -j ${j} -f data/data/USA-road-t.USA.gr -c data/data/USA_solution.txt 2> "${log_dir}/sssp_usa_${j}_stderr.txt" > "${result_dir}/sssp_usa_${j}.txt"
	done
else
	echo SSSP binary not executable, skipping >&2
fi

