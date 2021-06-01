#!/bin/bash

set -uo pipefail

hostname=$(cat /proc/sys/kernel/hostname)
bin_dir="./build_${hostname}"

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
	for graph in NY USA CAL CTR GER rhg_20 rhg_22 rhg_24; do
		echo ${sssp_bin} -j $(nproc) -f data/${graph}_graph.gr -c data/${graph}_solution.txt >&2
<<<<<<< HEAD
		if [[ -f  "${result_dir}/sssp_${graph}.txt" ]]; then
			echo SSSP benchmark ${graph} exists, skipping... >&2
		else
			timeout 600 ${sssp_bin} -j $(nproc) -f data/${graph}_graph.gr -c data/${graph}_solution.txt 2> "${log_dir}/sssp_${graph}_stderr.txt" > "${result_dir}/sssp_${graph}.txt"
		fi
=======
#		if [[ -f  "${result_dir}/sssp_${graph}.txt" ]]; then
#			echo SSSP benchmark ${graph} exists, skipping... >&2
		#else
			timeout 600 ${sssp_bin} -j $(nproc) -f data/${graph}_graph.gr -c data/${graph}_solution.txt 2> "${log_dir}/sssp_${graph}_stderr.txt" > "${result_dir}/sssp_${graph}.txt"
		#fi
>>>>>>> 66aa29a (updated benchmark scripts)
	done
else
	echo SSSP binary not executable, skipping >&2
fi

