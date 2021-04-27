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
quality_bin="${bin_dir}/benchmarks/${name}_quality"
throughput_bin="${bin_dir}/benchmarks/${name}_throughput"

echo Quality bin: ${quality_bin} >&2
echo Throughput bin: ${throughput_bin} >&2

mkdir -p ${log_dir}
mkdir -p ${result_dir}

for dist in ascending descending threadid; do
	if [[ -x ${quality_bin} ]]; then
		echo Starting quality benchmark >&2
		for ((j=1;j<=$(nproc);j=2*j)); do
			if [[ -f "${log_dir}/evaluate_quality_${j}_dist_${dist}_stderr.txt" ]]; then
				echo Quality benchmark exists, skipping... >&2
			else
				echo "${quality_bin} -j ${j} -d ${dist}" >&2
				${quality_bin} -j ${j} -d ${dist}  2> "${log_dir}/quality_${j}_dist_${dist}_stderr.txt" | ${bin_dir}/utils/evaluate_quality -r "${result_dir}/rank_${j}_dist_${dist}.txt" -d "${result_dir}/delay_${j}_dist_${dist}.txt" -t "${result_dir}/top_delay_${j}_dist_${dist}.txt" 2> "${log_dir}/evaluate_quality_${j}_dist_${dist}_stderr.txt"
			fi
		done
	else
		echo Quality binary not executable, skipping >&2
	fi

	if [[ -x ${throughput_bin} ]]; then
		if [[ -f  "${result_dir}/throughput_dist_${dist}.txt" ]]; then
			echo Throughput benchmark exists, skipping... >&2
		else
			echo Starting throughput benchmarks >&2
			{
				echo "threads rep throughput"
				for ((j=1;j<=$(nproc);j=2*j)); do
					for r in $(seq 1 $reps);do
						echo "${throughput_bin} -j ${j} -d ${dist} -t ${timeout}000" >&2
						${throughput_bin} -j ${j} -d ${dist} -t ${timeout}000 2> "${log_dir}/throughput_${j}_${r}_dist_${dist}_stderr.txt" > "${log_dir}/throughput_${j}_${r}_dist_${dist}_stdout.txt" 
						ops=$(grep "Ops/s:" "${log_dir}/throughput_${j}_${r}_dist_${dist}_stdout.txt" | cut -d' ' -f2)
						echo $j $r $ops
					done
				done
			} > "${result_dir}/throughput_dist_${dist}.txt"
		fi
	else
		echo Throughput binary not executable, skipping >&2
	fi
done

for prefill in 0 1000 10000000; do
	if [[ -x ${quality_bin} ]]; then
		echo Starting quality benchmark >&2
		for ((j=1;j<=$(nproc);j=2*j)); do
			if [[ -f "${log_dir}/evaluate_quality_${j}_n_${prefill}_stderr.txt" ]]; then
				echo Quality benchmark exists, skipping... >&2
			else
				echo "${quality_bin} -j ${j} -n ${prefill}" >&2
				${quality_bin} -j ${j} -n ${prefill} 2> "${log_dir}/quality_${j}_n_${prefill}_stderr.txt" | ${bin_dir}/utils/evaluate_quality -r "${result_dir}/rank_${j}_n_${prefill}.txt" -d "${result_dir}/delay_${j}_n_${prefill}.txt" -t "${result_dir}/top_delay_${j}_n_${prefill}.txt" 2> "${log_dir}/evaluate_quality_${j}_n_${prefill}_stderr.txt"
			fi
		done
	else
		echo Quality binary not executable, skipping >&2
	fi
	if [[ -x ${throughput_bin} ]]; then
		if [[ -f  "${result_dir}/throughput_n_${prefill}.txt" ]]; then
			echo Throughput benchmark exists, skipping... >&2
		else
			echo Starting throughput benchmarks >&2
			{
				echo "threads rep throughput"
				for ((j=1;j<=$(nproc);j=2*j)); do
					for r in $(seq 1 $reps);do
						echo "${throughput_bin} -j ${j} -n ${prefill} -t ${timeout}000" >&2
						${throughput_bin} -j ${j} -n ${prefill} -t ${timeout}000 2> "${log_dir}/throughput_${j}_${r}_n_${prefill}_stderr.txt" >  "${log_dir}/throughput_${j}_${r}_n_${prefill}_stdout.txt"
						ops=$(grep "Ops/s:" "${log_dir}/throughput_${j}_${r}_n_${prefill}_stdout.txt" | cut -d' ' -f2)
						echo $j $r $ops
					done
				done
			} > "${result_dir}/throughput_n_${prefill}.txt"
		fi
	else
		echo Throughput binary not executable, skipping >&2
	fi
done

for s in 1000 5000000; do
	if [[ -x ${quality_bin} ]]; then
		echo Starting quality benchmark >&2
		for ((j=1;j<=$(nproc);j=2*j)); do
			if [[ -f "${log_dir}/evaluate_quality_${j}_s_${s}_stderr.txt" ]]; then
				echo Quality benchmark exists, skipping... >&2
			else
				echo "${quality_bin} -j ${j} -s ${s} -o 10000" >&2
				${quality_bin} -j ${j} -s ${s} -o 10000 2> "${log_dir}/quality_${j}_s_${s}_stderr.txt" | ${bin_dir}/utils/evaluate_quality -r "${result_dir}/rank_${j}_s_${s}.txt" -d "${result_dir}/delay_${j}_s_${s}.txt" -t "${result_dir}/top_delay_${j}_s_${s}.txt" 2> "${log_dir}/evaluate_quality_${j}_s_${s}_stderr.txt"
			fi
		done
	else
		echo Quality binary not executable, skipping >&2
	fi
done
