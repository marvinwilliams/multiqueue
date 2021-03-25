#!/bin/bash

set -euo pipefail

hostname=$(cat /proc/sys/kernel/hostname)
bin_dir="build_${hostname}/bin"
reps=5
timeout=3

scenario_dir="$1"
name="$2"
throughput_bin="${3:-""}"
quality_bin="${4:-""}"

prefill=$(awk 'NR == 2 {print $1}' ${scenario_dir}/config)
dist=$(awk 'NR == 2 {print $2}' ${scenario_dir}/config)
threads_quality=$(awk 'NR == 2 {print $3}' ${scenario_dir}/config)

if [[ -z ${prefill} || -z ${dist} || -z ${threads_quality} ]]; then
	echo Could not read config
	exit 1
fi

echo Scenario: ${scenario_dir}
echo Prefill: ${prefill}
echo Distribution: ${dist}
echo Threads in quality benchmark: ${threads_quality}

if [[ -n ${throughput_bin} ]]; then
	if [[ ! -x ${bin_dir}/${throughput_bin} ]]; then
		echo Throughput binary not executable
		exit 1
	fi
	echo Starting throughput benchmarks
	{
		echo "threads rep throughput"
		for ((j=1;j<=$(nproc);j=2*j)); do
			for r in $(seq 1 $reps);do
				echo "${bin_dir}/${throughput_bin} -j ${j} -n ${prefill} -d ${dist} -t ${timeout}000" >&2
				set +e
				ops=$(timeout $(expr ${timeout} + 5) ${bin_dir}/${throughput_bin} -j ${j} -n ${prefill} -d ${dist} -t ${timeout}000 2> /dev/null | tail -n 1 | cut -d' ' -f2)
				if [[ $? -eq 0 ]]; then
					echo "-> $ops Ops" >&2
					echo $j $r $ops
				else
					echo "-> timeout" >&2
				fi
				set -e
				sleep 1
			done
		done
	} > "${scenario_dir}/throughput/${name}.txt"
fi

if [[ -n ${quality_bin} ]]; then
	if [[ ! -x ${bin_dir}/${quality_bin} ]]; then
		echo Quality binary not executable
		exit 1
	fi
	echo Starting quality benchmark
	echo "${bin_dir}/${quality_bin} -j ${threads_quality} -n ${prefill} -d ${dist}" >&2
	set +e
	timeout 20 ${bin_dir}/${quality_bin} -j ${threads_quality} -n ${prefill} -d ${dist} 2> /dev/null | tee "${scenario_dir}/logs/${name}.txt" | ${bin_dir}/evaluate_quality -r "${scenario_dir}/rank/${name}.txt" -d "${scenario_dir}/delay/${name}.txt" -t "${scenario_dir}/top_delay/${name}.txt"
	set -e
fi
