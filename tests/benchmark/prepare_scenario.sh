#!/bin/bash

set -euo pipefail

experiment_dir=${1:- "./experiments/current"}
scenario_name=${2:- "default"}
prefill=${3:- "1000000"}
dist=${4:- "uniform"}
threads_quality=${5:-"8"}


scenario_dir=${experiment_dir}/${scenario_name}

mkdir -p ${experiment_dir}
if [[ -d ${scenario_dir} ]];then
  echo "Scenario already exists"
  exit 1
fi

mkdir ${scenario_dir}
{
  echo prefill dist qthreads
  echo ${prefill} ${dist} ${threads_quality}
} > ${scenario_dir}/config

mkdir -p ${scenario_dir}/throughput
mkdir -p ${scenario_dir}/logs
mkdir -p ${scenario_dir}/rank
mkdir -p ${scenario_dir}/delay
mkdir -p ${scenario_dir}/top_delay

