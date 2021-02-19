#!/bin/bash

pq=$1

cmake --build build --target ${pq}_quality_benchmark
cmake --build build --target ${pq}_throughput_benchmark
cmake --build build --target evaluate_quality
for j in 1 2 4 8 16;do
  echo ${j} $(sudo ./build/benchmark/${pq}_throughput_benchmark -j ${j} 2> /dev/null | tail -n 1 | cut -d' ' -f2)
done > runtimes/${pq}.txt
./build/benchmark/${pq}_quality_benchmark -j 8 2> /dev/null | ./build/benchmark/evaluate_quality -r rank_histograms/${pq}.txt -d delay_histograms/${pq}.txt -t top_delay_histograms/${pq}.txt
