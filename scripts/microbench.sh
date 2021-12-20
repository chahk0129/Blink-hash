#!/bin/bash

mkdir output
mkdir output/microbench

path_throughput=output/microbench/throughput
path_profile=output/microbench/profile
mkdir $path_throughput
mkdir $path_profile

index="artolc masstree bwtree blinkhash"
workloads="load update read scan"
threads="1 64"
iterations="1"

init_num=100000000
run_num=100000000
"
## throughput
for iter in $iterations; do
	for wk in $workloads; do
		for idx in $index; do
			for t in $threads; do
				echo "------------------- running with $t threads -------------------------" >> ${path_throughput}/${idx}_${wk}
				./bin/microbench --workload $wk --init_num $init_num --run_num $run_num --index $idx --threads $t --hyper --earliest >> ${path_throughput}/${idx}_${wk}
			done
		done
	done
done

"
## perf profiling
for iter in $iterations; do
	for wk in $workloads; do
		for idx in $index; do
			for t in $threads; do
				echo "------------------- running with $t threads -------------------------" >> ${path_profile}/${idx}_${wk}
				sudo ./bin/microbench --workload $wk --init_num $init_num --run_num $run_num --index $idx --threads $t --hyper --earliest --profile >> ${path_profile}/${idx}_${wk}
			done
		done
	done
done
