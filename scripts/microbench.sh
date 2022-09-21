#!/bin/bash

mkdir output
mkdir output/microbench

path_throughput=output/microbench/throughput
path_latency=output/microbench/latency
path_profile=output/microbench/profile
mkdir $path_throughput
mkdir $path_latency
mkdir $path_profile

index="artolc masstree bwtree blink"
workloads="load"

#index="artolc hot masstree bwtree blink blinkhash"
#workloads="load update read scan"
threads="1 4 8 16 32 64"
iterations="1 2 3"

init_num=100000000
run_num=100000000


## load throughput
for iter in $iterations; do
	for idx in $index; do
		for t in $threads; do
			echo "------------------- running with $t threads -------------------------" >> ${path_throughput}/${idx}_${wk}
			./bin/microbench --workload load --init_num $init_num --run_num $run_num --index $idx --threads $t --hyper --insert_only --earliest >> ${path_throughput}/${idx}_${wk}
		done
	done
done

"
## throughput
for iter in $iterations; do
	for wk in $workloads; do
		for idx in $index; do
			for t in $threads; do
				echo "------------------- running with $t threads -------------------------" >> ${path_throughput}/${idx}_${wk}
				./bin/microbench --workload $wk --init_num $init_num --run_num $run_num --index $idx --threads $t --hyper --earliest >> ${path_throughput}/${idx}_${wk}
				echo "------------------- running with $t threads -------------------------" >> ${path_latency}/${idx}_${wk}
				./bin/microbench --workload $wk --init_num $init_num --run_num $run_num --index $idx --threads $t --latency 0.3 --hyper --earliest >> ${path_latency}/${idx}_${wk}
			done
		done
	done
done

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
"
