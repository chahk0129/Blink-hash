#!/bin/bash

mkdir output
mkdir output/microbench

path_throughput=output/microbench/throughput
path_profile=output/microbench/profile
mkdir $path_throughput
mkdir $path_profile

index="artolc masstree bwtree blink"
#index="artolc masstree bwtree blinkhash"
#workloads="scan"
workloads="load update read scan"
threads="1 4 8 16 32 64"
iterations="1 2 3"

init_num=100000000
run_num=100000000

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

for iter in $iterations; do
	for wk in $workloads; do
		for t in $threads; do
			echo "------------------ running with $t threads --------------------------" >> ${path_throughput}/blinkhash_${wk}
			./bin/microbench --workload $wk --init_num $init_num --run_num $run_num --index blinkhash --threads $t --hyper --earliest >> ${path_throughput}/blinkhash_${wk}
			echo "------------------ running with $t threads --------------------------" >> ${path_throughput}/blinkhash_new_${wk}
			./bin/microbench_new --workload $wk --init_num $init_num --run_num $run_num --index blinkhash --threads $t --hyper --earliest >> ${path_throughput}/blinkhash_new_${wk}
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
"
