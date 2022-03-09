#!/bin/bash

mkdir output
mkdir output/microbench

path_convert=output/microbench/conversion
mkdir $path_convert

targets="microbench_512b microbench_1kb microbench_2kb microbench_4kb microbench_8kb"
index="blinkhash"
workloads="scan"
threads="1"
#threads="1 4 8 16 32 64"
iterations="1"
#iterations="1 2 3"

init_num=1000000
run_num=1000000

for iter in $iterations; do
	for wk in $workloads; do
		for idx in $index; do
			for t in $threads; do
				for target in $targets; do
					echo "------------------- running with $t threads -------------------------" >> ${path_convert}/${target}
					./bin/${target} --workload $wk --init_num $init_num --run_num $run_num --index $idx --threads $t --hyper --earliest >> ${path_convert}/${target}
				done
			done
		done
	done
done
