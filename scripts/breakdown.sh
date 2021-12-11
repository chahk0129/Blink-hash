#!/bin/bash

mkdir output
mkdir output/breakdown
path_mixed=output/breakdown/mixed
path_rdtsc=output/breakdown/rdtsc
mkdir $path_mixed $path_rdtsc 

index="artolc artrowex hot masstree bwtree blink btreeolc"
workload="mixed"
num=200
threads="1 2 4 8 16 32 64 96 128"
input=/remote_dataset/workloads/200M/

## mixed workload
for wk in $workload; do
	for idx in $index; do
		for t in $threads; do
			./bin/breakdown_workload --input $input --workload $wk --key_type rand --index $idx --num 200 --threads $t --skew 1.2 --hyper --earliest >> ${path_mixed}/${idx}.${t}
		done
	done
done

## rdtsc
for idx in $index; do
	for t in $threads; do
		./bin/breakdown_workload --workload load --key_type rdtsc --index $idx --num 200 --threads $t --hyper --earliest >> ${path_rdtsc}/${idx}.${t}
	done
done
