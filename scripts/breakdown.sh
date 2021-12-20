#!/bin/bash

mkdir output
mkdir output/breakdown
path_rdtsc=output/breakdown/rdtsc
mkdir $path_mixed $path_rdtsc 

index="artolc masstree bwtree"
threads="32"

## rdtsc
for idx in $index; do
	for t in $threads; do
		./bin/breakdown_workload --workload load --key_type rdtsc --index $idx --num 100 --threads $t --hyper --earliest >> ${path_rdtsc}/${idx}.${t}
	done
done
