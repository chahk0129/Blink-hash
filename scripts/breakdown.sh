#!/bin/bash

mkdir output
path_breakdown=output/breakdown
mkdir $path_breakdown

index="artolc masstree bwtree blink"
threads="1 8 16 32"
num=100

## rdtsc
for idx in $index; do
	for t in $threads; do
		./bin/timeseries_breakdown --workload load --num $num --index $idx --threads $t --hyper --earliest --insert_only >> ${path_breakdown}/${idx}.${t}
	done
done
