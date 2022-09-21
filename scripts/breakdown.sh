#!/bin/bash

mkdir output
mkdir output/breakdown
path_breakdown=output/breakdown/
mkdir $path_breakdown

index="artolc masstree bwtree blink"
threads="1 8 16 32"

## rdtsc
for idx in $index; do
	for t in $threads; do
		./bin/timeseries_breakdown --workload load --num 100000000 --index $idx --threads $t --hyper --earliest --insert_only >> ${path_breakdown}/${idx}.${t}
	done
done
