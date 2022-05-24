#!/bin/bash

mkdir output
motiv=output/motiv
mkdir $motiv

num=100000000
index="cuckoo btreeolc"
threads="1 4 8 16 32 64"
iterations="1 2 3"

for iter in $iterations; do
	for t in $threads; do
		for idx in $index; do
			echo "-------------------- running with threads $t ------------------------" >> ${motiv}/random_${idx}
			./bin/microbench --init_num $num --run_num $num --threads $t --index $idx --earliest --hyper --workload load >> ${motiv}/random_${idx}
			echo "-------------------- running with threads $t ------------------------" >> ${motiv}/monotonic_${idx}
			./bin/timeseries --num $num --threads $t --index $idx --earliest --hyper --workload load >> ${motiv}/monotonic_${idx}
		done
	done
done

