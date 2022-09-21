#!/bin/bash

## measure throughput

mkdir output
mkdir output/blinkhash_split
output_int=output/blinkhash_split/int
output_ts=output/blinkhash_split/ts
mkdir $output_int $output_ts

int_path=/remote_dataset/workloads/100M/

index="blinkhash"
threads="1 4 8 16 32 64"
workloads="load"
ts_workloads="load"
iterations="1 2 3"
split="50 60 70 80 90"

## integer keys
for iter in $iterations; do
	for s in $split; do
		for t in $threads; do
			echo "---------------- running with threads $t ----------------" >> ${output_int}/${s}
			./bin/workload_${s} --input $int_path --index blinkhash --workload load --key_type rand --num 100 --skew 0.99 --threads $t --hyper --earliest --insert_only >> ${output_int}/${s}
		done
        done
done

## timeseries keys
for iter in $iterations; do
	for s in $split; do
		for t in $threads; do
			echo "---------------- running with threads $t ----------------" >> ${output_ts}/${s}
			./bin/timeseries_${s} --index blinkhash --num 100000000 --workload load --threads $t --hyper --earliest --insert_only >> ${output_ts}/${s}
		done
        done
done
