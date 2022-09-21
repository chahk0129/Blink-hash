#!/bin/bash

## measure throughput

mkdir output
mkdir output/blink_nodesize
output_int=output/blink_nodesize/int
output_ts=output/blink_nodesize/ts
mkdir $output_int $output_email $output_ts

int_path=/remote_dataset/workloads/100M/

index="blinkhash"
threads="1 4 8 16 32 64"
workloads="load c"
ts_workloads="load read mixed"
iterations="1 2 3"
node_size="64 128 256 512"

## integer keys
for iter in $iterations; do
        for wk in $workloads; do
                for idx in $index; do
			for node in $node_size; do
				for t in $threads; do
        	                       	echo "---------------- running with threads $t ----------------" >> ${output_int}/${node}_${wk}
                	               	./bin/workload_${node} --input $int_path --index $idx --workload $wk --key_type rand --num 100 --skew 0.99 --threads $t --hyper --earliest >> ${output_int}/${node}_${wk}
				done
			done
		done
        done
done

## timeseries keys
for iter in $iterations; do
        for wk in $ts_workloads; do
		for idx in $index; do
			for node in $node_size; do
				for t in $threads; do
					echo "---------------- running with threads $t ----------------" >> ${output_ts}/${node}_${wk}
					./bin/timeseries_${node} --index $idx --num 100000000 --workload $wk --threads $t --hyper --earliest >> ${output_ts}/${node}_${wk}
				done
			done
		done
        done
done
