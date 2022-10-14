#!/bin/bash

## measure throughput

mkdir output
mkdir output/adms
output_art=output/adms/art
output_bwtree=output/adms/bwtree
mkdir $output_art $output_bwtree

index="artolc hot masstree bwtree blink blinkhash"
threads="1 4 8 16 32 64"
workloads="load"
iterations="1 2 3"
num=100

## integer keys
for iter in $iterations; do
        for wk in $workloads; do
                for idx in $index; do
			for t in $threads; do
                               	echo "---------------- running with threads $t ----------------" >> ${output_art}/${idx}_${wk}
				./bin/adms --workload $wk --num $num --threads $t --index $idx --type 1 >> ${output_art}/${idx}_${wk}
				rm zipfian_data_*.bin
                               	echo "---------------- running with threads $t ----------------" >> ${output_bwtree}/${idx}_${wk}
				./bin/adms --workload $wk --num $num --threads $t --index $idx --type 2 >> ${output_bwtree}/${idx}_${wk}
				rm zipfian_data_*.bin
			done
		done
        done
done
