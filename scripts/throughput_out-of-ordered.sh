#!/bin/bash

## measure throughput when keys are out-of-ordered

mkdir output
mkdir output/throughput
output_out=output/throughput/fuzzy
mkdir $output_out

index="artolc hot masstree bwtree blink blinkhash"
index_batch="blinkbufferbatch"
t=64
iterations="1 2 3"
num=100

insert_fuzzy_rate="14600 18700"
mixed_fuzzy_rate="13200 15800 21300"
insert_fuzzy_rate="0 1000 10000 12400 14600 18700 100000"
mixed_fuzzy_rate="0 1000 10000 13200 15800 21300 100000"

## monotonic fuzzy insert
for iter in $iterations; do
	for fuzzy in $insert_fuzzy_rate; do
		for idx in $index; do
			./bin/timeseries --index $idx --num $num --workload load --threads $t --hyper --earliest --fuzzy $fuzzy >> ${output_out}/insert_${idx}${fuzzy}
		done
	done
done

## monotonic fuzzy mixed
for iter in $iterations; do
	for fuzzy in $mixed_fuzzy_rate; do
		for idx in $index; do
			./bin/timeseries --index $idx --num $num --workload mixed --threads $t --hyper --earliest --fuzzy $fuzzy >> ${output_out}/mixed_${idx}${fuzzy}
		done
	done
done


## blinkbufferbatch -- monotonic fuzzy insertion
for iter in $iterations; do
	for fuzzy in $insert_fuzzy_rate; do
		for idx in $index_batch; do
			./bin/timeseries --index $idx --num $num --workload load --threads $t --hyper --earliest --fuzzy $fuzzy >> ${output_out}/insert_${idx}${fuzzy}
			./bin/timeseries_flush --index $idx --num $num --workload load --threads $t --hyper --earliest --fuzzy $fuzzy >> ${output_out}/insert_${idx}flush${fuzzy}
		done
	done
done

## blinkbufferbatch -- monotonic fuzzy mixed
for iter in $iterations; do
	for fuzzy in $mixed_fuzzy_rate; do
		for idx in $index_batch; do
			./bin/timeseries --index $idx --num $num --workload mixed --threads $t --hyper --earliest --fuzzy $fuzzy >> ${output_out}/mixed_${idx}${fuzzy}
			./bin/timeseries_flush --index $idx --num $num --workload mixed --threads $t --hyper --earliest --fuzzy $fuzzy >> ${output_out}/mixed_${idx}flush${fuzzy}
		done
	done
done
