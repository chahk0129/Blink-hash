#!/bin/bash

## measure throughput when keys are out-of-ordered

mkdir output
mkdir output/throughput
output_out=output/throughput/fuzzy2
mkdir $output_out

#index="artolc hot masstree bwtree blink blinkhash"
index="artolc hot masstree bwtree"
new_index="blinkbufferbatch blinkhash blink"
flush_index="blinkbufferbatch"
t=64
iterations="1 2"
#iterations="1 2 3"
num=100

#fuzzy_rate="0"
#fuzzy_rate="0 1 10 100 1000 10000 100000"
insert_fuzzy_rate="19400 21800 24700 30800 100000"
mixed_fuzzy_rate="20800 23600 28000 38300 100000"
for iter in $iterations; do
	for fuzzy in $insert_fuzzy_rate; do
		for idx in $new_index; do
			./bin/timeseries --index $idx --num $num --workload load --threads $t --hyper --earliest --fuzzy $fuzzy >> ${output_out}/insert_${idx}${fuzzy}
#			./bin/timeseries_flush --index $idx --num $num --workload load --threads $t --hyper --earliest --fuzzy $fuzzy >> ${output_out}/insert_${idx}flush${fuzzy}
		done
	done
done

for iter in $iterations; do
	for fuzzy in $insert_fuzzy_rate; do
		for idx in $flush_index; do
			./bin/timeseries_flush --index $idx --num $num --workload load --threads $t --hyper --earliest --fuzzy $fuzzy >> ${output_out}/insert_${idx}flush${fuzzy}
		done
	done
done


for iter in $iterations; do
	for fuzzy in $mixed_fuzzy_rate; do
		for idx in $new_index; do
			./bin/timeseries --index $idx --num $num --workload mixed --threads $t --hyper --earliest --fuzzy $fuzzy >> ${output_out}/mixed_${idx}${fuzzy}
#			./bin/timeseries_flush --index $idx --num $num --workload mixed --threads $t --hyper --earliest --fuzzy $fuzzy >> ${output_out}/mixed_${idx}flush${fuzzy}
		done
	done
done

for iter in $iterations; do
	for fuzzy in $mixed_fuzzy_rate; do
		for idx in $flush_index; do
			./bin/timeseries_flush --index $idx --num $num --workload mixed --threads $t --hyper --earliest --fuzzy $fuzzy >> ${output_out}/mixed_${idx}flush${fuzzy}
		done
	done
done



for iter in $iterations; do
	for fuzzy in $insert_fuzzy_rate; do
		for idx in $index; do
			./bin/timeseries --index $idx --num $num --workload load --threads $t --hyper --earliest --fuzzy $fuzzy >> ${output_out}/insert_${idx}${fuzzy}
		done
	done
	for fuzzy in $mixed_fuzzy_rate; do
		for idx in $index; do
			./bin/timeseries --index $idx --num $num --workload mixed --threads $t --hyper --earliest --fuzzy $fuzzy >> ${output_out}/mixed_${idx}${fuzzy}
		done
	done

done
