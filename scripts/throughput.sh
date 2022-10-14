#!/bin/bash

## measure throughput

mkdir output
mkdir output/throughput
output_int=output/throughput/int
output_email=output/throughput/email
output_ts=output/throughput/ts
mkdir $output_int $output_email $output_ts

int_path=/remote_dataset/workloads/100M/
str_path=/remote_dataset/workloads/email/

index="artolc hot masstree bwtree blink blinkhash"
threads="1 4 8 16 32 64"
workloads="load a b c e"
ts_workloads="load read scan mixed"
iterations="1 2 3"
num=100

## integer keys
for iter in $iterations; do
        for wk in $workloads; do
                for idx in $index; do
			for t in $threads; do
                               	echo "---------------- running with threads $t ----------------" >> ${output_int}/${idx}_${wk}
                               	./bin/workload --input $int_path --index $idx --workload $wk --key_type rand --num $num --skew 0.99 --threads $t --hyper --earliest >> ${output_int}/${idx}_${wk}
			done
		done
        done
done

## timeseries keys
for iter in $iterations; do
        for wk in $ts_workloads; do
		for idx in $index; do
			for t in $threads; do
				echo "---------------- running with threads $t ----------------" >> ${output_ts}/${idx}_${wk}
				./bin/timeseries --index $idx --num $num --workload $wk --threads $t --hyper --earliest >> ${output_ts}/${idx}_${wk}
			done
		done
        done
done

## random insertion
mkdir ${output_ts}/random
for iter in $iterations; do
	for random in 0.0 0.2 0.4 0.6 0.8 1.0; do
		for idx in $index; do
			./bin/timeseries --index $idx --num $num --workload load --threads 64 --hyper --earliest --random $random >> ${output_ts}/random/insert_${idx}${random}
			./bin/timeseries --index $idx --num $num --workload mixed --threads 64 --hyper --earliest --random $random >> ${output_ts}/random/mixed_${idx}${random}
		done
	done
done

## fuzzy insertion
mkdir ${output_ts}/fuzzy
for iter in $iterations; do
	for fuzzy in 1 10 100 1000 10000 100000; do
		for idx in $index; do
			./bin/timeseries --index $idx --num $num --workload load --threads 64 --hyper --earliest --fuzzy $fuzzy >> ${output_ts}/fuzzy/insert_${idx}${fuzzy}
			./bin/timeseries --index $idx --num $num --workload mixed --threads 64 --hyper --earliest --fuzzy $fuzzy >> ${output_ts}/fuzzy/mixed_${idx}${fuzzy}
		done
	done
done

## email keys
for iter in $iterations; do
        for wk in $workloads; do
                for idx in $index; do
			for t in $threads; do
                                echo "---------------- running with threads $t ----------------" >> ${output_email}/${idx}_${wk}
                                ./bin/workload_string --input $str_path --index $idx --workload $wk --key_type email --threads $t --hyper --earliest >> ${output_email}/${idx}_${wk}
                        done
                done
        done
done
