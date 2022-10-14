#!/bin/bash

## measure latency 

mkdir output
mkdir output/latency

output_int=output/latency/int
output_ts=output/latency/timeseries
output_email=output/latency/email
mkdir $output_int $output_ts $output_email

int_path=/remote_dataset/workloads/100M/
str_path=/remote_dataset/workloads/100M/

index="artolc hot masstree bwtree blink blinkhash"
threads="32 64"
workloads="load a b c e"
ts_workloads="load read scan mixed"
iterations="1 2"
num=100

## integer keys
for iter in $iterations; do
	for wk in $workloads; do
		for idx in $index; do
			for t in $threads; do
				echo "---------------- running with threads $t ---------------" >> ${output_int}/${idx}_${wk}
				./bin/workload --input $int_path --index $idx --workload $wk --key_type rand --num $num --skew 0.99 --latency 0.001 --threads $t --hyper --earliest >> ${output_int}/${idx}_${wk}
				cp latency.txt /remote_dataset/latency3/${wk}_${idx}_${t}.${iter}
				rm latency.txt
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
				./bin/timeseries --index $idx --num $num --workload $wk --threads $t --hyper --earliest --latency 0.001 >> ${output_ts}/${idx}_${wk}
			done
		done
        done
done


## email keys
for iter in $iterations; do
	for idx in $index; do
		for t in $threads; do
			echo "threads $t" >> ${output_email}/${idx}_${wk}
			./bin/workload_string --input $str_path --index $idx --workload mixed --key_type email --latency 0.001 --threads $t --hyper --earliest >> ${output_email}/${idx}_${wk}
		done
	done
done
