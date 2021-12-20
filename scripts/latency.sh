#!/bin/bash

## measure latency 

mkdir output
mkdir output/latency

output_int=output/latency/int
output_email=output/latency/email
output_url=output/latency/url
mkdir $output_int $output_email $output_url

int_path=/remote_dataset/workloads/100M/
str_path=/remote_dataset/workloads/100M/

index="artolc masstree bwtree blinkhash"
threads="1 2 4 8 16 32 64"
workloads="load a b c e"
iterations="1 2 3"

## integer keys
for iter in $iterations; do
	for wk in $workloads; do
		for idx in $index; do
			for t in $threads; do
				echo "threads $t" >> ${output_int}/${idx}_${wk}
				./bin/workload --input $int_path --index $idx --workload $wk --key_type rand --num 100 --skew 1.2 --latency 0.3 --threads $t --hyper --earliest >> ${output_int}/${idx}_${wk}
			done
		done
	done
done

"
## email keys
for iter in $iterations; do
	for idx in $index; do
		for t in $threads; do
			echo "threads $t" >> ${output_email}/${idx}_${wk}
			./bin/workload_string --input $str_path --index $idx --workload mixed --key_type email --latency 0.3 --threads $t --hyper --earliest >> ${output_email}/${idx}_${wk}
		done
	done
done

## url keys
for iter in $iterations; do
	for idx in $index; do
		for t in $threads; do
			echo "threads $t" >> ${output_url}/${idx}_${wk}
			./bin/workload_url --input $str_path --index $idx --workload mixed --key_type url --latency 0.3 --threads $t --hyper --earliest >> ${output_url}/${idx}_${wk}
		done
	done
done
"
