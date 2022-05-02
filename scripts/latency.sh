#!/bin/bash

## measure latency 

mkdir output
mkdir output/latency

output_int=output/latency/int
output_ts=output/latency/timestamp
output_email=output/latency/email
output_url=output/latency/url
mkdir $output_int $output_ts $output_email $output_url

int_path=/remote_dataset/workloads/100M/
str_path=/remote_dataset/workloads/100M/

index="artolc artrowex hot masstree bwtree btreeolc blink blinkhash"
#threads="1 2 4 8 16 32 64"
threads="32 64"
workloads="load a b c e mixed"
ts_workloads="load read scan mixed"
iterations="1 2"

## integer keys
for iter in $iterations; do
	for wk in $workloads; do
		for idx in $index; do
			for t in $threads; do
				echo "---------------- running with threads $t ---------------" >> ${output_int}/${idx}_${wk}
				./bin/workload --input $int_path --index $idx --workload $wk --key_type rand --num 100 --skew 0.99 --latency 0.3 --threads $t --hyper --earliest >> ${output_int}/${idx}_${wk}
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
				./bin/timeseries --index $idx --num 100000000 --workload $wk --threads $t --hyper --earliest --latency 0.3 >> ${output_ts}/${idx}_${wk}
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
