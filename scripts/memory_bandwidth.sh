#!/bin/bash
## measure memory bandwidth


int_path=/remote_dataset/workloads/200M/
str_path=/remote_dataset/workloads/100M/

mkdir output
mkdir output/bandwidth
output_int=output/bandwidth/int
output_email=output/bandwidth/email
output_url=output/bandwidth/url

mkdir $output_int $output_email $output_url

index="artolc artrowex hot masstree bwtree blink btreeolc"
threads="1 2 4 8 16 32 64 96 128"
workloads="load a b c e mixed"

echo "------------------------------------------------------"
echo "---------- Measuring Memory Bandwidth ----------------"
echo "------------------------------------------------------"

## integer keys
for wk in $workloads; do
	for idx in $index; do
		for t in $threads; do
			echo "Running memory bandwidth for integer $wk workload for $idx with $t threads"
			sudo ./bin/workload --input $int_path --workload $wk --index $idx --threads $t --earliest --hyper --mem --key_type rand --skew 1.2 --num 200
		done
	done
done

## email keys
for idx in $index; do
	for t in $threads; do
		echo "Running memory bandwidth email $wk workload for $idx with $t threads"
		sudo ./bin/workload_string --input $str_path --workload $wk --index $idx --threads $t --earliest --hyper --mem --key_type email
	done
done

## url keys
for idx in $index; do
	for t in $threads; do
		echo "Running memory bandwidth url $wk workload for $idx with $t threads"
		sudo ./bin/workload_url --input $str_path --workload $wk --index $idx --threads $t --earliest --hyper --mem --key_type url
	done
done
