#!/bin/bash

## measure throughput

mkdir output
mkdir output/throughput
output_int=output/throughput/int
output_rdtsc=output/throughput/rdtsc
output_email=output/throughput/email
output_url=output/throughput/url
mkdir $output_int $output_email $output_url $output_rdtsc

int_path=/remote_dataset/workloads/100M/
str_path=/remote_dataset/workloads/100M/

index="blinkhash"
#index="artolc masstree bwtree blinkhash"
threads="1 2 4 8 16 32 64"
workloads="load a b c e"
#workloads="load a b c e mixed"
iterations="1 2 3"

"
## integer keys
for iter in $iterations; do
        for wk in $workloads; do
                for idx in $index; do
			for t in $threads; do
                               	echo "---------------- running with threads $t ----------------" >> ${output_int}/${idx}_${wk}
                               	./bin/workload --input $int_path --index $idx --workload $wk --key_type rand --num 100 --skew 1.2 --threads $t --hyper --earliest >> ${output_int}/${idx}_${wk}
			done
		done
        done
done


## email keys
for iter in $iterations; do
        for wk in mixed; do
                for idx in $index; do
                        for t in $threads; do
                                echo "---------------- running with threads $t ----------------" >> ${output_email}/${idx}_${wk}
                                ./bin/workload_string --input $str_path --index $idx --workload $wk --key_type email --threads $t --hyper --earliest >> ${output_email}/${idx}_${wk}
                        done
                done
        done
done

## url keys
for iter in $iterations; do
        for wk in mixed; do
                for idx in $index; do
                        for t in $threads; do
                                echo "---------------- running with threads $t ----------------" >> ${output_url}/${idx}_${wk}
                                ./bin/workload_url --input $str_path --index $idx --workload $wk --key_type url --threads $t --hyper --earliest >> ${output_url}/${idx}_${wk}
                        done
                done
        done
done
"
## rdtsc keys
for iter in $iterations; do
	for t in $threads; do
		echo "---------------- running with threads $t ----------------" >> ${output_rdtsc}/blinkhash
		./bin/workload --workload load --key_type rdtsc --index blinkhash --num 100 --threads $t --hyper --earliest >> ${output_rdtsc}/blinkhash
		echo "---------------- running with threads $t ----------------" >> ${output_rdtsc}/blinkhash_new
		./bin/workload_new --workload load --key_type rdtsc --index blinkhash --num 100 --threads $t --hyper --earliest >> ${output_rdtsc}/blinkhash_new
	done
done
"
for iter in $iterations; do
	for idx in $index; do
		for t in $threads; do
			echo "---------------- running with threads $t ----------------" >> ${output_rdtsc}/${idx}
			./bin/workload --workload load --key_type rdtsc --index $idx --num 100 --threads $t --hyper --earliest >> ${output_rdtsc}/${idx}
		done
	done
done
"
