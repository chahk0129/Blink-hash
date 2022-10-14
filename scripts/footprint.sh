#!/bin/bash

## measure footprint

mkdir output
path_footprint=output/footprint
mkdir $path_footprint

index="artolc hot masstree bwtree blink blinkhash"
int_input=/remote_dataset/workloads/100M/
str_input=/remote_dataset/workloads/100M/
num=100

for idx in $index; do
	./bin/workload --input $int_input --workload load --insert_only --key_type rand --index $idx --num $num --skew 0.99 --hyper --threads 64 >> ${path_footprint}/${idx}.int ## integer keys
	./bin/workload_string --input $str_input --workload load --insert_only --key_type email --index $idx --hyper --threads 64 >> ${path_footprint}/${idx}.email ## email keys
done
