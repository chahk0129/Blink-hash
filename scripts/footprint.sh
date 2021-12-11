#!/bin/bash

## measure footprint

mkdir output
path_footprint=output/footprint
mkdir $path_footprint

index="artolc hot masstree bwtree blink btreeolc"
int_input=/remote_dataset/workloads/200M/
str_input=/remote_dataset/workloads/100M/

for idx in $index; do
	./bin/workload --input $int_input --workload load --insert_only --key_type rand --index $idx --num 200 --skew 1.2 --hyper --threads 64 >> ${path_footprint}/${idx}.int ## integer keys
	./bin/workload_string --input $str_input --workload load --insert_only --key_type email --index $idx --hyper --threads 64 >> ${path_footprint}/${idx}.email ## email keys
	./bin/workload_url --input $str_input --workload load --insert_only --key_type url --index $idx --hyper --threads 64 >> ${path_footprint}/${idx}.url ## url keys
done
