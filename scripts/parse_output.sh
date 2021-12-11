#!/bin/bash

g++ -g -O3 -std=c++17 scripts/parse_output.cpp -lstdc++fs
output_dir="output"
parse_dir="output/parsed"
result_type="throughput latency"
key_type="int rdtsc str url"

mkdir $parse_dir
for result in $result_type; do
	mkdir $parse_dir/$result
	for key in $key_type; do
		mkdir $parse_dir/$result/$key
	done
done

for result in $result_type; do
	./a.out $output_dir/$result
done
rm a.out
		
