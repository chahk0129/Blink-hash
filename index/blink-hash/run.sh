#!/bin/bash
num=100000000

mkdir out
mkdir out/random2 out/rdtsc3
threads="1 2 4 8 16 32 64"
repeat="1 2 3"
"
for r in $repeat; do
	for t in $threads; do
		echo "------------- threads $t -----------------" >> out/rdtsc3/baseline
		./bin/rdtsc_baseline $num $t >> out/rdtsc3/baseline
		echo "------------- threads $t -----------------" >> out/rdtsc3/simd
		./bin/rdtsc_simd $num $t >> out/rdtsc3/simd
		echo "------------- threads $t -----------------" >> out/rdtsc3/sampling
		./bin/rdtsc_sample $num $t >> out/rdtsc3/sampling
		echo "------------- threads $t -----------------" >> out/rdtsc3/linked
		./bin/rdtsc_linked $num $t >> out/rdtsc3/linked
	done
done
"
for r in $repeat; do
	for t in $threads; do
		echo "------------- threads $t -----------------" >> out/random2/baseline
		./bin/test_baseline $num $t 1 >> out/random2/baseline
		echo "------------- threads $t -----------------" >> out/random2/sampling
		./bin/test_sample $num $t 1 >> out/random2/sampling
		echo "------------- threads $t -----------------" >> out/random2/linked
		./bin/test_linked $num $t 1 >> out/random2/linked
	done
done
