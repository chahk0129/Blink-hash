#!/bin/bash
num=100000000

mkdir out
mkdir out/rdtsc out/random
repeat="1 2 3"
for r in $repeat; do
	./bin/rdtsc_baseline $num 32 >> out/rdtsc/baseline
	./bin/rdtsc_simd $num 32 >> out/rdtsc/simd
	./bin/rdtsc_sample $num 32 >> out/rdtsc/sampling
	./bin/rdtsc_linked $num 32 >> out/rdtsc/linked

	./bin/test_baseline $num 32 0 >> out/random/baseline
	./bin/test_simd $num 32 0 >> out/random/simd
	./bin/test_sample $num 32 0 >> out/random/sampling
	./bin/test_linked $num 32 0 >> out/random/linked
done
