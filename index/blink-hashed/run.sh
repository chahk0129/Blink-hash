num=50000000
for repeat in 1 2 3; do
	for t in 1 2 4 8 16 28 56; do
		echo "thread ${t}" >> out/rdtsc/entry_sampling_simd_threadalloc
		./bin/rdtsc_random_entry_simd_threadalloc $num $t >> out/rdtsc/entry_sampling_simd_threadalloc
	done
done


"
for repeat in 1 2 3; do
	for t in 1 2 4 8 16 28 56; do
		echo "thread ${t}" >> out/rdtsc/bucket_sampling
		./bin/rdtsc_random_bucket $num $t >> out/rdtsc/bucket_sampling
		echo "thread ${t}" >> out/rdtsc/entry_sampling
		./bin/rdtsc_random_entry $num $t >> out/rdtsc/entry_sampling
		echo "thread ${t}" >> out/rdtsc/bucket_sampling_simd
		./bin/rdtsc_random_bucket_simd $num $t >> out/rdtsc/bucket_sampling_simd
		echo "thread ${t}" >> out/rdtsc/entry_sampling_simd
		./bin/rdtsc_random_entry_simd $num $t >> out/rdtsc/entry_sampling_simd
		echo "thread ${t}" >> out/rdtsc/bucket_sampling_simd_chunk
		./bin/rdtsc_random_bucket_simd_chunk $num $t >> out/rdtsc/bucket_sampling_simd_chunk
		echo "thread ${t}" >> out/rdtsc/entry_sampling_simd_chunk
		./bin/rdtsc_random_entry_simd_chunk $num $t >> out/rdtsc/entry_sampling_simd_chunk
	done
done


for size in 8 32 128 512 2048; do
	for t in 1 2 4 8 16 28 56; do
		echo "thread ${t}" >> out/random_hash_${size}
		./bin/rdtsc_random_${size} 50000000 ${t} >> out/random_hash_${size}
		echo "thread ${t}" >> out/random_finger_${size}
		./bin/rdtsc_finger_random_${size} 50000000 ${t} >> out/random_finger_${size}
	done
done

for size in 64 128 256 512 1024 2048; do
	for t in 1 2 4 8 16 28 56; do
		echo "thread ${t}" >> out/hash_${size}
		./bin/rdtsc_${size} 50000000 ${t} >> out/hash_${size}
		echo "thread ${t}" >> out/finger_${size}
		./bin/rdtsc_finger_${size} 50000000 ${t} >> out/finger_${size}
	done
done
for size in 8 32 128 512 2048; do
#	for t in 1 2 4 8 16 28 56; do
	for t in 1; do
		echo "thread ${t}" >> out/util/hash_${size}
		./bin/mutex_${size} 50000000 ${t} 1 >> out/util/hash_${size}
	done
done
"
