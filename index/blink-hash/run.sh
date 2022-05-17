#!/bin/bash
num=100000000

mkdir out
load_path=out/insert2
read_path=out/lookup2
scan_path=out/scan
mixed_path=out/mixed
mkdir $load_path $read_path $scan_path $mixed_path

threads="1 4 8 16 32 64"
iteration="1"

#rw_target="baseline_ fingerprint_ sampling_ linked_"
scan_target="baseline_ fingerprint_ sampling_ linked_ adapt_"
mixed_target="baseline fingerprint_ sampling_ linked_ adapt_"
"
## Insert 
for it in $iteration; do
	for target in $rw_target; do
		for t in $threads; do
			echo "------------------- threads $t ----------------" >> ${load_path}/${target}
			./build/test/$target $num $t 3 >> ${load_path}/${target}
		done
	done
done

## Read 
for it in $iteration; do
	for target in $rw_target; do
		for t in $threads; do
			echo "------------------- threads $t ----------------" >> ${read_path}/${target}
			./build/test/$target $num $t 1 >> ${read_path}/${target}
		done
	done
done
"
## Scan 
for it in $iteration; do
	for target in $scan_target; do
		for t in $threads; do
			echo "------------------- threads $t ----------------" >> ${scan_path}/${target}
			./build/test/$target $num $t 0 >> ${scan_path}/${target}
		done
	done
done

## Balanced
for it in $iteration; do
	for target in $mixed_target; do
		for t in $threads; do
			echo "------------------- threads $t ----------------" >> ${mixed_path}/${target}
			./build/test/$target $num $t 2 >> ${mixed_path}/${target}
		done
	done
done

