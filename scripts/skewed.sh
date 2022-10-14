#!/bin/bash

input_path=/remote_dataset/workloads/100M
index="artolc artrowex hot masstree bwtree blink btreeolc"
skew_factor="0.5 0.7 0.9 1.1 1.3"

output_path=output/skewed
workloads="a b c e mixed"
mkdir output
mkdir $output_path
for wk in $workloads; do
	mkdir ${output_path}/$wk
done

## integer keys
for i in 1 2 3 4 5; do
	for wk in $workloads; do
		for skew in $skew_factor; do 
			for idx in $index; do
				for t in 64 128; do
					echo "---------------- running with threads $t ----------------" >> ${output_path}/${wk}/${idx}_${skew}
					./bin/workload --input $input_path --workload $wk --threads $t --key_type rand --index $idx --hyper --skew $skew --earliest --num 100 >> ${output_path}/${wk}/${idx}_${skew_factor}
				done
			done
		done
	done
done
