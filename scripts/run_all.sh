#!/bin/bash
cur=$(pwd)

works="microbench.sh throughput.sh latency.sh footprint.sh breakdown.sh skewed.sh"

for w in $works; do
	./scripts/$w
done


## TODO: measuring memory bandwidth requires manual script triggers of a benchmark process and a separate AMDuProfPcm process
