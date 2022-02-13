#!/bin/bash
works="throughput.sh microbench.sh"
#works="footprint.sh throughput.sh latency.sh breakdown.sh skewed.sh"

for w in $works; do
	./scripts/$w
done
