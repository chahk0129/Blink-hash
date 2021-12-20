#!/bin/bash
works="breakdown.sh throughput.sh"
#works="footprint.sh throughput.sh latency.sh breakdown.sh skewed.sh"

for w in $works; do
	./scripts/$w
done
