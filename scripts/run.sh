#!/bin/bash
works="throughput.sh latency.sh"

for w in $works; do
	./scripts/$w
done
