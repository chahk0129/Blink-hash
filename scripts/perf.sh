#!/bin/bash
## measure hw stats with perf

if [[ $1 = "" ]] || [[ $1 != */* ]]; then
	echo "Usage $0 [output path]"
	exit 1
fi

sudo perf stat -d -C 0-31,64-95 -I 1000 -o $1
