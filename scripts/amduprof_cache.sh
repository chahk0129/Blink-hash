#!/bin/bash
## measure cpu cache stats 

if [[ $1 = "" ]] || [[ $1 != */* ]]; then
	echo "Usage $0 [output path]"
	exit 1
fi

AMDuProf=/opt/AMDuProf_3.4-502/bin
sudo ${AMDuProf}/AMDuProfPcm -m l1,l2,l3 -c package=0 -C -d 10 -o $1
