#!/bin/bash
## measure memory bandwidth 

if [[ $1 = "" ]] || [[ $1 != */* ]]; then
	echo "Usage $0 [output path]"
	exit 1
fi

AMDuProf=/opt/AMDuProf_3.4-502/bin
sudo ${AMDuProf}/AMDuProfPcm -m memory -a -d 20 -o $1
