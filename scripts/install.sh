sudo apt-get update

## make extra fs space
sudo mkdir /hk
sudo /usr/local/etc/emulab/mkextrafs.pl /hk
sudo chown hcha /hk

## GCC - compiler
sudo apt-get install gcc
sudo apt-get install g++
sudo apt-get install gdb

## make - for compilation
sudo apt-get install make

## openssl - for cmake build
sudo apt-get install openssl
sudo apt-get install libssl-dev

## CMAKE - for compilation
cd /hk
#mkdir ~/cmake
#cd ~/cmake
git clone https://github.com/Kitware/CMake.git
cd CMake
./bootstrap && make -j64 && sudo make install -j64

## JAVA - for YCSB build
sudo apt-get install default-jre openjdk-11-jre-headless openjdk-8-jre-headless

## maven - for YCSB compilation
sudo apt-get install maven

## YCSB - for workload generation
cd ~/
git clone https://github.com/brianfrankcooper/YCSB.git
cd -

## rar decompression - for workload decompression
sudo apt-get install unrar

## perf - for performance analysis
sudo apt-get install linux-tools-common linux-tools-generic linux-tools-$(uname -r)
#sudo apt-get install google-perftools

## TBB - for multi-threaded runs
sudo apt-get install libtbb-dev libtbb2

## PAPI - for performance analysis
sudo apt-get install libpapi5 libpapi-dev

## TCMALLOC - for faster allocator
sudo apt-get install libtcmalloc-minimal4
#sudo ln -s /usr/lib/aarch64-linux-gnu/libtcmalloc_minimal.so.4.3.0 /usr/lib/libtcmalloc_minimal.so 	# arm
sudo ln -s /usr/lib/x86_64-linux-gnu/libtcmalloc_minimal.so.4.3.0 /usr/lib/libtcmalloc_minimal.so	# x86

## likwid - for measuring memory bandwidth
sudo apt-get install likwid
