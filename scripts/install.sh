apt-get update

## make extra fs space - specific to Cloudlab
mkdir /hk
/usr/local/etc/emulab/mkextrafs.pl /hk
chown hcha /hk

## GCC - compiler
apt install -y gcc g++ gdb

## make - for compilation
apt install -y make

## openssl - for cmake build
apt install -y openssl libssl-dev

## CMAKE - for compilation
cd /hk
#mkdir ~/cmake
#cd ~/cmake
git clone https://github.com/Kitware/CMake.git
cd CMake
./bootstrap && make -j && make install -j

## JAVA - for YCSB build
apt install -y default-jre openjdk-11-jre-headless openjdk-8-jre-headless

## maven - for YCSB compilation
apt install -y maven

## YCSB - for workload generation
cd ~/
git clone https://github.com/brianfrankcooper/YCSB.git
cd -

## rar decompression - for workload decompression
apt install -y unrar

## perf - for performance analysis
apt install -y linux-tools-common linux-tools-generic linux-tools-$(uname -r)
#sudo apt-get install google-perftools

## TBB - for multi-threaded runs
apt install -y libtbb-dev libtbb2

## PAPI - for performance analysis
apt install -y libpapi5 libpapi-dev

## TCMALLOC - for faster allocator
apt install -y libtcmalloc-minimal4
#ln -s /usr/lib/aarch64-linux-gnu/libtcmalloc_minimal.so.4.3.0 /usr/lib/libtcmalloc_minimal.so 	# arm
ln -s /usr/lib/x86_64-linux-gnu/libtcmalloc_minimal.so.4.3.0 /usr/lib/libtcmalloc_minimal.so	# x86

## likwid - for measuring memory bandwidth
apt install -y likwid
