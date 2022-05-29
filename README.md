# $Blink-hash

The evaluation benchmark of Blink-hash extends the index-microbench (https://github.com/wangziqi2016/index-microbench).

## Dependencies ##

All the dependencies such as CMake, YCSB, memory allocator, etc, are scripted in scripts/install.sh.
```sh
./scripts/install.sh
```

## Generate Workloads ## 

Scripts to generate YCSB-Integer, YCSB-Email workloads are written in scripts/generate_all_workloads.sh.

The script receives a directory path as a parameter where those workloads are stored.

```sh
./scripts/generate_all_workloads.sh ${workload_directory_path}
```

## Compilation ##

Some indexes such as Masstree and HOT require explicit compilation separately.

```sh
# Blink-hash
cd index/blink-hash
mkdir build && cd build
cmake .. && make -j

# Masstree
cd index/Masstree
./compile.sh

# HOT
cd index/HOT
mkdir build && cd build
cmake .. && make -j

# Benchmark
mkdir obj bin
make -j
```
