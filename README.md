Blink-hash: An Adaptive Hybrid Index for In-Memory Timeseries Databases
========================================================================

The evaluation benchmark of Blink-hash extends the index-microbench (https://github.com/wangziqi2016/index-microbench) that is used to evaluate OpenBw-tree research paper (https://doi.org/10.1145/3183713.3196895).

## Dependencies ##

All the dependency installations for benchmark setup and index compilation such as CMake, YCSB, memory allocator, etc, are scripted in scripts/install.sh.
```sh
./scripts/install.sh
```

## Generate Workloads ## 

Scripts to generate YCSB-Integer, YCSB-Email workloads are written in scripts/generate_all_workloads.sh.

The script receives a directory path as a parameter where those workloads are stored.

```sh
./scripts/generate_all_workloads.sh ${workload_directory_path}
```

## Directories ##

* include/: includes a warpper for indexes, defines structures, flags, and options used in benchmark
* src/: includes benchmark implementations
* workloads/: includes source codes to generate workloads
* index/: includes the evaluated indexes
* scripts/: includes scripts to run experiments

## Compilation ##

Some indexes such as Masstree and HOT require explicit compilation separately.

```sh
# pwd = Blink-hsah/

## Blink-hash
cd index/blink-hash
mkdir build && cd build
cmake .. && make -j

cd index/blink-hash-str
mkdir build && cd build
cmake .. && make -j

## Masstree
cd index/Masstree
./compile.sh

## HOT
cd index/HOT
mkdir build && cd build
cmake .. && make -j

## libcuckoo
cd index/libcuckoo
mkdir build && cd build
cmake .. && make -j

## Benchmark
mkdir obj bin
make -j
```
