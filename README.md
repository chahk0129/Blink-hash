Blink-hash: An Adaptive Hybrid Index for In-Memory Timeseries Databases
========================================================================

The evaluation benchmark of Blink-hash extends the index-microbench (https://github.com/wangziqi2016/index-microbench) that is used to evaluate OpenBw-tree research paper (https://doi.org/10.1145/3183713.3196895).

## Dependencies ##

All the dependency installations for benchmark setup and index compilation such as CMake, YCSB, maven and java (for building YCSB), memory allocator, intel TBB, papi (performance analysis), etc, are scripted in scripts/install.sh.
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
* adms/: includes benchmark used in index locality paper (ADMS'20) --- (https://adms-conf.org/2020-camera-ready/ADMS20_03.pdf)


## Indexes ##

This repository includes various in-memory index types such as tries, B+-trees, hybrid structures, and hash table.
* Tries include Adaptive Radix Tree (ART), and Height Optimized Tree (HOT).
* B+-trees include OpenBw-tree, top-down latching B+-tree, Blink-tree, and Bepsilon-tree.
* Hybrid structures include Masstree and Blink-hash.
* Hash table includes cuckoo hashing.


## Compilation ##

Some indexes such as Blink-hash, Masstree and HOT require explicit compilation separately.
Other indexes such as ART, Bw-tree and Blink-tree are compiled during benchmark compilation.

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
