$B^{link}$-hash
========================================================================

$B^{link}$-hash is an in-memory hybrid index for time-series databases.
It enhances a tree-based index with hash leaf nodes to mitigate high contention of monotonic insertions in time-series workload by distributing thread accesses to multiple buckets within a hash node.
For more details, please refer to our [paper](https://www.vldb.org/pvldb/vol16/p1235-cha.pdf) in VLDB 2023.

## Benchmark ##

The evaluation benchmark of $B^{link}$-hash extends the [index-microbench](https://github.com/wangziqi2016/index-microbench) that is used to evaluate [OpenBw-tree](https://doi.org/10.1145/3183713.3196895).


## Dependencies ##

The dependency information for the benchmark setup and index compilation is located in `scripts/install.sh`
It includes CMake, YCSB, maven and java (for building YCSB), memory allocator, intel TBB, papi, etc.


## Workload Generation ## 

Scripts to generate YCSB-Integer, YCSB-Email workloads are written in `scripts/generate_all_workloads.sh`.
The script receives a directory path as a parameter where those workloads are stored.
Time-series workload uses timestamp keys that are generated with `rdtsc()` instruction inside the benchmark.

```sh
./scripts/generate_all_workloads.sh ${workload_directory_path}
```

## Directories ##

* `include/`: includes a warpper for indexes and defines structures, flags, and options used in the benchmark
* `src/`: includes benchmark implementations
* `workloads/`: includes source codes to generate workloads for YCSB-Integer and YCSB-Email
* `index/`: includes the evaluated indexes
* `scripts/`: includes scripts to run experiments
* `adms/`: includes a benchmark used in a previous study of [index locality](https://adms-conf.org/2020-camera-ready/ADMS20_03.pdf)


## Indexes ##

This repository includes various in-memory index types such as tries, B+-trees, hybrid structures, and a hash table.
* Tries include [Adaptive Radix Tree (ART)](https://ieeexplore.ieee.org/document/6544812), and [Height Optimized Tree (HOT)](https://dl.acm.org/doi/10.1145/3183713.3196896).
* B+-trees include [OpenBw-tree](https://dl.acm.org/doi/10.1145/3183713.3196895), top-down latching B+-tree, and [Blink-tree](https://dl.acm.org/doi/10.1145/319628.319663).
* Hybrid structures include [Masstree](https://dl.acm.org/doi/10.1145/2168836.2168855), and $B^{link}$-hash.
* Hash table includes [cuckoo hashing (libcuckoo)](https://dl.acm.org/doi/10.1145/2592798.2592820).


## Compilation ##

Some indexes such as $B^{link}$-hash, Masstree, and HOT require explicit compilation before the benchmark's compilation.
Other indexes such as ART, OpenBw-tree, and $B^{link}$-tree are compiled during the benchmark compilation.

```sh
# pwd = Blink-hsah/

## $B^{link}$-hash
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

## Benchmark
mkdir obj bin
make -j
```
