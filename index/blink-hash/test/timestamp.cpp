#include "tree.h"

#include <iostream>
#include <cstdlib>
#include <unistd.h>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <vector>
#include <thread>

using namespace std;
using namespace BLINK_HASH;
using Key_t = uint64_t;
using Value_t = uint64_t;

static int core_alloc_map_hyper[] = {
  0, 2, 4, 6, 8, 10, 12, 14,
  16, 18, 20, 22, 24, 26, 28, 30,
  32, 34, 36, 38, 40, 42, 44, 46,
  48, 50, 52, 54, 56, 58, 60, 62,
  1, 3, 5, 7 ,9, 11, 13, 15,
  17, 19, 21, 23, 25, 27, 29, 31,
  33, 35, 37, 39, 41, 43, 45, 47,
  49, 51, 53, 55, 57, 59, 61, 63
};

constexpr static size_t MAX_CORE_NUM = 64;
inline void pin_to_core(size_t thread_id){
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);

    size_t core_id = thread_id % MAX_CORE_NUM;
    //size_t core_id = thread_id % max_core_count;
    CPU_SET(core_alloc_map_hyper[core_id], &cpu_set);

    int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
    if(ret != 0){
        std::cerr << __func__ << ": pthread_set_affinity_np returns non-zero" << std::endl;
        exit(0);
    }
}


template <typename Fn, typename... Args>
void start_threads(btree_t<Key_t, Value_t>* tree, uint64_t num_threads, Fn&& fn, Args&& ...args){
    std::vector<std::thread> threads;
    auto fn2 = [&fn](uint64_t thread_id, Args ...args){
        pin_to_core(thread_id);
        fn(thread_id, args...);
        return;
    };

    for(auto thread_iter=0; thread_iter<num_threads; ++thread_iter){
        threads.emplace_back(std::thread(fn2, thread_iter, std::ref(args...)));
    }

    for(auto& t: threads) t.join();
}

inline uint64_t Rdtsc(){
    uint32_t lo, hi;
    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
    return (((uint64_t) hi << 32) | lo);
}

enum{
    OP_INSERT,
    OP_READ,
    OP_SCAN
};

template <typename Key_t, typename Value_t>
struct kvpair_t{
    Key_t key;
    Value_t value;
};

int main(int argc, char* argv[]){
    int num_data = atoi(argv[1]);
    int num_threads = atoi(argv[2]);
    int mode = 0; 
    if(argv[3] != nullptr){
	mode = atoi(argv[3]);
	// 0: scan only
	// 1: read only
	// 2: balanced
	// 3: insert only
    }

    //cpuinfo();

    btree_t<Key_t, Value_t>* tree = new btree_t<Key_t, Value_t>();
    std::vector<kvpair_t<Key_t, Value_t>> keys[num_threads];
    for(int i=0; i<num_threads; i++){
	size_t chunk = num_data / num_threads;
	keys[i].reserve(chunk);
    }

    bool earliest = false;
    std::vector<uint64_t> load_num(num_threads);
    auto load_earliest = [tree, num_data, num_threads, &load_num, &earliest](uint64_t tid, bool){
	int sensor_id = 0;
	size_t chunk = num_data / num_threads;
	kvpair_t<Key_t, Value_t>* kv = new kvpair_t<Key_t, Value_t>[chunk];

	for(int i=0; i<chunk; i++){
	    auto t = tree->getThreadInfo();
	    kv[i].key = ((Rdtsc() << 16) | sensor_id++ << 6) | tid;
	    kv[i].value = reinterpret_cast<Value_t>(&kv[i].key);
	    tree->insert(kv[i].key, kv[i].value, t);
	    /*
	    if(i % 1000 == 0){
		auto util = tree->rightmost_utilization();
		std::cout << util << std::endl;
	    }
	    */
	    if(sensor_id == 1024)
		sensor_id = 0;
	    if(earliest){
		load_num[tid] = i;
		return;
	    }
	}
	load_num[tid] = chunk;
	earliest = true;
    };

    auto load = [tree, num_data, num_threads, &keys](uint64_t tid, bool){
	int sensor_id = 0;
	size_t chunk = num_data / num_threads;
	kvpair_t<Key_t, Value_t>* kv = new kvpair_t<Key_t, Value_t>[chunk];

	for(int i=0; i<chunk; i++){
	    auto t = tree->getThreadInfo();
	    kv[i].key = ((Rdtsc() << 16) | sensor_id++ << 6) | tid;
	    kv[i].value = reinterpret_cast<Value_t>(&kv[i].key);
	    keys[tid].push_back(kv[i]);
	    tree->insert(kv[i].key, kv[i].value, t);
	    if(sensor_id == 1024)
		sensor_id = 0;
	}
    };

    struct timespec start, end;

    clock_gettime(CLOCK_MONOTONIC, &start);
    if(mode == 3) // insert_only
	start_threads(tree, num_threads, load_earliest, false);
    else
	start_threads(tree, num_threads, load, false);
    clock_gettime(CLOCK_MONOTONIC, &end);

    if(mode == 3){
	uint64_t num = 0;
	for(int i=0; i<num_threads; i++)
	    num += load_num[i];
	num_data = num;
    }
    auto elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    auto tput = (double)num_data / (elapsed/1000000000.0) / 1000000.0;
    std::cout << "Insertion: " << tput << " mops/sec" << std::endl;

    if(mode == 3)
	return 0;

    std::vector<std::pair<kvpair_t<Key_t, Value_t>, std::pair<int, int>>> ops;
    ops.reserve(num_data);
    if(mode == 0){ // scan only
	std::cout << "Scan 100%" << std::endl;
	for(int i=0; i<num_threads; i++){
	    for(auto& v: keys[i]){
		int r = rand() % 100;
		ops.push_back(std::make_pair(v, std::make_pair(OP_SCAN, r)));
	    }
	}
    }
    else if(mode == 1){ // read only
	std::cout << "Read 100%" << std::endl;
	for(int i=0; i<num_threads; i++){
	    for(auto& v: keys[i]){
		ops.push_back(std::make_pair(v, std::make_pair(OP_READ, 0)));
	    }
	}
    }
    else if(mode == 2){ // balanced
	std::cout << "Insert 50%, Short scan 30%, Long scan 10%, Read 10%" << std::endl;
	for(int i=0; i<num_threads; i++){
	    for(auto& v: keys[i]){
		int r = rand() % 100;
		if(r < 50)
		    ops.push_back(std::make_pair(v, std::make_pair(OP_INSERT, 0)));
		else if(r < 80){
		    int range = rand() % 5 + 5;
		    ops.push_back(std::make_pair(v, std::make_pair(OP_SCAN, range)));
		}
		else if(r < 90){
		    int range = rand() % 90 + 10;
		    ops.push_back(std::make_pair(v, std::make_pair(OP_SCAN, range)));
		}
		else 
		    ops.push_back(std::make_pair(v, std::make_pair(OP_READ, 0)));
	    }
	}
    }
    else{ 
	std::cout << "Invalid workload configuration" << std::endl;
	exit(0);
    }

    std::sort(ops.begin(), ops.end(), [](auto& a, auto& b){
	    return a.first.key < b.first.key;
	    });
    std::random_shuffle(ops.begin(), ops.end());

    std::vector<uint64_t> run_num(num_threads);
    earliest = false;
    auto scan = [tree, num_data, num_threads, ops, &run_num, &earliest](uint64_t tid, bool){
	size_t chunk = num_data / num_threads;
	size_t start = chunk * tid;
	size_t end = chunk * (tid+1);
	if(end > num_data)
	    end = num_data;

	for(auto i=start; i<end; i++){
	    uint64_t buf[ops[i].second.second];
	    auto t = tree->getThreadInfo();
	    auto ret = tree->range_lookup(ops[i].first.key, ops[i].second.second, buf, t);
	    if(earliest){
		run_num[tid] = i - start;
		return;
	    }
	}
	run_num[tid] = end - start;
	earliest = true;

    };

    auto read = [tree, num_data, num_threads, ops, &run_num, &earliest](uint64_t tid, bool){
	size_t chunk = num_data / num_threads;
	size_t start = chunk * tid;
	size_t end = chunk * (tid+1);
	if(end > num_data)
	    end = num_data;

	for(auto i=start; i<end; i++){
	    auto t = tree->getThreadInfo();
	    auto ret = tree->lookup(ops[i].first.key, t);
	    if(earliest){
		run_num[tid] = i - start;
		return;
	    }
	}
	run_num[tid] = end - start;
	earliest = true;
    };


    auto mix = [tree, num_data, num_threads, ops, &run_num, &earliest](uint64_t tid, bool){
	size_t chunk = num_data / num_threads;
	size_t start = chunk * tid;
	size_t end = chunk * (tid+1);
	if(end > num_data)
	    end = num_data;

	int sensor_id = 0;
	std::vector<uint64_t> v;
	v.reserve(5);

	for(auto i=start; i<end; i++){
	    auto t = tree->getThreadInfo();
	    auto op = ops[i].second.first;
	    if(op == OP_INSERT){
		auto key = ((Rdtsc() << 16) | sensor_id++ << 6) | tid;
		if(sensor_id == 1024) sensor_id = 0;
		tree->insert(key, (uint64_t)&key, t);
	    }
	    else if(op == OP_SCAN){
		uint64_t buf[ops[i].second.second];
		auto ret = tree->range_lookup(ops[i].first.key, ops[i].second.second, buf, t);
	    }
	    else{
		auto ret = tree->lookup(ops[i].first.key, t);
	    }
	    if(earliest){
		run_num[tid] = i - start;
		return;
	    }
	}
	run_num[tid] = end - start;
	earliest = true;
    };

    if(mode == 0){
	clock_gettime(CLOCK_MONOTONIC, &start);
	start_threads(tree, num_threads, scan, false);
	clock_gettime(CLOCK_MONOTONIC, &end);

	std::cout << "Scan: " ;
    }
    else if(mode == 1){
	clock_gettime(CLOCK_MONOTONIC, &start);
	start_threads(tree, num_threads, read, false);
	clock_gettime(CLOCK_MONOTONIC, &end);
	std::cout << "Read: "; 
    }
    else{
	clock_gettime(CLOCK_MONOTONIC, &start);
	start_threads(tree, num_threads, mix, false);
	clock_gettime(CLOCK_MONOTONIC, &end);
	std::cout << "Mix: "; 
    }
    uint64_t num = 0;
    for(int i=0; i<num_threads; i++)
	num += run_num[i];

    elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    tput = (double)num / (elapsed/1000000000.0) / 1000000.0;
    std::cout << tput << " mops/sec" << std::endl;
    return 0;
}

