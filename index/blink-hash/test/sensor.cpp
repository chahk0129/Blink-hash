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

int main(int argc, char* argv[]){
    int num_data = atoi(argv[1]);
    int num_threads = atoi(argv[2]);

    //cpuinfo();

    btree_t<Key_t, Value_t>* tree = new btree_t<Key_t, Value_t>();
    std::vector<Key_t> keys[num_threads];
    for(int i=0; i<num_threads; i++){
	size_t chunk = num_data / num_threads;
	keys[i].reserve(chunk);
    }

    auto func = [tree, num_data, num_threads, &keys](uint64_t tid, bool){
	int sensor_id = 0;
	size_t chunk = num_data / num_threads;
	for(int i=0; i<chunk; i++){
	    auto t = tree->getThreadInfo();
	    auto key = ((Rdtsc() << 10) | sensor_id++ << 6) | tid;
	    keys[tid].push_back(key);
	    tree->insert(key, (Value_t)&keys[tid].back(), t);
	    if(sensor_id == 1024)
		sensor_id = 0;
	}
    };

    struct timespec start, end;

    clock_gettime(CLOCK_MONOTONIC, &start);
    start_threads(tree, num_threads, func, false);
    clock_gettime(CLOCK_MONOTONIC, &end);

    auto elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    auto tput = (double)num_data / (elapsed/1000000000.0) / 1000000.0;
    std::cout << "Insertion: " << tput << " mops/sec" << std::endl;

    std::vector<std::pair<Key_t, int>> ops;
    ops.reserve(num_data);
    for(int i=0; i<num_threads; i++){
	for(auto& v: keys[i]){
	    int r = rand() % 100;
	    if(r < 50)
		ops.push_back(std::make_pair(v, 0)); // insert
	    else if(r < 80)
		ops.push_back(std::make_pair(v, 1)); // short scan
	    else if(r < 90)
		ops.push_back(std::make_pair(v, 2)); // long scan
	    else
		ops.push_back(std::make_pair(v, 3)); // read
	}
    }
    std::sort(ops.begin(), ops.end(), [](auto& a, auto& b){
	    return a.first < b.first;
	    });

    auto func_read = [tree, num_data, num_threads, ops](uint64_t tid, bool){
	size_t chunk = num_data / num_threads;
	size_t start = chunk*tid;
	size_t end = chunk*(tid+1);
	if(end > num_data)
	    end = num_data;
	for(auto i=start; i<end; i++){
	    auto t = tree->getThreadInfo();
	    auto ret = tree->lookup(ops[i].first, t);
	    if(ret == 0){
		std::cout << "Not found --- key " << ops[i].first << std::endl;
	    }
	}
    };
    auto func_scan = [tree, num_data, num_threads, ops](uint64_t tid, bool){
	size_t chunk = num_data / num_threads;
	size_t start = chunk*tid;
	size_t end = chunk*(tid+1);
	if(end > num_data)
	    end = num_data;
	int range = 50;
	for(auto i=start; i<end; i++){
	    auto t = tree->getThreadInfo();
	    uint64_t buf[range];
	    auto ret = tree->range_lookup(ops[i].first, range, buf, t);
	}
    };

    auto func_mix = [tree, num_data, num_threads, ops](uint64_t tid, bool){
	size_t chunk = num_data / num_threads;
	size_t start = chunk*tid;
	size_t end = chunk*(tid+1);
	if(end > num_data)
	    end = num_data;
	int long_range = 50;
	int short_range = 5;
	int sensor_id = 0;
	for(auto i=start; i<end; i++){
	    auto t = tree->getThreadInfo();
	    auto op = ops[i].second;
	    if(op == 0){
		auto key = ((Rdtsc() << 10) | sensor_id++ << 6) | tid;
		tree->insert(key, (Value_t)&key, t);
		if(sensor_id == 1024)
		    sensor_id = 0;
	    }
	    else if(op == 1){ // short scan
		uint64_t buf[short_range];
		auto ret = tree->range_lookup(ops[i].first, short_range, buf, t);
	    }
	    else if(op == 2){ // long scan
		uint64_t buf[long_range];
		auto ret = tree->range_lookup(ops[i].first, long_range, buf, t);
	    }
	    else{ // read
		auto ret = tree->lookup(ops[i].first, t);
		if(ret == 0){
		    std::cout << "Not found --- key " << ops[i].first << std::endl;
		}
	    }
	}
    };

    clock_gettime(CLOCK_MONOTONIC, &start);
    start_threads(tree, num_threads, func_read, false);
    clock_gettime(CLOCK_MONOTONIC, &end);

    elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    tput = (double)num_data / (elapsed/1000000000.0) / 1000000.0;
    std::cout << "Search: " << tput << " mops/sec" << std::endl;


    clock_gettime(CLOCK_MONOTONIC, &start);
    start_threads(tree, num_threads, func_scan, false);
    clock_gettime(CLOCK_MONOTONIC, &end);

    elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    tput = (double)num_data / (elapsed/1000000000.0) / 1000000.0;
    std::cout << "Scan: " << tput << " mops/sec" << std::endl;

    clock_gettime(CLOCK_MONOTONIC, &start);
    start_threads(tree, num_threads, func_mix, false);
    clock_gettime(CLOCK_MONOTONIC, &end);

    elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    tput = (double)num_data / (elapsed/1000000000.0) / 1000000.0;
    std::cout << "Mix: " << tput << " mops/sec" << std::endl;

    auto height = tree->height();
    std::cout << "Height of tree: " << height+1 << std::endl;

    auto util = tree->utilization();
    std::cout << util << " \% of utilization" << std::endl;
    return 0;
}

