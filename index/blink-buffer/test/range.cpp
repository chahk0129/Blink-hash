#include "tree_optimized.h"

#include <ctime>
#include <vector>
#include <thread>
#include <iostream>
#include <random>
#include <algorithm>

using Key_t = uint64_t;
using namespace BLINK_OPTIMIZED;

inline uint64_t _Rdtsc(){
    uint32_t lo, hi;
    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
    return (((uint64_t)hi << 32 ) | lo);
}

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
void start_threads(BLINK_OPTIMIZED::btree_t<Key_t>* tree, uint64_t num_threads, Fn&& fn, Args&& ...args){
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



int main(int argc, char* argv[]){
    int num_data = atoi(argv[1]);
    int num_threads = atoi(argv[2]);
    Key_t* keys = new Key_t[num_data];
    for(int i=0; i<num_data; i++){
	keys[i] = i+1;
    }
    std::random_shuffle(keys, keys+num_data);

    btree_t<Key_t>* tree = new btree_t<Key_t>();
    struct timespec start, end;
    int warmup_threads= 64;

    auto warmup = [&tree, &keys, num_data, warmup_threads](uint64_t tid, bool){
	size_t chunk = num_data / warmup_threads;
	int from = chunk * tid;
	int to = chunk * (tid + 1);
	if(tid == warmup_threads-1)
	    to = num_data;

	for(int i=from; i<to; i++){
	    tree->insert(keys[i], (uint64_t)keys[i]);
	}
    };

    auto scan = [&tree, &keys, num_data, num_threads](uint64_t tid, bool){
	size_t chunk = num_data / num_threads;
	int from = chunk * tid;
	int to = chunk * (tid + 1);
	if(tid == num_threads - 1)
	    to = num_data;
	uint64_t buf[100];
	int range = 50;

	for(int i=from; i<to; i++){
	    //int range = rand() % 100;
	    //std::cout << range << std::endl;
	    auto ret = tree->range_lookup(keys[i], range, buf);
	}
    };

    std::cout << "warmup starts" << std::endl;
    start_threads(tree, warmup_threads, warmup, false);

    auto height = tree->height();
    std::cout << "height of three: " << height+1 << std::endl;

    std::cout << "scan starts" << std::endl;
    clock_gettime(CLOCK_MONOTONIC, &start);
    start_threads(tree, num_threads, scan, false);
    clock_gettime(CLOCK_MONOTONIC, &end);

    auto elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    auto tput = (double)num_data / (elapsed/1000000000.0) / 1000000.0;
    std::cout << "elapsed time: " << elapsed/1000.0 << " usec" << std::endl;
    std::cout << "throughput: " << tput << " mops/sec" << std::endl;

    height = tree->height();
    std::cout << "height of three: " << height+1 << std::endl;
    return 0;
}
