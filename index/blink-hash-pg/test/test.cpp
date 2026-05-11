//#define BREAKDOWN
#include "tree.h"

#include <ctime>
#include <vector>
#include <thread>
#include <iostream>
#include <random>
#include <algorithm>
#include <mutex>
#include <sched.h>

using Key_t = uint64_t;
using Value_t = uint64_t;

inline uint64_t _Rdtsc(){
#if defined(__x86_64__) || defined(_M_X64)
    uint32_t lo, hi;
    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
    return (((uint64_t)hi << 32 ) | lo);
#else
    uint64_t val;
    asm volatile("mrs %0, cntvct_el0" : "=r" (val));
    return val;
#endif
}

static const std::vector<int>& allowed_cpus(){
    static std::once_flag init_once;
    static std::vector<int> cpus;

    std::call_once(init_once, []() {
        cpu_set_t mask;
        CPU_ZERO(&mask);
        if(sched_getaffinity(0, sizeof(mask), &mask) == 0){
            for(int cpu = 0; cpu < CPU_SETSIZE; ++cpu){
                if(CPU_ISSET(cpu, &mask)){
                    cpus.push_back(cpu);
                }
            }
        }
    });

    return cpus;
}

inline void pin_to_core(size_t thread_id){
#ifdef __linux__
    const auto& cpus = allowed_cpus();
    if(cpus.empty()){
        return;
    }

    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    int target_cpu = cpus[thread_id % cpus.size()];
    CPU_SET(target_cpu, &cpu_set);

    int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
    if(ret != 0){
        static std::once_flag warn_once;
        std::call_once(warn_once, [ret]() {
            std::cerr << __func__ << ": pthread_setaffinity_np failed (ret=" << ret
                      << "), continuing without strict pinning" << std::endl;
        });
    }
#else
    (void)thread_id;  // thread affinity not supported on this platform
#endif
}

template <typename Fn, typename... Args>
void start_threads(BLINK_HASH::btree_t<Key_t, Value_t>* tree, uint64_t num_threads, Fn&& fn, Args&& ...args){
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
    int insert_only = atoi(argv[3]);
    //cpuinfo();

    Key_t* keys = new Key_t[num_data];
    for(int i=0; i<num_data; i++){
	keys[i] = i+1;
    }
    std::shuffle(keys, keys+num_data, std::mt19937{std::random_device{}()});

    std::vector<std::thread> inserting_threads;
    std::vector<std::thread> searching_threads;
    std::vector<std::thread> mixed_threads;

    std::vector<Key_t> notfound_keys[num_threads];

    BLINK_HASH::btree_t<Key_t, Value_t>* tree = new BLINK_HASH::btree_t<Key_t, Value_t>();
    std::cout << "inode_size(" << BLINK_HASH::inode_t<Key_t>::cardinality << "), lnode_btree_size(" << BLINK_HASH::lnode_btree_t<Key_t, Value_t>::cardinality << "), lnode_hash_size(" << BLINK_HASH::lnode_hash_t<Key_t, Value_t>::cardinality << ")" << std::endl;

    struct timespec start, end;

    std::atomic<uint64_t> insert_time = 0;
    std::atomic<uint64_t> search_time = 0;

    size_t chunk = num_data / num_threads;
    auto insert = [&tree, &keys, &insert_time, num_data, num_threads](uint64_t tid, bool){
	size_t chunk = num_data / num_threads;
	int from = chunk * tid;
	int to = chunk * (tid + 1);
	for(int i=from; i<to; i++){
	    auto t = tree->getThreadInfo();
	    tree->insert(keys[i], (Value_t)&keys[i], t);
	}
    };
    auto search = [&tree, &keys, &notfound_keys, num_data, num_threads](uint64_t tid, bool){
	size_t chunk = num_data / num_threads;
	int from = chunk * tid;
	int to = chunk * (tid + 1);
	for(int i=from; i<to; i++){
	    auto t = tree->getThreadInfo();
	    auto ret = tree->lookup(keys[i], t);
	    if(ret != (Value_t)&keys[i]){
		notfound_keys[tid].push_back(i);
	    }
	}
    };

    std::cout << "Insertion starts" << std::endl;
    clock_gettime(CLOCK_MONOTONIC, &start);
    start_threads(tree, num_threads, insert, false);
    clock_gettime(CLOCK_MONOTONIC, &end);
    uint64_t elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    std::cout << "elapsed time: " << elapsed/1000.0 << " usec" << std::endl;
    std::cout << "throughput: " << num_data / (double)(elapsed/1000000000.0) / 1000000 << " mops/sec" << std::endl;

    if(insert_only){
	std::cout << "Search starts" << std::endl;
	clock_gettime(CLOCK_MONOTONIC, &start);
	start_threads(tree, num_threads, search, false);
	clock_gettime(CLOCK_MONOTONIC, &end);
	elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
	std::cout << "elapsed time: " << elapsed/1000.0 << " usec" << std::endl;
	std::cout << "throughput: " << num_data / (double)(elapsed/1000000000.0) / 1000000 << " mops/sec" << std::endl;
    }

    tree->sanity_check();
    auto height = tree->height();
    std::cout << "Height of tree: " << height+1 << std::endl;
    auto util = tree->utilization();
    std::cout << "utilization of leaf nodes: " << util << " %" << std::endl;
    return 0;
}
