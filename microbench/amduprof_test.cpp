#ifdef BTREE_OLC
#include "index/BTreeOLC/BTreeOLC_child_layout.h"
#elif defined BTREE_OLC_ADJACENT
#include "index/BTreeOLC/BTreeOLC_adjacent_layout.h"
#elif defined BLINKTREE_OPTIMIZED
#include "index/blink/tree_optimized.h"
#else
#include "index/blink/tree.h"
#endif

#include <AMDProfileController.h>
#include <ctime>
#include <vector>
#include <thread>
#include <iostream>
#include <random>
#include <algorithm>

using Key_t = uint64_t;

inline uint64_t _Rdtsc(){
    uint32_t lo, hi;
    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
    return (((uint64_t)hi << 32 ) | lo);
}

#ifdef BTREE_OLC// conventional btree
using namespace btreeolc;
#elif defined BTREE_OLC_ADJACENT // inner node adjacent layout in conventional btree
using namespace btreeolc;
#elif defined BLINKTREE_OPTIMIZED // optimized blink-tree
using namespace BLINK_OPTIMIZED;
#else // original blink-tree
using namespace BLINK;
#endif

void clear_cache(){
    uint64_t* arr = new uint64_t[1024*1024*256];
    for(int i=0; i<1024*1024*256; i++){
	arr[i] = i+1;
    }
    for(int i=100; i<1024*1024*256-100; i++){
	arr[i] = arr[i-100] + arr[i+100];
    }
    delete []arr;
}


static int max_core_count = 128;
static int core_alloc_map_hyper[] = {
    0,  64,   1,  65,   2,  66,   3,  67,
    4,  68,   5,  69,   6,  70,   7,  71,
    8,  72,   9,  73,  10,  74,  11,  75,
   12,  76,  13,  77,  14,  78,  15,  79,
   16,  80,  17,  81,  18,  82,  19,  83,
   20,  84,  21,  85,  22,  86,  23,  87,
   24,  88,  25,  89,  26,  90,  27,  91,
   28,  92,  29,  93,  30,  94,  31,  95, // socket 0
   32,  96,  33,  97,  34,  98,  35,  99,
   36, 100,  37, 101,  38, 102,  39, 103,
   40, 104,  41, 105,  42, 106,  43, 107,
   44, 108,  45, 109,  46, 110,  47, 111,
   48, 112,  49, 113,  50, 114,  51, 115,
   52, 116,  53, 117,  54, 118,  55, 119,
   56, 120,  57, 121,  58, 122,  59, 123,
   60, 124,  61, 125,  62, 126,  63, 127 // socket 1	// amd
 
/*
  0, 2, 4, 6, 8, 10, 12, 14,
  16, 18, 20, 22, 24, 26, 28, 30,
  32, 34, 36, 38, 40, 42, 44, 46,
  48, 50, 52, 54, 56, 58, 60, 62,
  1, 3, 5, 7 ,9, 11, 13, 15,
  17, 19, 21, 23, 25, 27, 29, 31,
  33, 35, 37, 39, 41, 43, 45, 47,
  49, 51, 53, 55, 57, 59, 61, 63
    0,   1,   2,   3,   4,   5,   6,   7,
    8,   9,  10,  11,  12,  13,  14,  15,
   16,  17,  18,  19,  20,  21,  22,  23,
   24,  25,  26,  27,  28,  29,  30,  31,
   64,  65,  66,  67,  68,  69,  70,  71,
   72,  73,  74,  75,  76,  77,  78,  79,
   80,  81,  82,  83,  84,  85,  86,  87,
   88,  89,  90,  91,  92,  93,  94,  95, // socket 0
   32,  33,  34,  35,  36,  37,  38,  39,
   40,  41,  42,  43,  44,  45,  46,  47,
   48,  49,  50,  51,  52,  53,  54,  55,
   56,  57,  58,  59,  60,  61,  62,  63,
   96,  97,  98,  99, 100, 101, 102, 103,
  104, 105, 106, 107, 108, 109, 110, 111,
  112, 113, 114, 115, 116, 117, 118, 119,
  120, 121, 122, 123, 124, 125, 126, 127 // socket 1
*/
};

inline void pin_to_core(size_t thread_id){
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);

    size_t core_id = thread_id % max_core_count;
    CPU_SET(core_alloc_map_hyper[core_id], &cpu_set);

    int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
    if(ret != 0){
        std::cerr << __func__ << ": pthread_set_affinity_np returns non-zero" << std::endl;
        exit(0);
    }
}

template <typename Fn, typename... Args>
#if defined (BTREE_OLC) || defined (BTREE_OLC_ADJACENT)
void start_threads(BTree<Key_t, uint64_t>* tree, uint64_t num_threads, Fn&& fn, Args&& ...args){
#else
void start_threads(btree_t<Key_t>* tree, uint64_t num_threads, Fn&& fn, Args&& ...args){
#endif

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
    if(argc < 3){
	std::cerr << "Usage: ./" << argv[0] << " (num_data) (num_threads)" << std::endl;
	exit(0);
    }
    int num_data = atoi(argv[1]);
    int num_threads = atoi(argv[2]);

    Key_t* keys = new Key_t[num_data];
    for(int i=0; i<num_data; i++){
	keys[i] = i+1;
    }
    std::random_shuffle(keys, keys+num_data);

    std::vector<std::thread> inserting_threads;
    std::vector<std::thread> searching_threads;

    std::vector<uint64_t> notfound_keys[128];

    #if defined (BTREE_OLC) || defined (BTREE_OLC_ADJACENT)
    BTree<Key_t, uint64_t>* tree = new BTree<Key_t, uint64_t>();
    #else
    btree_t<Key_t>* tree = new btree_t<Key_t>();
    #endif

    struct timespec start, end;

    size_t chunk = num_data / num_threads;
    auto insert = [&tree, &keys, num_data, num_threads](uint64_t tid, bool){
	size_t chunk = num_data / num_threads;
	int from = chunk * tid;
	int to = chunk * (tid + 1);
	for(int i=from; i<to; i++){
	    tree->insert(keys[i], (uint64_t)&keys[i]);
	}
    };
    //clear_cache();
    std::cout << "Insertion starts" << std::endl;
    amdProfileResume(AMD_PROFILE_CPU);
    
    clock_gettime(CLOCK_MONOTONIC, &start);
    start_threads(tree, num_threads, insert, false);
    clock_gettime(CLOCK_MONOTONIC, &end);
    amdProfilePause(AMD_PROFILE_CPU);
    uint64_t elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    std::cout << "elapsed time: " << elapsed/1000.0 << " usec" << std::endl;
    std::cout << "throughput: " << num_data / (double)(elapsed/1000000000.0) / 1000000 << " mops/sec" << std::endl;

    //num_threads = 64;
    auto search = [&tree, &keys, &notfound_keys, num_data, num_threads](uint64_t tid, bool){
	size_t chunk = num_data / num_threads;
	int from = chunk * tid;
	int to = chunk * (tid + 1);
	for(int i=from; i<to; i++){
    	    #if defined (BTREE_OLC) || defined (BTREE_OLC_ADJACENT)
	    uint64_t val;
	    auto ret = tree->lookup(keys[i], val);
	    if(val != (uint64_t)&keys[i]){
		notfound_keys[tid].push_back(i);
	    }
	    #else
	    auto ret = tree->lookup(keys[i]);
	    if(ret != (uint64_t)&keys[i]){
		notfound_keys[tid].push_back(i);
	    }
	    #endif
	}
    };

    //clear_cache();
    std::cout << "Search starts" << std::endl;
    getchar();
    clock_gettime(CLOCK_MONOTONIC, &start);
    start_threads(tree, num_threads, search, false);
    clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    std::cout << "elapsed time: " << elapsed/1000.0 << " usec" << std::endl;
    std::cout << "throughput: " << num_data / (double)(elapsed/1000000000.0) / 1000000 << " mops/sec" << std::endl;

    for(int i=0; i<num_threads; i++){
	for(auto& it: notfound_keys[i]){
	    std::cout << "key " << keys[it] << "not found in first place" << std::endl;
	    #if defined (BTREE_OLC) || defined (BTREE_OLC_ADJACENT)
	    uint64_t val;
	    auto ret = tree->lookup(keys[i], val);
	    if(val != (uint64_t)&(keys[it]))
		std::cout << "key " << keys[it] << " not found" << std::endl;
	    #else
	    auto ret = tree->lookup(keys[it]);
	    if(ret != (uint64_t)&keys[it])
		std::cout << "key " << keys[it] << " not found" << std::endl;
	    #endif
	}
    }
    /*
    #if not defined (BTREE_OLC) || not defined (BTREE_OLC_ADJACENT)
    uint64_t meta, structural_unoccupied, structural_occupied, key_occupied, key_unoccupied;
    meta = structural_occupied = structural_unoccupied = key_occupied = key_unoccupied = 0;
    tree->footprint(meta, structural_occupied, structural_unoccupied, key_occupied, key_unoccupied);
    std::cout << "Metadata: " << meta << " B (" << meta/1000000.0 << " MB)" << std::endl;
    std::cout << "Structural occupied: " << structural_occupied << " B (" << structural_occupied/1000000.0 << " MB)" << std::endl;
    std::cout << "Structural unoccupied: " << structural_unoccupied << " B (" << structural_unoccupied/1000000.0 << " MB)" << std::endl;
    std::cout << "Key occupied: " << key_occupied << " B (" << key_occupied/1000000.0 << " MB)" << std::endl;
    std::cout << "Key unoccupied: " << key_unoccupied << " B (" << key_unoccupied/1000000.0 << " MB)" << std::endl;
    #endif
    */
    return 0;
}
