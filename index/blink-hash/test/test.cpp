//#define BREAKDOWN
#include "tree.h"

#include <ctime>
#include <vector>
#include <thread>
#include <iostream>
#include <random>
#include <algorithm>

using Key_t = uint64_t;
using Value_t = uint64_t;

inline uint64_t _Rdtsc(){
    uint32_t lo, hi;
    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
    return (((uint64_t)hi << 32 ) | lo);
}

//using namespace BLINK_HASH;

/*
static int* core_alloc_map_hyper;
static int* core_alloc_map_numa;
int max_core_count;
int num_socket;
int cores_per_socket;
*/
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

/*
void cpuinfo(){
    FILE* fp;
    std::string cmd = "lscpu";

    fp = popen("lscpu", "r");
    if(!fp){
        std::cerr << "failed to collect cpu information" << std::endl;
        exit(0);
    }

    int cores_per_socket;
    int num_sockets;
    char temp[1024];
    while(fgets(temp, 1024, fp) != NULL){
        if(strncmp(temp, "CPU(s):", 7) == 0){
            char _temp[100];
            char _temp_[100];
            sscanf(temp, "%s %s\n", _temp, _temp_);
            max_core_count = atoi(_temp_);

            core_alloc_map_hyper = new int[max_core_count]; // hyperthreading
            core_alloc_map_numa = new int[max_core_count]; // hyperthreading
        }
        if(strncmp(temp, "Core(s) per socket:", 19) == 0){
            char _temp[100];
            char _temp_[100];
            sscanf(temp, "%s %s %s %s\n", _temp, _temp, _temp, _temp_);
            cores_per_socket = atoi(_temp_);
        }
        if(strncmp(temp, "Socket(s):", 10) == 0){
            char _temp[100];
            char _temp_[100];
            sscanf(temp, "%s %s\n", _temp, _temp_);
            num_socket = atoi(_temp_);
        }


        if(strncmp(temp, "NUMA node", 9) == 0){
            if(strncmp(temp, "NUMA node(s)", 12) == 0) continue;
            char _temp[64];
            char _temp_[64];
            char __temp[64];
            char __temp_[64];
            sscanf(temp, "%s %s %s %s\n", _temp, _temp_, __temp, __temp_);
            int num_node;
            char dummy[4], nodes[4];
            sscanf(_temp_, "%c%c%c%c%s", &dummy[0], &dummy[1], &dummy[2], &dummy[3], nodes);
            num_node = atoi(nodes);
            char* node;
            char* ptr = __temp_;
            int idx= num_node*cores_per_socket*2;
            node = strtok(ptr, ",");
            while(node != nullptr){
                core_alloc_map_hyper[idx++] = atoi(node);
                node = strtok(NULL, ",");
            }
        }

    }

    for(int i=0; i<max_core_count; i++){
        core_alloc_map_numa[i] = i;
    }

}*/

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
#ifdef MIXED
    int insert_ratio = atoi(argv[4]);
#endif
    //cpuinfo();

    Key_t* keys = new Key_t[num_data];
    for(int i=0; i<num_data; i++){
	keys[i] = i+1;
    }
    std::random_shuffle(keys, keys+num_data);

    std::vector<std::thread> inserting_threads;
    std::vector<std::thread> searching_threads;
    std::vector<std::thread> mixed_threads;

    std::vector<Key_t> notfound_keys[num_threads];

    BLINK_HASH::btree_t<Key_t, Value_t>* tree = new BLINK_HASH::btree_t<Key_t, Value_t>();
    std::cout << "inode_size(" << BLINK_HASH::inode_t<Key_t>::cardinality << "), lnode_btree_size(" << BLINK_HASH::lnode_btree_t<Key_t, Value_t>::cardinality << "), lnode_hash_size(" << BLINK_HASH::lnode_hash_t<Key_t, Value_t>::cardinality << ")" << std::endl;

    struct timespec start, end;

    std::atomic<uint64_t> insert_time = 0;
    std::atomic<uint64_t> search_time = 0;

#ifdef MIXED
    int half = num_data / 2;
    size_t chunk = half / num_threads;
    auto mixed = [&tree, &keys, &notfound_keys, &insert_ratio, num_threads, half](uint64_t tid, bool){
	#ifdef THREAD_ALLOC
	threadinfo* ti = threadinfo::make(0, 0);
	#endif
	size_t chunk = half / num_threads;
	int from = chunk*tid;
	int to = chunk*(tid+1);
	int _not_found = 0;
	for(int i=from; i<to; i++){
	    int ratio = rand() % 10;
	    if(ratio < insert_ratio)
		#ifdef THREAD_ALLOC
		tree->insert(keys[i+half], (Value_t)&keys[i+half], ti);
		#else
		tree->insert(keys[i+half], (Value_t)&keys[i+half]);
		#endif
	    else{
		auto ret = tree->lookup(keys[i]);
		if(ret != (Value_t)&keys[i]){
		    notfound_keys[tid].push_back(i);
		}
	    }
	}
    };

    std::cout << "wramup starts" << std::endl;
    for(int i=0; i<half; i++){
	tree->insert(keys[i], (Value_t)&keys[i]);
    }
    std::cout << "mixed starts" << std::endl;
    clock_gettime(CLOCK_MONOTONIC, &start);
    start_threads(tree, num_threads, mixed, false);
    clock_gettime(CLOCK_MONOTONIC, &end);

    uint64_t elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    std::cout << "elapsed time: " << elapsed/1000.0 << " usec" << std::endl;
    std::cout << "throughput: " << half / (double)(elapsed/1000000000.0) / 1000000 << " mops/sec" << std::endl;
#else


    size_t chunk = num_data / num_threads;
    auto insert = [&tree, &keys, &insert_time, num_data, num_threads](uint64_t tid, bool){
	#ifdef THREAD_ALLOC
	threadinfo* ti = threadinfo::make(0, 0);
	#endif
	size_t chunk = num_data / num_threads;
	int from = chunk * tid;
	int to = chunk * (tid + 1);
	for(int i=from; i<to; i++){
	    tree->insert(keys[i], (Value_t)&keys[i]);
	}
    };
    auto search = [&tree, &keys, &notfound_keys, num_data, num_threads](uint64_t tid, bool){
	size_t chunk = num_data / num_threads;
	int from = chunk * tid;
	int to = chunk * (tid + 1);
	for(int i=from; i<to; i++){
	    auto ret = tree->lookup(keys[i]);
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

	bool not_found = false;
	uint64_t not_found_num = 0;
	for(int i=0; i<num_threads; i++){
	    for(auto& it: notfound_keys[i]){
		not_found_num++;
		auto ret = tree->lookup(keys[it]);
		if(ret != (Value_t)&keys[it]){
		    not_found = true;
		    std::cout << "key " << keys[it] << " not found" << std::endl;
		    std::cout << "returned " << ret << "\toriginal " << (Value_t)&keys[it] << std::endl;
		}
	    }
	}
	std::cout << "# of not found keys: " << not_found_num << std::endl;

	if(not_found){
	    tree->print();
	}
    }

    std::cout << "Converting ... " << std::endl;
    tree->convert_all();
    tree->print();

    start_threads(tree, num_threads, search, false);
    bool not_found = false;
    uint64_t not_found_num = 0;
    for(int i=0; i<num_threads; i++){
	for(auto& it: notfound_keys[i]){
	    not_found_num++;
	    auto ret = tree->lookup(keys[it]);
	    if(ret != (Value_t)&keys[it]){
		not_found = true;
		std::cout << "key " << keys[it] << " not found" << std::endl;
		std::cout << "returned " << ret << "\toriginal " << (Value_t)&keys[it] << std::endl;
	    }
	}
    }
    std::cout << "# of not found keys after conversion: " << not_found_num << std::endl;


#endif
    tree->sanity_check();

    auto range = [&tree, &keys, num_data, num_threads](uint64_t tid, bool){
	size_t chunk = num_data / num_threads;
	int from = chunk * tid;
	int to = chunk * (tid + 1);
	for(int i=from; i<to; i++){
	    uint64_t buf[50];
	    auto ret = tree->range_lookup(keys[i], 50, buf);
	}
    };


    std::cout << "range starts" << std::endl;
    start_threads(tree, num_threads, range, false);
    std::cout << "range ends " << std::endl;

    auto height = tree->height();
    std::cout << "Height of tree: " << height+1 << std::endl;

    auto util = tree->utilization();
    std::cout << "utilization of leaf nodes: " << util << " %" << std::endl;
    return 0;
}
