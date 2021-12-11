//#define BREAKDOWN
#include "tree.h"

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

using namespace BLINK_HASHED;

static int* core_alloc_map_hyper;
static int* core_alloc_map_numa;
int max_core_count;
int num_socket;
int cores_per_socket;

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

}

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
void start_threads(btree_t<Key_t>* tree, uint64_t num_threads, Fn&& fn, Args&& ...args){
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
    cpuinfo();

    Key_t* keys = new Key_t[num_data];
    for(int i=0; i<num_data; i++){
	keys[i] = i+1;
    }
    std::random_shuffle(keys, keys+num_data);

    std::vector<std::thread> inserting_threads;
    std::vector<std::thread> searching_threads;
    std::vector<std::thread> mixed_threads;

    std::vector<uint64_t> notfound_keys[num_threads];

    btree_t<Key_t>* tree = new btree_t<Key_t>();
    std::cout << "inode_size(" << inode_t<Key_t>::cardinality << "), lnode_size(" << lnode_t<Key_t>::cardinality << "), lnode_total_entry(" << lnode_t<Key_t>::cardinality * entry_num << ")" << std::endl;

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
		tree->insert(keys[i+half], (uint64_t)&keys[i+half], ti);
		#else
		tree->insert(keys[i+half], (uint64_t)&keys[i+half]);
		#endif
	    else{
		auto ret = tree->lookup(keys[i]);
		if(ret != (uint64_t)&keys[i]){
		    notfound_keys[tid].push_back(i);
		}
	    }
	}
    };

    std::cout << "wramup starts" << std::endl;
    for(int i=0; i<half; i++){
	tree->insert(keys[i], (uint64_t)&keys[i]);
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
	    #ifdef THREAD_ALLOC
	    tree->insert(keys[i], (uint64_t)&keys[i], ti);
	    #else
	    tree->insert(keys[i], (uint64_t)&keys[i]);
	    #endif
	}
    };
    auto search = [&tree, &keys, &notfound_keys, num_data, num_threads](uint64_t tid, bool){
	size_t chunk = num_data / num_threads;
	int from = chunk * tid;
	int to = chunk * (tid + 1);
	for(int i=from; i<to; i++){
	    auto ret = tree->lookup(keys[i]);
	    if(ret != (uint64_t)&keys[i]){
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
	for(int i=0; i<num_threads; i++){
	    for(auto& it: notfound_keys[i]){
		auto ret = tree->lookup(keys[it]);
		if(ret != (uint64_t)&keys[it]){
		    not_found = true;
		    std::cout << "key " << keys[it] << " not found" << std::endl;
		    std::cout << "returned " << ret << "\toriginal " << (uint64_t)&keys[it] << std::endl;
		}
	    }
	}

		/*
	std::cout << "finding it anyway\n\n" << std::endl;
	for(int i=0; i<num_threads; i++){
	    for(auto& it: notfound_keys[i]){
		auto ret = tree->find_anyway(keys[it]);
		if(ret != (uint64_t)&keys[it])
		    std::cout << "key " << keys[it] << " not found" << std::endl;
		   else{
		// lower key
		std::cout << "lower key find_anyway" << std::endl;
		ret = tree->find_anyway(keys[it]-1);
		if(ret != (uint64_t)&keys[it]-1)
		std::cout << "key " << keys[it]-1 << " not found" << std::endl;
		}
	    }
	}*/
	/*
	if(not_found){
	    tree->print_internal();
	    tree->print_leaf();
	}
	*/
    }

#endif
    tree->sanity_check();

    auto height = tree->height();
    std::cout << "Height of tree: " << height+1 << std::endl;

    auto util = tree->utilization();
    std::cout << "utilization of leaf nodes: " << util << " %" << std::endl;

#ifdef BREAKDOWN
    auto inode_traversal = tree->time_inode_traversal.load();
    auto inode_alloc = tree->time_inode_allocation.load();
    auto inode_write = tree->time_inode_write.load();
    auto inode_sync = tree->time_inode_sync.load();
    auto inode_split = tree->time_inode_split.load();
    auto lnode_traversal = tree->time_lnode_traversal.load();
    auto lnode_alloc = tree->time_lnode_allocation.load();
    auto lnode_write = tree->time_lnode_write.load();
    auto lnode_sync = tree->time_lnode_sync.load();
    auto lnode_split = tree->time_lnode_split.load();
    
    auto lnode_key_copy = tree->time_lnode_key_copy.load();
    auto lnode_find_median = tree->time_lnode_find_median.load();
    auto lnode_copy = tree->time_lnode_copy.load();
    auto lnode_update = tree->time_lnode_update.load();

    auto total = inode_traversal + inode_alloc + inode_write + inode_sync + inode_split +
		 lnode_traversal + lnode_alloc + lnode_write + lnode_sync + lnode_split + lnode_key_copy + lnode_find_median + lnode_copy + lnode_update;
    //auto total = inode_traversal + inode_alloc + inode_write + inode_sync + inode_split +
	//	 lnode_traversal + lnode_alloc + lnode_write + lnode_sync + lnode_split;

    auto leaf_split_total = lnode_key_copy + lnode_find_median + lnode_copy + lnode_update;

    std::cout << "inode traversal: \t" << inode_traversal << "\t " << (double)inode_traversal/total * 100 << " \% out of total" << std::endl;
    std::cout << "inode alloc: \t" << inode_alloc << "\t " << (double)inode_alloc/total * 100 << " \% out of total" << std::endl;
    std::cout << "inode write: \t" << inode_write << "\t " << (double)inode_write/total * 100 << " \% out of total" << std::endl;
    std::cout << "inode sync: \t" << inode_sync << "\t " << (double)inode_sync/total * 100 << " \% out of total" << std::endl;
    std::cout << "inode split: \t" << inode_split << "\t " << (double)inode_split/total * 100 << " \% out of total" << std::endl;
    std::cout << "lnode traversal: \t" << lnode_traversal << "\t " << (double)lnode_traversal/total * 100 << " \% out of total" << std::endl;
    std::cout << "lnode alloc: \t" << lnode_alloc << "\t " << (double)lnode_alloc/total * 100 << " \% out of total" << std::endl;
    std::cout << "lnode write: \t" << lnode_write << "\t " << (double)lnode_write/total * 100 << " \% out of total" << std::endl;
    std::cout << "lnode sync: \t" << lnode_sync << "\t " << (double)lnode_sync/total * 100 << " \% out of total" << std::endl;
    std::cout << "lnode split: \t" << lnode_split << "\t " << (double)lnode_split/total * 100 << " \% out of total" << std::endl;

    std::cout << "\n\n";
    std::cout << "lnode key copy: \t" << lnode_key_copy << "\t " << (double)lnode_key_copy/total * 100 << " \% out of total\t in leaf split total: " << (double)lnode_key_copy/leaf_split_total * 100 << std::endl;
    std::cout << "lnode find median: \t" << lnode_find_median << "\t " << (double)lnode_find_median/total * 100 << " \% out of total\t in leaf split total: " << (double)lnode_find_median/leaf_split_total * 100 << std::endl;
    std::cout << "lnode copy: \t" << lnode_copy << "\t " << (double)lnode_copy/total * 100 << " \% out of total\t in leaf split total: " << (double)lnode_copy/leaf_split_total * 100 << std::endl;
    std::cout << "lnode update: \t" << lnode_update<< "\t " << (double)lnode_update/total * 100 << " \% out of total\t in leaf split total: " << (double)lnode_update/leaf_split_total * 100 << std::endl;

#endif

    return 0;
}
