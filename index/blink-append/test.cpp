//#define BREAKDOWN
#ifdef BREAKDOWN
#include "tree_breakdown.h"
#else
#include "tree.h"
#endif

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

using namespace BLINK_APPEND;

int main(int argc, char* argv[]){
    int num_data = atoi(argv[1]);
    int num_threads = atoi(argv[2]);
    int insert_only = atoi(argv[3]);
#ifdef MIXED
    int insert_ratio = atoi(argv[4]);
#endif
    Key_t* keys = new Key_t[num_data];
    for(int i=0; i<num_data; i++){
	keys[i] = i+1;
	//keys[i] = i+1;
    }
    std::random_shuffle(keys, keys+num_data);

    std::vector<std::thread> inserting_threads;
    std::vector<std::thread> searching_threads;
    std::vector<std::thread> mixed_threads;

    std::vector<uint64_t> notfound_keys[num_threads];

    btree_t<Key_t>* tree = new btree_t<Key_t>();
    std::cout << "inode_size(" << inode_t<Key_t>::cardinality << "), lnode_size(" << lnode_t<Key_t>::cardinality << ")" << std::endl;

    struct timespec start, end;

    std::atomic<uint64_t> insert_time = 0;
    std::atomic<uint64_t> search_time = 0;

#ifdef MIXED
    int half = num_data / 2;
    size_t chunk = half / num_threads;
    auto mixed = [&tree, &keys, &notfound_keys, &insert_ratio, half](int from, int to, int tid){
	int _not_found = 0;
	for(int i=from; i<to; i++){
	    int ratio = rand() % 10;
	    if(ratio < insert_ratio)
		tree->insert(keys[i+half], (uint64_t)&keys[i+half]);
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
    for(int i=0; i<num_threads; i++){
	if(i != num_threads-1)
	    mixed_threads.emplace_back(std::thread(mixed, chunk*i, chunk*(i+1), i));
	else
	    mixed_threads.emplace_back(std::thread(mixed, chunk*i, half, i));
    }
    for(auto& t: mixed_threads) t.join();
    clock_gettime(CLOCK_MONOTONIC, &end);

    uint64_t elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    std::cout << "elapsed time: " << elapsed/1000.0 << " usec" << std::endl;
    std::cout << "throughput: " << half / (double)(elapsed/1000000000.0) / 1000000 << " mops/sec" << std::endl;
#else


    size_t chunk = num_data / num_threads;
#ifdef BREAKDOWN
    auto insert = [&tree, &keys, &insert_time](int from, int to){
#else
    auto insert = [&tree, &keys, &insert_time](int from, int to){
#endif
#ifdef BREAKDOWN
	auto start = _Rdtsc()();
#endif
	for(int i=from; i<to; i++){
	    tree->insert(keys[i], (uint64_t)&keys[i]);
	}
#ifdef BREAKDOWN
	auto end = _Rdtsc()();
	insert_time += (end - start);
#endif
    };
#ifdef BREAKDOWN
    auto search = [&tree, &keys, &notfound_keys, &search_time](int from, int to, int tid){
#else
    auto search = [&tree, &keys, &notfound_keys](int from, int to, int tid){
#endif
#ifdef BREAKDOWN
	auto start = _Rdtsc()();
#endif
	for(int i=from; i<to; i++){
	    auto ret = tree->lookup(keys[i]);
	    if(ret != (uint64_t)&keys[i]){
		notfound_keys[tid].push_back(i);
	    }
	}
#ifdef BREAKDOWN
	auto end = _Rdtsc()();
	search_time += (end - start);
#endif
    };

    std::cout << "Insertion starts" << std::endl;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for(int i=0; i<num_threads; i++){
	if(i != num_threads-1)
	    inserting_threads.emplace_back(std::thread(insert, chunk*i, chunk*(i+1)));
	else
	    inserting_threads.emplace_back(std::thread(insert, chunk*i, num_data));
    }

    for(auto& t: inserting_threads) t.join();
    clock_gettime(CLOCK_MONOTONIC, &end);
    uint64_t elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    std::cout << "elapsed time: " << elapsed/1000.0 << " usec" << std::endl;
    std::cout << "throughput: " << num_data / (double)(elapsed/1000000000.0) / 1000000 << " mops/sec" << std::endl;

#ifdef TIME
    uint64_t internal_traversal, internal_sync, internal_write, leaf_traversal, leaf_sync, leaf_write;
    internal_traversal = tree->time_internal_traverse.load();
    internal_sync = tree->time_internal_sync.load();
    internal_write = tree->time_internal_write.load();
    leaf_traversal = tree->time_leaf_traverse.load();
    leaf_sync = tree->time_leaf_sync.load();
    leaf_write = tree->time_leaf_write.load();
    auto total = internal_traversal + internal_sync + internal_write + leaf_traversal + leaf_sync + leaf_write;
    std::cout << "internal_traversal: \t" << (double)internal_traversal/total*100.0 << "\t(" << internal_traversal << ")" << std::endl;
    std::cout << "internal_sync: \t" << (double)internal_sync/total*100.0 << "\t(" << internal_sync<< ")" << std::endl;
    std::cout << "internal_write: \t" << (double)internal_write/total*100.0 << "\t(" << internal_write<< ")" << std::endl;
    std::cout << "leaf_traversal: \t" << (double)leaf_traversal/total*100.0 << "\t(" << leaf_traversal << ")" << std::endl;
    std::cout << "leaf_sync: \t" << (double)leaf_sync/total*100.0 << "\t(" << leaf_sync<< ")" << std::endl;
    std::cout << "leaf_write: \t" << (double)leaf_write/total*100.0 << "\t(" << leaf_write << ")" << std::endl;
#endif


    if(insert_only){
	std::cout << "Search starts" << std::endl;
	clock_gettime(CLOCK_MONOTONIC, &start);
	for(int i=0; i<num_threads; i++){
	    if(i != num_threads-1)
		searching_threads.emplace_back(std::thread(search, chunk*i, chunk*(i+1), i));
	    else
		searching_threads.emplace_back(std::thread(search, chunk*i, num_data, i));
	}

	for(auto& t: searching_threads) t.join();
	clock_gettime(CLOCK_MONOTONIC, &end);
	elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
	std::cout << "elapsed time: " << elapsed/1000.0 << " usec" << std::endl;
	std::cout << "throughput: " << num_data / (double)(elapsed/1000000000.0) / 1000000 << " mops/sec" << std::endl;

	for(int i=0; i<num_threads; i++){
	    for(auto& it: notfound_keys[i]){
		auto ret = tree->lookup(keys[it]);
		if(ret != (uint64_t)&keys[it])
		    std::cout << "key " << keys[it] << " not found" << std::endl;
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
    }

    tree->sanity_check();

#endif
#ifdef BREAKDOWN
    auto alloc_time = tree->global_alloc_time.load();
    auto sync_time = tree->global_sync_time.load();
    auto writing_time = tree->global_writing_time.load();
    auto total = insert_time + search_time;
    auto traversal_time = total - alloc_time - sync_time - writing_time;

    std::cout << "Alloc time: " << alloc_time << "\t " << (double)alloc_time/total * 100 << " \% out of total" << std::endl;
    std::cout << "Sync time: " << sync_time << "\t " << (double)sync_time/total * 100 << " \% out of total" << std::endl;
    std::cout << "Writing time: " << writing_time << "\t " << (double)writing_time/total * 100 << " \% out of total" << std::endl;
    std::cout << "Traversal time: " << traversal_time << "\t " << (double)traversal_time/total * 100 << " \% out of total" << std::endl;
#endif
//tree->print_internal();
//tree->print_leaf();
    return 0;
}
