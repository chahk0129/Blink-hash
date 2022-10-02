#ifdef BUFFER
#include "run.h"
#else
#include "tree_optimized.h"
#endif
#include <iostream>
#include <cstdlib>
#include <vector>
#include <thread>
#include <algorithm>
#include <random>

using Key_t = uint64_t;
using Value_t = uint64_t;
using namespace B_EPSILON_TREE;

int main(int argc, char* argv[]){
    if(argc < 3){
	std::cout << "Usage: " << argv[0] << " [numData] [numThreads]" << std::endl;
	exit(0);
    }
    std::cout << "Inode: pivot(" << inode_t<Key_t, Value_t>::pivot_cardinality << "), msg(" << inode_t<Key_t, Value_t>::msg_cardinality << ")" << std::endl;
    std::cout << "Lnode: entry(" << lnode_t<Key_t, Value_t>::cardinality << ")" << std::endl;
    #ifdef BUFFER
    std::cout << "Buffer: " << buffer_t<Key_t, Value_t>::cardinality << std::endl;
    #endif

    int num_data = atoi(argv[1]);
    int num_threads = atoi(argv[2]);
    std::vector<std::thread> insert_threads;
    std::vector<std::thread> lookup_threads;

    auto data = new Key_t[num_data];
    for(int i=0; i<num_data; i++)
	data[i] = i+1;

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(data, data+num_data, g);
	
    #ifdef BUFFER
    auto tree = new run_t<Key_t, Value_t>();
    #else
    auto tree = new betree_t<Key_t, Value_t>();
    #endif
    
    auto insert_func = [&tree, data, num_threads, num_data](int tid){
	auto num = num_data / num_threads;
	auto from = num * tid;
	auto to = num * (tid+1);
	if(to > num_data) to = num_data;

	for(auto i=from; i<to; i++){
	    #ifdef BUFFER
	    tree->insert(data[i], data[i], tid);
	    #else
	    tree->insert(data[i], data[i]);
	    #endif
	}
    };

    int not_found[num_threads];
    auto lookup_func = [&tree, data, num_threads, num_data, &not_found](int tid){
	auto num = num_data / num_threads;
	auto from = num * tid;
	auto to = num * (tid+1);
	if(to > num_data) to = num_data;

	int _not_found = 0;
	for(auto i=from; i<to; i++){
	    auto ret = tree->lookup(data[i]);
	    if(ret != reinterpret_cast<Value_t>(data[i])){
		std::cout << "Key " << data[i] << " not found" << std::endl;
		_not_found++;
	    }
	}
	not_found[tid] = _not_found;
    };

    std::cout << "Insertion start" << std::endl;
    uint64_t elapsed = 0;
    struct timespec s, e;
    clock_gettime(CLOCK_MONOTONIC, &s);
    for(int i=0; i<num_threads; i++)
	insert_threads.push_back(std::thread(insert_func, i));
    for(auto& t: insert_threads) t.join();
    clock_gettime(CLOCK_MONOTONIC, &e);
    elapsed = e.tv_nsec - s.tv_nsec + (e.tv_sec - s.tv_sec)*1000000000;
    std::cout << "    " << num_data / (double)(elapsed/1000000000.0) / 1000000 << " mops/sec" << std::endl;

    std::cout << "Lookup start" << std::endl;
    clock_gettime(CLOCK_MONOTONIC, &s);
    for(int i=0; i<num_threads; i++)
	lookup_threads.push_back(std::thread(lookup_func, i));
    for(auto& t: lookup_threads) t.join();
    clock_gettime(CLOCK_MONOTONIC, &e);
    elapsed = e.tv_nsec - s.tv_nsec + (e.tv_sec - s.tv_sec)*1000000000;
    std::cout << "    " << num_data / (double)(elapsed/1000000000.0) / 1000000 << " mops/sec" << std::endl;

    int notFound = 0;
    for(int i=0; i<num_threads; i++)
	notFound += not_found[i];
    std::cout << "Not found: " << notFound << std::endl;

    #ifndef BUFFER
    if(notFound)
	tree->print();
    #endif
    return 0;
}
