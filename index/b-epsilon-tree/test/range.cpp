#include "tree.h"
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

    int num_data = atoi(argv[1]);
    int num_threads = atoi(argv[2]);
    std::vector<std::thread> scan_threads;

    auto data = new Key_t[num_data];
    for(int i=0; i<num_data; i++)
	data[i] = i+1;

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(data, data+num_data, g);
	
    auto tree = new betree_t<Key_t, Value_t>();

    int range[num_data];
    for(int i=0; i<num_data; i++){
	range[i] = rand() % 100;
    }

    auto scan_func = [&tree, data, num_threads, num_data, &range](int tid){
	auto num = num_data / num_threads;
	auto from = num * tid;
	auto to = num * (tid+1);
	if(to > num_data) to = num_data;

	for(auto i=from; i<to; i++){
	    auto num = range[i];
	    Value_t buf[num];
	    auto ret = tree->range_lookup(data[i], num, buf);
	}
    };

    std::cout << "Warmup starts" << std::endl;
    for(int i=0; i<num_data; i++)
	tree->insert(data[i], data[i]);
    uint64_t elapsed = 0;
    struct timespec s, e;

    std::cout << "Scan start" << std::endl;
    clock_gettime(CLOCK_MONOTONIC, &s);
    for(int i=0; i<num_threads; i++)
	scan_threads.push_back(std::thread(scan_func, i));
    for(auto& t: scan_threads) t.join();
    clock_gettime(CLOCK_MONOTONIC, &e);
    elapsed = e.tv_nsec - s.tv_nsec + (e.tv_sec - s.tv_sec)*1000000000;
    std::cout << "    " << num_data / (double)(elapsed/1000000000.0) / 1000000 << " mops/sec" << std::endl;

    return 0;
}
