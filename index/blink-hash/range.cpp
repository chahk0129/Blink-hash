#include "tree.h"

#include <ctime>
#include <vector>
#include <thread>
#include <iostream>
#include <random>
#include <algorithm>

using Key_t = uint64_t;
using Value_t = uint64_t;
using namespace BLINK_HASH;

inline uint64_t _Rdtsc(){
    uint32_t lo, hi;
    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
    return (((uint64_t)hi << 32 ) | lo);
}

int main(int argc, char* argv[]){
    int num_data = atoi(argv[1]);
    int num_threads = atoi(argv[2]);
    int insert_ratio = atoi(argv[3]);
    Key_t* keys = new Key_t[num_data];
    for(int i=0; i<num_data; i++){
	keys[i] = i+1;
    }
    std::random_shuffle(keys, keys+num_data);

    std::vector<std::thread> warmup_threads;
    std::vector<std::thread> mixed_threads;

    btree_t<Key_t, Value_t>* tree = new btree_t<Key_t, Value_t>();
    std::cout << "inode_size(" << inode_t<Key_t>::cardinality << "), lnode_size(" << lnode_t<Key_t, Value_t>::cardinality << ")" << std::endl;

    struct timespec start, end;


    int half = num_data / 2;
    size_t chunk = half / num_threads;

    auto warmup = [&tree, &keys, half](int from, int to){
	for(int i=from; i<to; i++){
	    tree->insert(keys[i], (uint64_t)keys[i]);
	}
    };

    struct range_breakdown_t{
	uint64_t travel;
	uint64_t collect;
	uint64_t sort;
	uint64_t copy;
	uint64_t stabilization;
    };
    struct range_breakdown_t range_break[num_threads];
    auto mixed = [&tree, &keys, &insert_ratio, half, &range_break](int from, int to, int tid){
	for(int i=from; i<to; i++){
	    int ratio = rand() % 100;
	    if(ratio < insert_ratio)
		tree->insert(keys[i+half], (uint64_t)keys[i+half]);
	    else{
		uint64_t buf[50];
		auto ret = tree->range_lookup(keys[i], 50, buf);
		/*
		for(int j=0; j<ret; j++){
		    std::cout << j << ":\tlookup key: " << keys[i] << "\tfound value: " << buf[j] << std::endl;
		}*/
	    }
	}
	#ifdef RANGE_BREAKDOWN
	struct range_breakdown_t _range_break;
	tree->get_range_breakdown(_range_break.travel, _range_break.collect, _range_break.sort, _range_break.copy, _range_break.stabilization);
	memcpy(&range_break[tid], &_range_break, sizeof(struct range_breakdown_t));
	#endif
    };

    std::cout << "warmup starts" << std::endl;
    for(int i=0; i<num_threads; i++){
	if(i != num_threads-1)
	    warmup_threads.emplace_back(std::thread(warmup, chunk*i, chunk*(i+1)));
	else
	    warmup_threads.emplace_back(std::thread(warmup, chunk*i, half));
    }
    for(auto& t: warmup_threads) t.join();

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

    #ifdef RANGE_BREAKDOWN
    struct range_breakdown_t bk;
    for(int i=0; i<num_threads; i++){
	bk.travel += range_break[i].travel;
	bk.collect += range_break[i].collect;
	bk.sort += range_break[i].sort;
	bk.copy += range_break[i].copy;
	bk.stabilization += range_break[i].stabilization;
    }
    auto t_total = bk.travel + bk.collect + bk.sort + bk.copy + bk.stabilization;
    std::cout << "----------------------- RANGE BREAKDOWN ----------------------" << std::endl;
    std::cout << "Total            \t" << t_total << std::endl;
    std::cout << "  Travesral    : \t" << bk.travel << ", \t" << (double)bk.travel/t_total*100 <<  " \%" << std::endl;
    std::cout << "  Collect      : \t" << bk.collect << ", \t" << (double)bk.collect/t_total*100 << " \%" << std::endl;
    std::cout << "  Sort         : \t" << bk.sort << ", \t" << (double)bk.sort/t_total*100 << " \%" << std::endl;
    std::cout << "  Copy         : \t" << bk.copy << ", \t" << (double)bk.copy/t_total*100 << " \%" << std::endl;
    std::cout << "  Stabilization: \t" << bk.stabilization << ", \t" << (double)bk.stabilization/t_total*100 << " \%" << std::endl;
    #endif

//tree->print_internal();
//tree->print_leaf();
    return 0;
}
