#ifdef MUTEX
#include "mutex_node.h"
#else
#include "node.h"
#endif
#include <ctime>
#include <thread>
#include <vector>
#include <iostream>
using namespace BLINK_HASHED;


int main(int argc, char* argv[]){
    int num = atoi(argv[1]);
    int num_threads =atoi(argv[2]);

    lnode_t<uint64_t>* node = new lnode_t<uint64_t>(nullptr, 0, 0);
    uint64_t* keys = new uint64_t[num];
    for(int i=0; i<num; i++){
	keys[i] = rand() % num;

    }

    std::vector<std::thread> threads;
    auto insert = [&node, &keys](int from, int to){
	for(int i=from; i<to; i++){
	    auto ret = node->insert(keys[i], (uint64_t)&keys[i]);
	}
    };



    struct timespec start, end;
    int chunk = num/num_threads;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for(int i=0; i<num_threads; i++){
	if(i != num_threads-1)
	    threads.emplace_back(std::thread(insert, chunk*i, chunk*(i+1)));
	else
	    threads.emplace_back(std::thread(insert, chunk*i, num));
    }

    for(auto& t: threads) t.join();
    clock_gettime(CLOCK_MONOTONIC, &end);
    auto elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    std::cout << (double)num/(elapsed/1000000.0) << " kops/sec" << std::endl;
    return 0;
}


