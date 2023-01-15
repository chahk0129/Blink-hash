#include "run.h"
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

using namespace BLINK_BUFFER_BATCH;

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

    int cores_per_socket = 0;
    int num_sockets = 0;
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
void start_threads(run_t<Key_t, Value_t>* tree, uint64_t num_threads, Fn&& fn, Args&& ...args){
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

enum{
    OP_INSERT,
    OP_READ,
    OP_SCAN
};

template <typename Key_t, typename Value_t>
struct kvpair_t{
    Key_t key;
    Value_t value;
};


int main(int argc, char* argv[]){
    int num_data = atoi(argv[1]);
    int num_threads = atoi(argv[2]);
    int insert_only = atoi(argv[3]);
    cpuinfo();

    std::vector<kvpair_t<Key_t, Value_t>> keys[num_threads];
    for(int i=0; i<num_threads; i++){
	size_t chunk = num_data / num_threads;
	keys[i].reserve(chunk);
    }

    std::vector<uint64_t> notfound_keys[num_threads];

    auto tree = new run_t<Key_t, Value_t>();
    struct timespec start, end;

    std::atomic<uint64_t> insert_time = 0;
    std::atomic<uint64_t> search_time = 0;

    size_t chunk = num_data / num_threads;
    auto insert = [&tree, &keys, num_data, num_threads](uint64_t tid, bool){
	int sensor_id = 0;
	size_t chunk = num_data / num_threads;
	kvpair_t<Key_t, Value_t>* kv = new kvpair_t<Key_t, Value_t>[chunk];

	for(int i=0; i<chunk; i++){
	    kv[i].key = ((_Rdtsc() << 16) | sensor_id++ << 6) | tid;
	    kv[i].value = reinterpret_cast<Value_t>(&kv[i].key);
	    keys[tid].push_back(kv[i]);
	    tree->insert(kv[i].key, kv[i].value);
	    if(sensor_id == 1024)
		sensor_id = 0;
	}
    };

    auto search = [&tree, &keys, &notfound_keys, num_data, num_threads](uint64_t tid, bool){
	for(auto& v: keys[tid]){
	    auto ret = tree->find(v.key);
	    if(ret != v.value)
		notfound_keys[tid].push_back(v.key);
	}
    };

    std::cout << "Insertion starts" << std::endl;
    clock_gettime(CLOCK_MONOTONIC, &start);
    start_threads(tree, num_threads, insert, false);
    clock_gettime(CLOCK_MONOTONIC, &end);
    uint64_t elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    std::cout << "elapsed time: " << elapsed / 1000000000.0 << " sec" << std::endl;
    std::cout << "throughput: " << num_data / (double)(elapsed/1000000000.0) / 1000000 << " mops/sec" << std::endl;

    if(insert_only){
	std::cout << "Search starts" << std::endl;
	clock_gettime(CLOCK_MONOTONIC, &start);
	start_threads(tree, num_threads, search, false);
	clock_gettime(CLOCK_MONOTONIC, &end);
	elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
	std::cout << "elapsed time: " << elapsed / 1000000000.0 << " sec" << std::endl;
	std::cout << "throughput: " << num_data / (double)(elapsed/1000000000.0) / 1000000 << " mops/sec" << std::endl;

	int not_found = 0;
	for(int i=0; i<num_threads; i++)
	    not_found += notfound_keys[i].size();
	std::cout << "not found: " << not_found << std::endl;
    }

    return 0;
}
