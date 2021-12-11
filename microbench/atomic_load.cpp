#include <atomic>
#include <cstdlib>
#include <ctime>
#include <unistd.h>
#include <cstdint>
#include <vector>
#include <thread>
#include <iostream>
#include <fstream>
#include <cstring>
#include <string>

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
};


template <typename Fn, typename... Args>
void start_threads(std::atomic<uint64_t>& data, uint64_t num_threads, Fn&& fn, Args&& ...args){
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
    int num = atoi(argv[1]);
    int num_threads = atoi(argv[2]);

    int num_cores;
    int* cores;
    cpuinfo();

    std::atomic<uint64_t> data;
    std::vector<int> fetch_num_per_threads[num_threads];
    std::vector<std::thread> threads;

    data.store(0);
    auto atomic_load = [&data, &fetch_num_per_threads](uint64_t tid, int num){
	int fetch_num = 0;
	for(int i=0; i<num; i++){
	    uint64_t _data;
	    if(i % 1000 == 1){
		do{
		    _data = data.load();
		}while(!data.compare_exchange_strong(_data, _data+1));
		data.fetch_add(1);
		fetch_num++;
	    }
	    else{
		_data = data.load();
	    }
	}
	fetch_num_per_threads[tid].push_back(fetch_num);
    };

    struct timespec start, end;
    int chunk = num / num_threads;

    std::cout << "doing normal cas trial... " << std::endl;
    clock_gettime(CLOCK_MONOTONIC, &start);
    start_threads(data, num_threads, atomic_load, chunk);
    clock_gettime(CLOCK_MONOTONIC, &end);
    
    auto elapsed = (end.tv_nsec - start.tv_nsec) + (end.tv_sec - start.tv_sec)*1000000000;
    auto throughput = (double)num / ((double)elapsed / 1000000000);
    std::cout << "num of atomic loads: " << num << std::endl;
    std::cout << "throughput: " << (double)throughput/1000000 << " Mops/sec" << std::endl;

    int fetch_num = 0;
    for(int i=0; i<num_threads; i++){
	fetch_num += *fetch_num_per_threads[i].data();
    }
    std::cout << "fetch_add num: " << fetch_num << std::endl;
    return 0;
}
