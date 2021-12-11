#include <cstdlib>
#include <unistd.h>
#include <cstdint>
#include <vector>
#include <thread>
#include <iostream>
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
void start_threads(uint64_t num_threads, Fn&& fn, Args&& ...args){
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


struct entry_t{
    uint64_t key;
    uint64_t value;
};

#define INODE_SIZE 512
class inode_t{
    public:
    static constexpr size_t cardinality = INODE_SIZE / sizeof(entry_t);
    entry_t entry[cardinality];

    inode_t(){ }
};

#ifdef SAME 
#define LNODE_SIZE 512
#else
#define LNODE_SIZE 1024*256
#endif
class lnode_t{
    public:
    static constexpr size_t cardinality = LNODE_SIZE / sizeof(entry_t);
    entry_t entry[cardinality];

    lnode_t() { }
};

int main(int argc, char* argv[]){

    //uint64_t num_bytes = atoi(argv[1]);
    uint64_t num_bytes = 10000000000; // 1GB
    int num_threads = atoi(argv[1]);

    uint64_t num_alloc = num_bytes / sizeof(lnode_t);
    uint64_t period = num_alloc / lnode_t::cardinality;
    cpuinfo();

    auto alloc = [num_alloc, num_threads, period](uint64_t tid, bool){
	size_t chunk = num_alloc / num_threads;
	int from = chunk * tid;
	int to = chunk * (tid+1);
	for(int i=from; i<to; i++){
	    if(i % period == 0)
		auto inode = new inode_t();
	    auto lnode = new lnode_t();
	}
    };
    struct timespec start, end;

    std::cout << "Total alloc mem: " << num_bytes/1000000.0 << " MB \t, calling " << num_alloc << " malloc using " << num_threads << std::endl;
    std::cout << "inode_size(" << INODE_SIZE << "), \tlnode_Size(" << LNODE_SIZE << ")" << std::endl;
    clock_gettime(CLOCK_MONOTONIC, &start);
    start_threads(num_threads, alloc, false);
    clock_gettime(CLOCK_MONOTONIC, &end);
    auto elapsed = (end.tv_nsec - start.tv_nsec) + (end.tv_sec - start.tv_sec)*1000000000;
    auto throughput = (double)num_bytes / ((double)elapsed / 1000.0);
    std::cout << "throughput: " << (double)throughput/1000000 << " MB/sec" << std::endl;
    return 0;
}



