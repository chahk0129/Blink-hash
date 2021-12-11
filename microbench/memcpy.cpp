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

#define LNODE_SIZE 1024*256
class lnode_t{
    public:
    static constexpr size_t cardinality = LNODE_SIZE / sizeof(entry_t);
    entry_t entry[cardinality];

    lnode_t() { }
    void* operator new(size_t size){
	void* mem;
	auto ret = posix_memalign(&mem, 64, size);
	return mem;
    }
};

int main(int argc, char* argv[]){

    int repeat = atoi(argv[1]);
    int num_threads = atoi(argv[2]);
    int mode = atoi(argv[3]); // 0-bulk copy, 1-chunk copy

    cpuinfo();


    auto leaf = new lnode_t();
    auto copy = [num_threads, &leaf](uint64_t tid, bool flag){
	size_t chunk_size;
	size_t chunk;
	if(flag){
	    //chunk_size = 512;
	    //chunk = LNODE_SIZE/2 / chunk_size;
	    auto lnode = new lnode_t();
	    chunk = lnode_t::cardinality;
	    for(auto i=0; i<lnode_t::cardinality; i+=2){
		memcpy(&lnode->entry[i], &leaf->entry[i], sizeof(entry_t));
	    }
	    /*
	    for(auto i=0; i<chunk; i++){
		memcpy((void*)((uint64_t)lnode+(chunk_size*i)), (void*)((uint64_t)leaf+(chunk_size*i)), chunk_size);
	    }*/
	    leaf = lnode;
	}
	else{
	    chunk_size = LNODE_SIZE;
	    chunk = LNODE_SIZE / chunk_size;
	    auto lnode = new lnode_t();
	    memcpy(lnode, leaf, sizeof(lnode_t));
	    leaf = lnode;
	}
	
    };
    struct timespec start, end;

    clock_gettime(CLOCK_MONOTONIC, &start);
    if(mode){
	for(int i=0; i<repeat; i++)
	    start_threads(num_threads, copy, true);
    }
    else{
	for(int i=0; i<repeat; i++)
	    start_threads(num_threads, copy, false);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);
    if(mode) std::cout << "chunk copy" << std::endl;
    else     std::cout << "bulk copy" << std::endl;
    auto elapsed = (end.tv_nsec - start.tv_nsec) + (end.tv_sec - start.tv_sec)*1000000000;
    std::cout << "elapsed time(usec): " << elapsed/1000.0 << std::endl;
    std::cout << "average time(usec): " << (double)elapsed/1000.0/repeat << std::endl;
    return 0;
}



