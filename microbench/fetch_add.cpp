#include <cstdlib>
#include <unistd.h>
#include <vector>
#include <thread>
#include <cstdint>
#include <atomic>
#include <iostream>

int main(int argc, char* argv[]){
    int num_ops = atoi(argv[1]);
    int num_threads = atoi(argv[2]);

    std::atomic<uint64_t> data;
    data.store(0, std::memory_order_relaxed);

    auto fetch_add = [&data](size_t chunk){
	for(int i=0; i<chunk; i++){
	    data.fetch_add(1, std::memory_order_relaxed);
	}
    };

 
    size_t chunk = num_ops / num_threads;
    std::vector<std::thread> threads;

    for(int i=0; i<num_threads; i++)
	threads.emplace_back(std::thread(fetch_add, chunk));

    for(auto& t: threads) t.join();

    auto ret = data.load();
    std::cout << "after fetching: " << ret << std::endl;
    return 0;
}

