#include "tree.h"
#include <vector>
#include <thread>
#include <iostream>
#include <random>
#include <algorithm>
#include <atomic>

using Key_t = uint64_t;
using Value_t = uint64_t;
using namespace BLINK_HASH;

// Global counter — increment inside convert() or convert_worker_loop
// when a conversion succeeds. Defined in tree.cpp under ASYNC_ADAPT.
#ifdef ASYNC_ADAPT
namespace BLINK_HASH {
    extern std::atomic<uint64_t> async_convert_count;
}
#endif

inline void pin_to_core(size_t thread_id){
#ifdef __linux__
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(thread_id % 64, &cpu_set);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
#else
    (void)thread_id;
#endif
}

int main(int argc, char* argv[]){
    if(argc < 3){
        std::cerr << "Usage: " << argv[0]
                  << " <num_data> <num_threads>" << std::endl;
        return 1;
    }

    int num_data    = atoi(argv[1]);
    int num_threads = atoi(argv[2]);
    int range       = 50;

    Key_t* keys = new Key_t[num_data];
    for(int i = 0; i < num_data; i++)
        keys[i] = i + 1;
    std::shuffle(keys, keys + num_data, std::mt19937{std::random_device{}()});

    btree_t<Key_t, Value_t>* tree = new btree_t<Key_t, Value_t>();

    // Insert all keys
    {
        std::vector<std::thread> threads;
        for(int t = 0; t < num_threads; t++){
            threads.emplace_back([&, t]{
                pin_to_core(t);
                size_t chunk = num_data / num_threads;
                size_t from  = chunk * t;
                size_t to    = (t == num_threads - 1) ? num_data : from + chunk;
                for(size_t i = from; i < to; i++){
                    auto ti = tree->getThreadInfo();
                    tree->insert(keys[i], keys[i], ti);
                }
            });
        }
        for(auto& th : threads) th.join();
    }

    std::cout << "Inserted " << num_data << " keys" << std::endl;
    std::cout << "Height before scans: " << tree->height() + 1 << std::endl;

    // Run range queries — this should trigger conversions
    std::atomic<uint64_t> total_queries{0};
    {
        std::vector<std::thread> threads;
        for(int t = 0; t < num_threads; t++){
            threads.emplace_back([&, t]{
                pin_to_core(t);
                std::mt19937 rng(t);
                std::uniform_int_distribution<Key_t> dist(1, num_data);
                for(int q = 0; q < 100000; q++){
                    Key_t min_key = dist(rng);
                    Value_t buf[range];
                    auto ti = tree->getThreadInfo();
                    tree->range_lookup(min_key, range, buf, ti);
                }
                total_queries.fetch_add(100000);
            });
        }
        for(auto& th : threads) th.join();
    }

    std::cout << "Total queries: " << total_queries.load() << std::endl;
    std::cout << "Height after scans: " << tree->height() + 1 << std::endl;

#ifdef ASYNC_ADAPT
    // Allow background workers to finish pending conversions
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    std::cout << "Async conversions completed: "
              << async_convert_count.load() << std::endl;
#else
    std::cout << "(Synchronous mode — conversions happen inline)" << std::endl;
#endif

    delete[] keys;
    return 0;
}