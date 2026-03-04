#include "tree.h"
#include <vector>
#include <thread>
#include <iostream>
#include <random>
#include <algorithm>
#include <atomic>
#include <cstring>

using Key_t = uint64_t;
using Value_t = uint64_t;
using namespace BLINK_HASH;

static inline uint64_t now_ns(){
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

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
    if(argc < 5){
        std::cerr << "Usage: " << argv[0]
                  << " <initial_keys> <scan_threads> <insert_threads>"
                  << " <duration_sec>" << std::endl;
        return 1;
    }

    int initial_keys   = atoi(argv[1]);
    int scan_threads   = atoi(argv[2]);
    int insert_threads = atoi(argv[3]);
    int duration_sec   = atoi(argv[4]);
    int range          = 50;

    // --- Generate keys ---
    Key_t* keys = new Key_t[initial_keys];
    for(int i = 0; i < initial_keys; i++)
        keys[i] = i + 1;
    std::shuffle(keys, keys + initial_keys, std::mt19937{std::random_device{}()});

    // --- Build initial tree ---
    btree_t<Key_t, Value_t>* tree = new btree_t<Key_t, Value_t>();
    {
        std::vector<std::thread> threads;
        for(int t = 0; t < std::min(insert_threads, 64); t++){
            threads.emplace_back([&, t]{
                pin_to_core(t);
                size_t chunk = initial_keys / std::min(insert_threads, 64);
                size_t from  = chunk * t;
                size_t to    = (t == std::min(insert_threads, 64) - 1)
                               ? initial_keys : from + chunk;
                for(size_t i = from; i < to; i++){
                    auto ti = tree->getThreadInfo();
                    tree->insert(keys[i], keys[i], ti);
                }
            });
        }
        for(auto& th : threads) th.join();
    }

    std::cout << "Initial: " << initial_keys << " keys, height = "
              << tree->height() + 1 << std::endl;

    // --- Mixed workload ---
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> total_scans{0};
    std::atomic<uint64_t> total_inserts{0};
    std::atomic<Key_t> next_key{(Key_t)initial_keys + 1};

    // Scan threads
    std::vector<std::thread> scanners;
    for(int t = 0; t < scan_threads; t++){
        scanners.emplace_back([&, t]{
            pin_to_core(t);
            std::mt19937 rng(t * 111 + 222);
            uint64_t local_count = 0;
            while(!stop.load(std::memory_order_relaxed)){
                Key_t min_key = rng() % (next_key.load(std::memory_order_relaxed) - 1) + 1;
                Value_t buf[range];
                auto ti = tree->getThreadInfo();
                tree->range_lookup(min_key, range, buf, ti);
                local_count++;
            }
            total_scans.fetch_add(local_count, std::memory_order_relaxed);
        });
    }

    // Insert threads
    std::vector<std::thread> inserters;
    for(int t = 0; t < insert_threads; t++){
        inserters.emplace_back([&, t]{
            pin_to_core(scan_threads + t);
            uint64_t local_count = 0;
            while(!stop.load(std::memory_order_relaxed)){
                Key_t k = next_key.fetch_add(1, std::memory_order_relaxed);
                auto ti = tree->getThreadInfo();
                tree->insert(k, k, ti);
                local_count++;
            }
            total_inserts.fetch_add(local_count, std::memory_order_relaxed);
        });
    }

    // Timer
    std::this_thread::sleep_for(std::chrono::seconds(duration_sec));
    stop.store(true, std::memory_order_relaxed);

    for(auto& th : scanners)  th.join();
    for(auto& th : inserters) th.join();

    std::cout << "\n=== Mixed Workload (" << duration_sec << "s) ===" << std::endl;
    std::cout << "  scan throughput:   "
              << total_scans.load() / (double)duration_sec / 1e6
              << " Mops/sec" << std::endl;
    std::cout << "  insert throughput: "
              << total_inserts.load() / (double)duration_sec / 1e6
              << " Mops/sec" << std::endl;
    std::cout << "  total keys:        "
              << next_key.load() - 1 << std::endl;
    std::cout << "  height:            "
              << tree->height() + 1 << std::endl;

    delete[] keys;
    return 0;
}