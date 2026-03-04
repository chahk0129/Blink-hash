#include "tree.h"
#include <ctime>
#include <sys/time.h>
#include <vector>
#include <thread>
#include <iostream>
#include <random>
#include <algorithm>
#include <numeric>
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
    if(argc < 4){
        std::cerr << "Usage: " << argv[0]
                  << " <num_data> <num_threads> <num_queries_per_thread>"
                  << std::endl;
        return 1;
    }

    int num_data    = atoi(argv[1]);
    int num_threads = atoi(argv[2]);
    int num_queries = atoi(argv[3]);
    int range       = 50;

    // --- Generate keys ---
    Key_t* keys = new Key_t[num_data];
    for(int i = 0; i < num_data; i++)
        keys[i] = i + 1;
    std::shuffle(keys, keys + num_data, std::mt19937{std::random_device{}()});

    // --- Build tree (insert-only, no conversion) ---
    btree_t<Key_t, Value_t>* tree = new btree_t<Key_t, Value_t>();

    // Parallel insert
    int warmup_threads = std::min(num_threads, 64);
    {
        std::vector<std::thread> threads;
        for(int t = 0; t < warmup_threads; t++){
            threads.emplace_back([&, t]{
                pin_to_core(t);
                size_t chunk = num_data / warmup_threads;
                size_t from  = chunk * t;
                size_t to    = (t == warmup_threads - 1) ? num_data : from + chunk;
                for(size_t i = from; i < to; i++){
                    auto ti = tree->getThreadInfo();
                    tree->insert(keys[i], keys[i], ti);
                }
            });
        }
        for(auto& th : threads) th.join();
    }

    std::cout << "Inserted " << num_data << " keys, height = "
              << tree->height() + 1 << std::endl;

    // --- Range query latency benchmark ---
    // Each thread does num_queries range scans and records per-query latency
    std::vector<std::vector<uint64_t>> latencies(num_threads);
    for(auto& v : latencies) v.resize(num_queries);

    {
        std::vector<std::thread> threads;
        for(int t = 0; t < num_threads; t++){
            threads.emplace_back([&, t]{
                pin_to_core(t);
                std::mt19937 rng(t * 12345 + 67890);
                std::uniform_int_distribution<Key_t> dist(1, num_data);

                for(int q = 0; q < num_queries; q++){
                    Key_t min_key = dist(rng);
                    Value_t buf[range];

                    auto ti = tree->getThreadInfo();
                    uint64_t t0 = now_ns();
                    tree->range_lookup(min_key, range, buf, ti);
                    uint64_t t1 = now_ns();

                    latencies[t][q] = t1 - t0;
                }
            });
        }
        for(auto& th : threads) th.join();
    }

    // --- Merge and report ---
    std::vector<uint64_t> all;
    all.reserve(num_threads * num_queries);
    for(auto& v : latencies)
        all.insert(all.end(), v.begin(), v.end());
    std::sort(all.begin(), all.end());

    size_t n = all.size();
    auto percentile = [&](double p) -> uint64_t {
        size_t idx = (size_t)(p / 100.0 * n);
        if(idx >= n) idx = n - 1;
        return all[idx];
    };

    double mean = std::accumulate(all.begin(), all.end(), 0.0) / n;

    std::cout << "\n=== Range Query Latency (" << n << " queries, range="
              << range << ") ===" << std::endl;
    std::cout << "  mean:  " << (uint64_t)mean << " ns" << std::endl;
    std::cout << "  p50:   " << percentile(50)  << " ns" << std::endl;
    std::cout << "  p90:   " << percentile(90)  << " ns" << std::endl;
    std::cout << "  p99:   " << percentile(99)  << " ns" << std::endl;
    std::cout << "  p99.9: " << percentile(99.9) << " ns" << std::endl;
    std::cout << "  max:   " << all.back()       << " ns" << std::endl;

    // Count spike queries (> 10× median)
    uint64_t spike_threshold = percentile(50) * 10;
    int spikes = 0;
    for(auto& l : all)
        if(l > spike_threshold) spikes++;
    std::cout << "  spikes (>10x median): " << spikes
              << " (" << (double)spikes / n * 100 << "%)" << std::endl;

    delete[] keys;
    return 0;
}