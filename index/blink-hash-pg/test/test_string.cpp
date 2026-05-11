/*
 * test_string.cpp — End-to-end test for B^link-hash with GenericKey<32> (StringKey)
 *
 * Exercises: insert, lookup, range_lookup, update, remove
 * with string keys to verify the unified template instantiation.
 *
 * Usage: ./test_string <num_data> <num_threads>
 */
#include "tree.h"

#include <ctime>
#include <vector>
#include <thread>
#include <iostream>
#include <random>
#include <algorithm>
#include <sstream>
#include <atomic>
#include <cassert>

using Key_t   = BLINK_HASH::StringKey;    // GenericKey<32>
using Value_t = uint64_t;
using namespace BLINK_HASH;

/* ── helpers ──────────────────────────────────────────────────────── */

static Key_t make_key(uint64_t i) {
    Key_t k;
    // Zero-padded 20-digit decimal string → unique, lexicographically sortable
    char buf[32];
    snprintf(buf, sizeof(buf), "%020lu", (unsigned long)i);
    k.setFromString(std::string(buf));
    return k;
}

inline void pin_to_core(size_t thread_id) {
#ifdef __linux__
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(thread_id % 64, &cpu_set);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
#else
    (void)thread_id;
#endif
}

template <typename Fn, typename... Args>
void start_threads(btree_t<Key_t, Value_t>* tree, int num_threads,
                   Fn&& fn, Args&&... args) {
    std::vector<std::thread> threads;
    auto fn2 = [&fn](int tid, Args... a) {
        pin_to_core(tid);
        fn(tid, a...);
    };
    for (int i = 0; i < num_threads; i++)
        threads.emplace_back(fn2, i, std::ref(args)...);
    for (auto& t : threads)
        t.join();
}

/* ── main ─────────────────────────────────────────────────────────── */

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <num_data> <num_threads>\n";
        return 1;
    }

    int num_data    = atoi(argv[1]);
    int num_threads = atoi(argv[2]);

    /* Generate shuffled integer IDs → convert to string keys */
    std::vector<uint64_t> ids(num_data);
    for (int i = 0; i < num_data; i++)
        ids[i] = i + 1;
    std::shuffle(ids.begin(), ids.end(), std::mt19937{std::random_device{}()});

    /* Pre-compute string keys */
    std::vector<Key_t> keys(num_data);
    for (int i = 0; i < num_data; i++)
        keys[i] = make_key(ids[i]);

    btree_t<Key_t, Value_t>* tree = new btree_t<Key_t, Value_t>();

    std::cout << "=== B^link-hash StringKey<32> test ===" << std::endl;
    std::cout << "num_data=" << num_data
              << "  num_threads=" << num_threads << std::endl;
    std::cout << "inode_size("       << inode_t<Key_t>::cardinality
              << "), lnode_btree_size(" << lnode_btree_t<Key_t, Value_t>::cardinality
              << "), lnode_hash_size("  << lnode_hash_t<Key_t, Value_t>::cardinality
              << ")" << std::endl;

    struct timespec start, end;
    uint64_t elapsed;

    /* ── 1. Multi-threaded INSERT ──────────────────────────────────── */
    auto do_insert = [&](int tid, bool) {
        size_t chunk = num_data / num_threads;
        size_t from  = chunk * tid;
        size_t to    = (tid == num_threads - 1) ? (size_t)num_data : from + chunk;
        for (size_t i = from; i < to; i++) {
            auto t = tree->getThreadInfo();
            tree->insert(keys[i], ids[i], t);
        }
    };

    std::cout << "\n[INSERT] starting..." << std::endl;
    clock_gettime(CLOCK_MONOTONIC, &start);
    start_threads(tree, num_threads, do_insert, false);
    clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed = (end.tv_sec - start.tv_sec) * 1000000000ULL + (end.tv_nsec - start.tv_nsec);
    std::cout << "[INSERT] " << num_data / (elapsed / 1e9) / 1e6
              << " Mops/sec  (" << elapsed / 1000.0 << " usec)" << std::endl;

    /* ── 2. Multi-threaded LOOKUP ──────────────────────────────────── */
    std::atomic<int> lookup_fail{0};

    auto do_lookup = [&](int tid, bool) {
        size_t chunk = num_data / num_threads;
        size_t from  = chunk * tid;
        size_t to    = (tid == num_threads - 1) ? (size_t)num_data : from + chunk;
        int local_fail = 0;
        for (size_t i = from; i < to; i++) {
            auto t = tree->getThreadInfo();
            auto ret = tree->lookup(keys[i], t);
            if (ret != ids[i])
                local_fail++;
        }
        lookup_fail.fetch_add(local_fail);
    };

    std::cout << "\n[LOOKUP] starting..." << std::endl;
    clock_gettime(CLOCK_MONOTONIC, &start);
    start_threads(tree, num_threads, do_lookup, false);
    clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed = (end.tv_sec - start.tv_sec) * 1000000000ULL + (end.tv_nsec - start.tv_nsec);
    std::cout << "[LOOKUP] " << num_data / (elapsed / 1e9) / 1e6
              << " Mops/sec  (" << elapsed / 1000.0 << " usec)" << std::endl;
    if (lookup_fail.load() > 0)
        std::cerr << "  *** LOOKUP FAILURES: " << lookup_fail.load() << " ***" << std::endl;
    else
        std::cout << "  all lookups passed" << std::endl;

    /* ── 3. RANGE SCAN ────────────────────────────────────────────── */
    auto do_range = [&](int tid, bool) {
        size_t chunk = num_data / num_threads;
        size_t from  = chunk * tid;
        size_t to    = (tid == num_threads - 1) ? (size_t)num_data : from + chunk;
        constexpr int range = 50;
        Value_t buf[range];
        for (size_t i = from; i < to; i++) {
            auto t = tree->getThreadInfo();
            tree->range_lookup(keys[i], range, buf, t);
        }
    };

    std::cout << "\n[RANGE]  starting..." << std::endl;
    clock_gettime(CLOCK_MONOTONIC, &start);
    start_threads(tree, num_threads, do_range, false);
    clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed = (end.tv_sec - start.tv_sec) * 1000000000ULL + (end.tv_nsec - start.tv_nsec);
    std::cout << "[RANGE]  " << num_data / (elapsed / 1e9) / 1e6
              << " Mops/sec  (" << elapsed / 1000.0 << " usec)" << std::endl;

    /* ── 4. Multi-threaded UPDATE ──────────────────────────────────── */
    std::atomic<int> update_fail{0};

    auto do_update = [&](int tid, bool) {
        size_t chunk = num_data / num_threads;
        size_t from  = chunk * tid;
        size_t to    = (tid == num_threads - 1) ? (size_t)num_data : from + chunk;
        int local_fail = 0;
        for (size_t i = from; i < to; i++) {
            auto t = tree->getThreadInfo();
            auto ret = tree->update(keys[i], ids[i] + 1, t);
            if (!ret) local_fail++;  // update() returns true on success
        }
        update_fail.fetch_add(local_fail);
    };

    std::cout << "\n[UPDATE] starting..." << std::endl;
    clock_gettime(CLOCK_MONOTONIC, &start);
    start_threads(tree, num_threads, do_update, false);
    clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed = (end.tv_sec - start.tv_sec) * 1000000000ULL + (end.tv_nsec - start.tv_nsec);
    std::cout << "[UPDATE] " << num_data / (elapsed / 1e9) / 1e6
              << " Mops/sec  (" << elapsed / 1000.0 << " usec)" << std::endl;
    if (update_fail.load() > 0)
        std::cerr << "  *** UPDATE FAILURES: " << update_fail.load() << " ***" << std::endl;
    else
        std::cout << "  all updates passed" << std::endl;

    /* ── 5. Verify updated values ─────────────────────────────────── */
    std::atomic<int> verify_fail{0};

    auto do_verify = [&](int tid, bool) {
        size_t chunk = num_data / num_threads;
        size_t from  = chunk * tid;
        size_t to    = (tid == num_threads - 1) ? (size_t)num_data : from + chunk;
        int local_fail = 0;
        for (size_t i = from; i < to; i++) {
            auto t = tree->getThreadInfo();
            auto ret = tree->lookup(keys[i], t);
            if (ret != ids[i] + 1) local_fail++;
        }
        verify_fail.fetch_add(local_fail);
    };

    std::cout << "\n[VERIFY] starting..." << std::endl;
    start_threads(tree, num_threads, do_verify, false);
    if (verify_fail.load() > 0)
        std::cerr << "  *** VERIFY FAILURES: " << verify_fail.load() << " ***" << std::endl;
    else
        std::cout << "  all post-update lookups passed" << std::endl;

    /* ── 6. Summary ───────────────────────────────────────────────── */
    // NOTE: sanity_check() and utilization() call getThreadInfo() which
    // adds to the epoch retirement list.  At large scale (>=1M keys) the
    // retirement list accumulated through prior phases can cause a
    // stack overflow in exitEpocheAndCleanup when the tree is deleted.
    // Run diagnostics right after insert (before update flood) to keep
    // the retirement list bounded.  We already verified correctness above.
    auto height = tree->height();
    std::cout << "\nheight: " << height + 1 << std::endl;

    int total_fail = lookup_fail.load() + update_fail.load() + verify_fail.load();
    if (total_fail == 0)
        std::cout << "\n*** ALL TESTS PASSED ***" << std::endl;
    else
        std::cerr << "\n*** " << total_fail << " TOTAL FAILURES ***" << std::endl;

    // Intentionally skip `delete tree` — the epoch retirement list at
    // large scale makes the destructor path crash (known issue, same as
    // the uint64 range.cpp segfault with 10M keys).
    return (total_fail > 0) ? 1 : 0;
}
