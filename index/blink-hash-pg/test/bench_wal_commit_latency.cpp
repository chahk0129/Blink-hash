/*
 * bench_wal_commit_latency.cpp — Measure commit latency through the
 * full WAL pipeline: serialize → ThreadBuf → Ring → Flusher → disk.
 *
 * Measures:
 *   - p50, p90, p99, p99.9 latency for "insert + wait for durability"
 *   - Throughput (ops/sec)
 *
 * Build:
 *   cd index/blink-hash-pg/build && cmake .. && make bench_wal_commit_latency
 * Run:
 *   ./test/bench_wal_commit_latency [num_threads] [records_per_thread] [batch_size]
 *
 * Defaults: 8 threads, 100K records each, batch=100.
 */

#include "wal_record.h"
#include "wal_thread_buf.h"
#include "wal_ring.h"
#include "wal_flusher.h"
#include "wal_group_commit.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>
#include <string>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#include <numeric>

using namespace BLINK_HASH::WAL;

static constexpr size_t RING_CAP = 128ULL * 1024 * 1024;

/* ── temp dir ──────────────────────────────────────────────────── */

static std::string make_temp_dir() {
    char tmpl[] = "/tmp/wal_bench_XXXXXX";
    char* dir = ::mkdtemp(tmpl);
    assert(dir);
    return std::string(dir);
}

static void rm_rf(const std::string& path) {
    DIR* d = ::opendir(path.c_str());
    if (!d) return;
    struct dirent* ent;
    while ((ent = ::readdir(d)) != nullptr) {
        if (std::strcmp(ent->d_name, ".") == 0 ||
            std::strcmp(ent->d_name, "..") == 0)
            continue;
        ::unlink((path + "/" + ent->d_name).c_str());
    }
    ::closedir(d);
    ::rmdir(path.c_str());
}

/* ── global state ──────────────────────────────────────────────── */

static RingBuffer*  g_ring;
static Flusher*     g_flusher;
static GroupCommit* g_gc;

static std::atomic<bool> notifier_running{false};

static void gc_notifier_fn() {
    while (notifier_running.load(std::memory_order_acquire)) {
        uint64_t cur = g_flusher->flushed_lsn();
        if (cur > 0)
            g_gc->notify_flushed(cur);
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    g_gc->notify_flushed(g_flusher->flushed_lsn());
}

/* ── bench worker ──────────────────────────────────────────────── */

struct Latencies {
    std::vector<double> us;
};

static void bench_worker(int tid, int records_per, int batch_sz, Latencies* lat) {
    ThreadBuf tb;
    lat->us.reserve(records_per);

    uint64_t max_lsn_in_batch = 0;

    for (int i = 0; i < records_per; ++i) {
        auto t0 = std::chrono::steady_clock::now();

        /* Build record */
        InsertPayload ip;
        ip.node_id    = static_cast<uint64_t>(tid);
        ip.bucket_idx = static_cast<uint32_t>(i);
        ip.key_len    = 8;

        uint64_t key = static_cast<uint64_t>(tid) * records_per + i;
        uint64_t val = key;

        char payload[sizeof(InsertPayload) + 16];
        std::memcpy(payload, &ip, sizeof(ip));
        std::memcpy(payload + sizeof(ip), &key, 8);
        std::memcpy(payload + sizeof(ip) + 8, &val, 8);

        size_t payload_sz = sizeof(InsertPayload) + 16;
        uint64_t lsn = key + 1;   /* LSN must be > 0 */

        char record[256];
        size_t rec_sz = wal_record_serialize(RecordType::INSERT, lsn,
                                             payload, payload_sz, record);

        /* Append to thread buf, auto-flush at threshold */
        tb.append_and_maybe_flush(record, rec_sz, *g_ring);

        max_lsn_in_batch = std::max(max_lsn_in_batch, lsn);

        /* Every batch_sz records: flush + wait for durability.
         * This is the "group commit" pattern — multiple records
         * share a single fsync. */
        bool is_batch_end = ((i + 1) % batch_sz == 0) || (i == records_per - 1);

        if (is_batch_end) {
            tb.flush(*g_ring);
            g_gc->wait_for_lsn(max_lsn_in_batch);

            auto t1 = std::chrono::steady_clock::now();
            double us = std::chrono::duration<double, std::micro>(t1 - t0).count();

            /* Amortize: assign the batch latency to each record in the batch */
            int batch_count = std::min(batch_sz, i + 1);
            double per_record = us / batch_count;
            for (int j = 0; j < batch_count; ++j)
                lat->us.push_back(per_record);

            max_lsn_in_batch = 0;
        }
    }

    tb.drain(*g_ring);
}

/* ── main ──────────────────────────────────────────────────────── */

int main(int argc, char** argv) {
    int num_threads = (argc > 1) ? std::atoi(argv[1]) : 8;
    int records_per = (argc > 2) ? std::atoi(argv[2]) : 100000;
    int batch_sz    = (argc > 3) ? std::atoi(argv[3]) : 100;

    printf("bench_wal_commit_latency: %d threads × %dK records, batch=%d\n",
           num_threads, records_per / 1000, batch_sz);

    std::string wal_dir = make_temp_dir();
    RingBuffer ring(RING_CAP);
    Flusher flusher(wal_dir, ring);
    GroupCommit gc;

    g_ring    = &ring;
    g_flusher = &flusher;
    g_gc      = &gc;

    flusher.start();

    notifier_running.store(true, std::memory_order_release);
    std::thread notifier(gc_notifier_fn);

    auto t0 = std::chrono::steady_clock::now();

    std::vector<Latencies> latencies(num_threads);
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t)
        threads.emplace_back(bench_worker, t, records_per, batch_sz,
                             &latencies[t]);

    for (auto& th : threads)
        th.join();

    auto t1 = std::chrono::steady_clock::now();

    notifier_running.store(false, std::memory_order_release);
    notifier.join();
    flusher.stop();

    /* ── aggregate latencies ── */
    std::vector<double> all_us;
    for (auto& lat : latencies)
        all_us.insert(all_us.end(), lat.us.begin(), lat.us.end());

    std::sort(all_us.begin(), all_us.end());

    size_t n = all_us.size();
    double total_sec = std::chrono::duration<double>(t1 - t0).count();

    printf("\n--- Results (batch=%d) ---\n", batch_sz);
    printf("Total records : %zu\n", n);
    printf("Elapsed       : %.2f s\n", total_sec);
    printf("Throughput    : %.0f ops/s\n", n / total_sec);
    printf("\nPer-record latency (amortized over batch):\n");
    printf("  p50  : %8.1f µs\n", all_us[n * 50 / 100]);
    printf("  p90  : %8.1f µs\n", all_us[n * 90 / 100]);
    printf("  p99  : %8.1f µs\n", all_us[n * 99 / 100]);
    printf("  p99.9: %8.1f µs\n", all_us[std::min(n - 1, n * 999 / 1000)]);
    printf("  max  : %8.1f µs\n", all_us.back());
    printf("  mean : %8.1f µs\n",
           std::accumulate(all_us.begin(), all_us.end(), 0.0) / n);

    rm_rf(wal_dir);
    return 0;
}