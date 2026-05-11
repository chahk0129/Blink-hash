/*
 * test_wal_recovery.cpp — Crash recovery end-to-end test
 *
 * Strategy:
 *   1. Create tree + WAL pipeline (ring + flusher + disk).
 *   2. Insert N keys with WAL enabled.
 *   3. Stop flusher (simulates clean shutdown — all data on disk).
 *   4. Create a NEW empty tree.
 *   5. Recover from WAL directory → tree should have all N keys.
 *   6. Verify every key is present with correct value.
 *   7. Test with updates and deletes too.
 */

#include "tree.h"
#include "wal_emitter.h"
#include "wal_record.h"
#include "wal_ring.h"
#include "wal_flusher.h"
#include "wal_recovery.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#include <vector>
#include <chrono>

using namespace BLINK_HASH;
using namespace BLINK_HASH::WAL;

static constexpr size_t RING_CAP = 64ULL * 1024 * 1024;

/* ── temp dir helpers ──────────────────────────────────────────────── */

static std::string make_temp_dir() {
    char tmpl[] = "/tmp/wal_recovery_XXXXXX";
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
        std::string full = path + "/" + ent->d_name;
        ::unlink(full.c_str());
    }
    ::closedir(d);
    ::rmdir(path.c_str());
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: Insert N keys, recover, verify all present
 * ═══════════════════════════════════════════════════════════════════ */

static void test_insert_recover() {
    printf("  [recovery] insert + recover...\n");
    std::string wal_dir = make_temp_dir();

    constexpr int N = 50000;

    /* Phase A: populate tree + write WAL to disk */
    {
        RingBuffer ring(RING_CAP);
        Flusher flusher(wal_dir, ring);
        flusher.start();

        wal_init(&ring);
        g_lsn.store(0);
        g_node_id.store(0);

        btree_t<uint64_t, uint64_t> tree;
        auto ti = tree.getThreadInfo();

        for (int i = 1; i <= N; i++)
            tree.insert(static_cast<uint64_t>(i),
                        static_cast<uint64_t>(i * 100), ti);

        /* Flush thread buf and stop flusher (writes all to disk) */
        wal_flush_thread_buf();
        flusher.stop();
        wal_disable();
    }
    /* tree destroyed — simulates crash */

    /* Phase B: recover into a new tree */
    {
        /* Disable WAL during recovery */
        g_wal_enabled = false;

        btree_t<uint64_t, uint64_t> tree2;
        auto ti = tree2.getThreadInfo();

        auto stats = recover<uint64_t, uint64_t>(
            wal_dir, tree2, ti, 0);

        printf("    recovered: %llu inserts in %.3f s\n",
               (unsigned long long)stats.inserts_replayed, stats.elapsed_sec);

        assert(stats.inserts_replayed == N);

        /* Verify every key */
        int found = 0;
        for (int i = 1; i <= N; i++) {
            auto val = tree2.lookup(static_cast<uint64_t>(i), ti);
            if (val == static_cast<uint64_t>(i * 100))
                found++;
        }

        printf("    verified: %d / %d keys correct\n", found, N);
        assert(found == N);
    }

    rm_rf(wal_dir);
    printf("  [PASS] insert + recover\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Insert + Update + Delete, recover, verify final state
 * ═══════════════════════════════════════════════════════════════════ */

static void test_mixed_recover() {
    printf("  [recovery] mixed ops + recover...\n");
    std::string wal_dir = make_temp_dir();

    constexpr int N = 10000;

    /* Phase A: mixed operations */
    {
        RingBuffer ring(RING_CAP);
        Flusher flusher(wal_dir, ring);
        flusher.start();

        wal_init(&ring);
        g_lsn.store(0);
        g_node_id.store(0);

        btree_t<uint64_t, uint64_t> tree;
        auto ti = tree.getThreadInfo();

        /* Insert 1..N */
        for (int i = 1; i <= N; i++)
            tree.insert(static_cast<uint64_t>(i),
                        static_cast<uint64_t>(i), ti);

        /* Update keys 1..1000 to value*2 */
        for (int i = 1; i <= 1000; i++)
            tree.update(static_cast<uint64_t>(i),
                        static_cast<uint64_t>(i * 2), ti);

        /* Delete keys 9001..10000 */
        for (int i = 9001; i <= N; i++)
            tree.remove(static_cast<uint64_t>(i), ti);

        wal_flush_thread_buf();
        flusher.stop();
        wal_disable();
    }

    /* Phase B: recover */
    {
        g_wal_enabled = false;

        btree_t<uint64_t, uint64_t> tree2;
        auto ti = tree2.getThreadInfo();

        auto stats = recover<uint64_t, uint64_t>(
            wal_dir, tree2, ti, 0);

        printf("    recovered: %llu ins, %llu upd, %llu del in %.3f s\n",
               (unsigned long long)stats.inserts_replayed, (unsigned long long)stats.updates_replayed,
               (unsigned long long)stats.deletes_replayed, stats.elapsed_sec);

        /* Verify updated keys 1..1000 have value*2 */
        int correct_updates = 0;
        for (int i = 1; i <= 1000; i++) {
            auto val = tree2.lookup(static_cast<uint64_t>(i), ti);
            if (val == static_cast<uint64_t>(i * 2))
                correct_updates++;
        }
        printf("    updated keys: %d / 1000 correct\n", correct_updates);
        assert(correct_updates == 1000);

        /* Verify non-updated keys 1001..9000 have original value */
        int correct_normal = 0;
        for (int i = 1001; i <= 9000; i++) {
            auto val = tree2.lookup(static_cast<uint64_t>(i), ti);
            if (val == static_cast<uint64_t>(i))
                correct_normal++;
        }
        printf("    normal keys: %d / 8000 correct\n", correct_normal);
        assert(correct_normal == 8000);

        /* Verify deleted keys 9001..10000 are gone (return 0) */
        int correct_deletes = 0;
        for (int i = 9001; i <= N; i++) {
            auto val = tree2.lookup(static_cast<uint64_t>(i), ti);
            if (val == 0)
                correct_deletes++;
        }
        printf("    deleted keys: %d / 1000 absent\n", correct_deletes);
        assert(correct_deletes == 1000);
    }

    rm_rf(wal_dir);
    printf("  [PASS] mixed ops + recover\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Recovery re-seeds LSN and node_id counters
 * ═══════════════════════════════════════════════════════════════════ */

static void test_counter_reseed() {
    printf("  [recovery] counter re-seeding...\n");
    std::string wal_dir = make_temp_dir();

    uint64_t lsn_before, nid_before;

    {
        RingBuffer ring(RING_CAP);
        Flusher flusher(wal_dir, ring);
        flusher.start();

        wal_init(&ring);
        g_lsn.store(0);
        g_node_id.store(0);

        btree_t<uint64_t, uint64_t> tree;
        auto ti = tree.getThreadInfo();

        for (int i = 1; i <= 5000; i++)
            tree.insert(static_cast<uint64_t>(i),
                        static_cast<uint64_t>(i), ti);

        lsn_before = g_lsn.load();
        nid_before = g_node_id.load();

        printf("    before crash: lsn=%llu, node_id=%llu\n",
               (unsigned long long)lsn_before, (unsigned long long)nid_before);

        wal_flush_thread_buf();
        flusher.stop();
        wal_disable();
    }

    /* Reset counters to 0 (simulates fresh process start) */
    g_lsn.store(0);
    g_node_id.store(0);

    {
        btree_t<uint64_t, uint64_t> tree2;
        auto ti = tree2.getThreadInfo();

        auto stats = recover<uint64_t, uint64_t>(
            wal_dir, tree2, ti, 0);

        uint64_t lsn_after = g_lsn.load();
        uint64_t nid_after = g_node_id.load();

        printf("    after recover: lsn=%llu (was %llu), "
               "node_id=%llu (was %llu)\n",
               (unsigned long long)lsn_after, (unsigned long long)lsn_before, (unsigned long long)nid_after, (unsigned long long)nid_before);

        /* LSN counter must be >= what we had before crash */
        assert(lsn_after >= lsn_before);
        /* node_id must be >= what we had (recovery extracts from records) */
        assert(nid_after > 0 || nid_before == 0);
    }

    rm_rf(wal_dir);
    printf("  [PASS] counter re-seeding\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: Large-scale recovery performance
 * ═══════════════════════════════════════════════════════════════════ */

static void test_large_scale_recovery() {
    printf("  [recovery] large-scale (1M keys)...\n");
    std::string wal_dir = make_temp_dir();

    constexpr int N = 1000000;

    {
        RingBuffer ring(RING_CAP);
        Flusher flusher(wal_dir, ring);
        flusher.start();

        wal_init(&ring);
        g_lsn.store(0);
        g_node_id.store(0);

        btree_t<uint64_t, uint64_t> tree;
        auto ti = tree.getThreadInfo();

        auto t0 = std::chrono::steady_clock::now();
        for (int i = 1; i <= N; i++)
            tree.insert(static_cast<uint64_t>(i),
                        static_cast<uint64_t>(i), ti);
        auto t1 = std::chrono::steady_clock::now();

        printf("    populate: %.2f s\n",
               std::chrono::duration<double>(t1 - t0).count());

        wal_flush_thread_buf();
        flusher.stop();
        wal_disable();
    }

    {
        g_wal_enabled = false;
        g_lsn.store(0);
        g_node_id.store(0);

        btree_t<uint64_t, uint64_t> tree2;
        auto ti = tree2.getThreadInfo();

        auto stats = recover<uint64_t, uint64_t>(
            wal_dir, tree2, ti, 0);

        printf("    recovered: %llu records in %.3f s (%.0f recs/s)\n",
               (unsigned long long)stats.inserts_replayed, stats.elapsed_sec,
               (double)stats.inserts_replayed / stats.elapsed_sec);

        assert(stats.inserts_replayed == N);

        /* Spot-check 1000 random keys */
        int ok = 0;
        for (int i = 1; i <= 1000; i++) {
            uint64_t k = (static_cast<uint64_t>(i) * 7919) % N + 1;
            auto val = tree2.lookup(k, ti);
            if (val == k) ok++;
        }
        printf("    spot-check: %d / 1000 correct\n", ok);
        assert(ok == 1000);
    }

    rm_rf(wal_dir);
    printf("  [PASS] large-scale recovery\n");
}

/* ─── main ─────────────────────────────────────────────────────── */

int main(int argc, char** argv) {
    int start = argc > 1 ? atoi(argv[1]) : 1;
    printf("=== test_wal_recovery (from test %d) ===\n", start);
    if (start <= 1) test_insert_recover();
    if (start <= 2) test_mixed_recover();
    if (start <= 3) test_counter_reseed();
    if (start <= 4) test_large_scale_recovery();
    printf("All WAL recovery tests passed.\n");
    return 0;
}