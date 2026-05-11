/*
 * test_wal_checkpoint.cpp — Fuzzy checkpointing end-to-end test
 *
 * Strategy:
 *   1. Create tree + WAL pipeline.
 *   2. Insert N keys.
 *   3. Take a checkpoint (snapshot + manifest).
 *   4. Insert M more keys.
 *   5. Simulate crash (destroy tree + flusher).
 *   6. Recover: load snapshot + replay WAL tail.
 *   7. Verify all N+M keys are present.
 */

#include "tree.h"
#include "wal_emitter.h"
#include "wal_record.h"
#include "wal_ring.h"
#include "wal_flusher.h"
#include "wal_recovery.h"
#include "wal_checkpoint.h"

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

static constexpr size_t RING_CAP = 128ULL * 1024 * 1024;

/* ── temp dir helpers ──────────────────────────────────────────────── */

static std::string make_temp_dir() {
    char tmpl[] = "/tmp/wal_checkpoint_XXXXXX";
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
 *  Test 1: Checkpoint + recover
 *
 *  Insert N, checkpoint, insert M more, crash, recover.
 *  Verify all N+M keys present.
 * ═══════════════════════════════════════════════════════════════════ */

static void test_checkpoint_recover() {
    printf("  [checkpoint] checkpoint + recover...\n");
    std::string wal_dir = make_temp_dir();

    constexpr int N = 50000;   /* keys before checkpoint */
    constexpr int M = 20000;   /* keys after checkpoint  */

    /* Phase A: populate, checkpoint, insert more */
    {
        RingBuffer ring(RING_CAP);
        Flusher flusher(wal_dir, ring);
        flusher.start();

        wal_init(&ring);
        g_lsn.store(0);
        g_node_id.store(0);

        btree_t<uint64_t, uint64_t> tree;
        auto ti = tree.getThreadInfo();

        /* Insert first batch: 1..N */
        for (int i = 1; i <= N; i++)
            tree.insert(static_cast<uint64_t>(i),
                        static_cast<uint64_t>(i * 10), ti);

        printf("    inserted %d keys\n", N);

        /* Take checkpoint */
        Checkpointer ckpt(wal_dir, flusher);
        auto manifest = ckpt.run_checkpoint<uint64_t, uint64_t>(tree, ti);

        printf("    checkpoint at lsn=%llu, %llu entries\n",
               (unsigned long long)manifest.checkpoint_lsn,
               (unsigned long long)manifest.num_entries);

        assert(manifest.num_entries == N);

        /* Insert second batch: N+1..N+M */
        for (int i = N + 1; i <= N + M; i++)
            tree.insert(static_cast<uint64_t>(i),
                        static_cast<uint64_t>(i * 10), ti);

        printf("    inserted %d more keys after checkpoint\n", M);

        wal_flush_thread_buf();
        flusher.stop();
        wal_disable();
    }
    /* tree destroyed — simulates crash */

    /* Phase B: recover from checkpoint + WAL tail */
    {
        g_wal_enabled = false;
        g_lsn.store(0);
        g_node_id.store(0);

        btree_t<uint64_t, uint64_t> tree2;
        auto ti = tree2.getThreadInfo();

        auto stats = recover<uint64_t, uint64_t>(
            wal_dir, tree2, ti, 0);

        printf("    recovered: %llu inserts from WAL in %.3f s\n",
               (unsigned long long)stats.inserts_replayed,
               stats.elapsed_sec);

        /* Verify ALL N+M keys present */
        int found = 0;
        for (int i = 1; i <= N + M; i++) {
            auto val = tree2.lookup(static_cast<uint64_t>(i), ti);
            if (val == static_cast<uint64_t>(i * 10))
                found++;
        }

        printf("    verified: %d / %d keys correct\n", found, N + M);
        assert(found == N + M);
    }

    rm_rf(wal_dir);
    printf("  [PASS] checkpoint + recover\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Manifest persistence
 *
 *  Write a manifest, read it back, verify fields match.
 * ═══════════════════════════════════════════════════════════════════ */

static void test_manifest_roundtrip() {
    printf("  [checkpoint] manifest roundtrip...\n");
    std::string wal_dir = make_temp_dir();

    CheckpointManifest m;
    m.checkpoint_lsn = 123456;
    m.end_lsn        = 234567;
    m.snapshot_file   = "snap_000042.dat";
    m.num_entries     = 9999999;

    bool ok = write_manifest(wal_dir, m);
    assert(ok);

    CheckpointManifest m2;
    ok = read_manifest(wal_dir, &m2);
    assert(ok);

    assert(m2.checkpoint_lsn == m.checkpoint_lsn);
    assert(m2.end_lsn        == m.end_lsn);
    assert(m2.snapshot_file   == m.snapshot_file);
    assert(m2.num_entries     == m.num_entries);

    rm_rf(wal_dir);
    printf("  [PASS] manifest roundtrip\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Snapshot write + load (no WAL, pure snapshot test)
 * ═══════════════════════════════════════════════════════════════════ */

static void test_snapshot_roundtrip() {
    printf("  [checkpoint] snapshot roundtrip...\n");
    std::string wal_dir = make_temp_dir();

    constexpr int N = 100000;

    /* Build a tree (WAL enabled just for node_id allocation) */
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
                    static_cast<uint64_t>(i * 7), ti);

    /* Write snapshot */
    Checkpointer ckpt(wal_dir, flusher);
    auto manifest = ckpt.run_checkpoint<uint64_t, uint64_t>(tree, ti);

    wal_flush_thread_buf();
    flusher.stop();
    wal_disable();

    printf("    snapshot: %llu entries\n",
           (unsigned long long)manifest.num_entries);
    assert(manifest.num_entries == N);

    /* Load into a new tree */
    btree_t<uint64_t, uint64_t> tree2;
    auto ti2 = tree2.getThreadInfo();

    std::string snap_path = wal_dir + "/" + manifest.snapshot_file;
    uint64_t loaded = load_snapshot<uint64_t, uint64_t>(
        snap_path, tree2, ti2);

    printf("    loaded: %llu entries\n", (unsigned long long)loaded);
    assert(loaded == N);

    /* Verify all keys */
    int found = 0;
    for (int i = 1; i <= N; i++) {
        auto val = tree2.lookup(static_cast<uint64_t>(i), ti2);
        if (val == static_cast<uint64_t>(i * 7))
            found++;
    }

    printf("    verified: %d / %d keys correct\n", found, N);
    assert(found == N);

    rm_rf(wal_dir);
    printf("  [PASS] snapshot roundtrip\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: Multiple checkpoints (latest wins)
 * ═══════════════════════════════════════════════════════════════════ */

static void test_multiple_checkpoints() {
    printf("  [checkpoint] multiple checkpoints...\n");
    std::string wal_dir = make_temp_dir();

    constexpr int BATCH = 10000;

    {
        RingBuffer ring(RING_CAP);
        Flusher flusher(wal_dir, ring);
        flusher.start();

        wal_init(&ring);
        g_lsn.store(0);
        g_node_id.store(0);

        btree_t<uint64_t, uint64_t> tree;
        auto ti = tree.getThreadInfo();
        Checkpointer ckpt(wal_dir, flusher);

        /* Insert 10K, checkpoint */
        for (int i = 1; i <= BATCH; i++)
            tree.insert(static_cast<uint64_t>(i),
                        static_cast<uint64_t>(i), ti);
        ckpt.run_checkpoint<uint64_t, uint64_t>(tree, ti);
        printf("    checkpoint 1: 10K keys\n");

        /* Insert 10K more, checkpoint again */
        for (int i = BATCH + 1; i <= 2 * BATCH; i++)
            tree.insert(static_cast<uint64_t>(i),
                        static_cast<uint64_t>(i), ti);
        auto m2 = ckpt.run_checkpoint<uint64_t, uint64_t>(tree, ti);
        printf("    checkpoint 2: 20K keys, snap=%s\n",
               m2.snapshot_file.c_str());

        /* Insert 5K more, no checkpoint */
        for (int i = 2 * BATCH + 1; i <= 2 * BATCH + 5000; i++)
            tree.insert(static_cast<uint64_t>(i),
                        static_cast<uint64_t>(i), ti);

        wal_flush_thread_buf();
        flusher.stop();
        wal_disable();
    }

    /* Recover: should use checkpoint 2 + WAL tail */
    {
        g_wal_enabled = false;
        g_lsn.store(0);
        g_node_id.store(0);

        btree_t<uint64_t, uint64_t> tree2;
        auto ti = tree2.getThreadInfo();

        auto stats = recover<uint64_t, uint64_t>(
            wal_dir, tree2, ti, 0);

        /* Verify all 25K keys */
        int total = 2 * BATCH + 5000;
        int found = 0;
        for (int i = 1; i <= total; i++) {
            auto val = tree2.lookup(static_cast<uint64_t>(i), ti);
            if (val == static_cast<uint64_t>(i))
                found++;
        }

        printf("    verified: %d / %d keys correct\n", found, total);
        assert(found == total);
    }

    rm_rf(wal_dir);
    printf("  [PASS] multiple checkpoints\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: Large checkpoint performance
 * ═══════════════════════════════════════════════════════════════════ */

static void test_checkpoint_performance() {
    printf("  [checkpoint] performance (1M keys)...\n");
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

        /* Checkpoint */
        Checkpointer ckpt(wal_dir, flusher);
        auto t2 = std::chrono::steady_clock::now();
        auto manifest = ckpt.run_checkpoint<uint64_t, uint64_t>(tree, ti);
        auto t3 = std::chrono::steady_clock::now();
        printf("    checkpoint: %.2f s (%llu entries, %.0f entries/s)\n",
               std::chrono::duration<double>(t3 - t2).count(),
               (unsigned long long)manifest.num_entries,
               manifest.num_entries /
               std::chrono::duration<double>(t3 - t2).count());

        wal_flush_thread_buf();
        flusher.stop();
        wal_disable();
    }

    /* Recovery from checkpoint */
    {
        g_wal_enabled = false;
        g_lsn.store(0);
        g_node_id.store(0);

        btree_t<uint64_t, uint64_t> tree2;
        auto ti = tree2.getThreadInfo();

        auto t0 = std::chrono::steady_clock::now();
        auto stats = recover<uint64_t, uint64_t>(
            wal_dir, tree2, ti, 0);
        auto t1 = std::chrono::steady_clock::now();

        printf("    recovery: %.2f s (%llu from snapshot + %llu from WAL)\n",
               std::chrono::duration<double>(t1 - t0).count(),
               (unsigned long long)(N - stats.inserts_replayed),
               (unsigned long long)stats.inserts_replayed);

        /* Spot-check */
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
    printf("  [PASS] checkpoint performance\n");
}

/* ─── main ─────────────────────────────────────────────────────── */

int main(int argc, char** argv) {
    int start = argc > 1 ? atoi(argv[1]) : 1;
    printf("=== test_wal_checkpoint (from test %d) ===\n", start);
    if (start <= 1) test_manifest_roundtrip();
    if (start <= 2) test_snapshot_roundtrip();
    if (start <= 3) test_checkpoint_recover();
    if (start <= 4) test_multiple_checkpoints();
    if (start <= 5) test_checkpoint_performance();
    printf("All checkpoint tests passed.\n");
    return 0;
}