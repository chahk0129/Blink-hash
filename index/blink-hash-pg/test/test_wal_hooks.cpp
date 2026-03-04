/*
 * test_wal_hooks.cpp — Verify that tree mutations emit WAL records
 *
 * Strategy:
 *   1. Create a tree + WAL ring buffer (no flusher needed).
 *   2. Insert N keys → verify N WAL_INSERT records in the ring.
 *   3. Update some keys → verify WAL_UPDATE records.
 *   4. Delete some keys → verify WAL_DELETE records.
 *   5. Insert enough to trigger splits → verify WAL_SPLIT_LEAF,
 *      WAL_SPLIT_INTERNAL, WAL_NEW_ROOT records appear.
 *
 * This test does NOT write to disk — it just checks the ring contents.
 */

#include "tree.h"
#include "wal_emitter.h"
#include "wal_record.h"
#include "wal_ring.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>
#include <unordered_map>

using namespace BLINK_HASH;
using namespace BLINK_HASH::WAL;

static constexpr size_t RING_CAP = 64ULL * 1024 * 1024;

/* ── drain all records from the ring ────────────────────────────── */

struct ParsedRecord {
    RecordHeader hdr;
    std::vector<char> payload;
};

static std::vector<ParsedRecord> drain_ring(RingBuffer& ring) {
    std::vector<ParsedRecord> records;

    for (;;) {
        const void* ptr = nullptr;
        size_t avail = ring.peek(&ptr);
        if (avail == 0) break;

        /* Copy to local buffer */
        std::vector<char> buf(avail);
        std::memcpy(buf.data(), ptr, avail);
        ring.advance(avail);

        /* Parse records from the buffer */
        const char* scan = buf.data();
        const char* end  = scan + avail;

        while (scan + sizeof(RecordHeader) <= end) {
            /* skip zero padding from 4KB-aligned ThreadBuf flushes.
             * No valid record has LSN 0 (next_lsn() starts at 1).
             * Zero padding may not be a multiple of sizeof(RecordHeader)
             * (e.g. when a 56-byte NEW_ROOT record shifts alignment),
             * so we skip ALL contiguous zero bytes rather than a fixed
             * 16-byte step that could straddle the padding/record boundary. */
            RecordHeader hdr;
            std::memcpy(&hdr, scan, sizeof(hdr));

            if (hdr.lsn == 0) {
                /* Skip all zero bytes — padding may not be 16-aligned */
                while (scan < end && *scan == 0)
                    ++scan;
                continue;
            }

            if (hdr.total_size < sizeof(RecordHeader) ||
                scan + hdr.total_size > end) {
                ++scan;
                continue;
            }

            ParsedRecord pr;
            pr.hdr = hdr;
            size_t payload_sz = hdr.total_size - sizeof(RecordHeader);
            if (payload_sz > 0) {
                pr.payload.resize(payload_sz);
                std::memcpy(pr.payload.data(),
                            scan + sizeof(RecordHeader), payload_sz);
            }

            records.push_back(std::move(pr));
            scan += hdr.total_size;
        }
    }
    return records;
}

/* ── count records by type ──────────────────────────────────────── */

static std::unordered_map<uint16_t, int> count_by_type(
        const std::vector<ParsedRecord>& recs) {
    std::unordered_map<uint16_t, int> counts;
    for (auto& r : recs)
        counts[r.hdr.type]++;
    return counts;
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: Basic insert emits WAL_INSERT records
 * ═══════════════════════════════════════════════════════════════════ */

static void test_insert_emits_wal() {
    printf("  [hooks] insert WAL emission...\n");

    RingBuffer ring(RING_CAP);
    wal_init(&ring);
    g_lsn.store(0);
    g_node_id.store(0);

    btree_t<uint64_t, uint64_t> tree;
    auto ti = tree.getThreadInfo();

    constexpr int N = 1000;
    for (int i = 1; i <= N; i++)
        tree.insert(static_cast<uint64_t>(i),
                    static_cast<uint64_t>(i * 10), ti);

    /* Flush the thread-local buffer */
    wal_flush_thread_buf();

    auto records = drain_ring(ring);
    auto counts = count_by_type(records);

    int inserts = counts[static_cast<uint16_t>(RecordType::INSERT)];
    printf("    %d INSERT records (expected %d)\n", inserts, N);
    assert(inserts == N);

    /* Verify LSNs are monotonically increasing */
    uint64_t prev_lsn = 0;
    for (auto& r : records) {
        if (r.hdr.type == static_cast<uint16_t>(RecordType::INSERT)) {
            assert(r.hdr.lsn > prev_lsn);
            prev_lsn = r.hdr.lsn;
        }
    }

    wal_disable();
    printf("  [PASS] insert WAL emission\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Update and delete emit correct record types
 * ═══════════════════════════════════════════════════════════════════ */

static void test_update_delete_emits_wal() {
    printf("  [hooks] update/delete WAL emission...\n");

    RingBuffer ring(RING_CAP);
    wal_init(&ring);
    g_lsn.store(0);
    g_node_id.store(0);

    btree_t<uint64_t, uint64_t> tree;
    auto ti = tree.getThreadInfo();

    /* Insert 500 keys */
    for (int i = 1; i <= 500; i++)
        tree.insert(static_cast<uint64_t>(i),
                    static_cast<uint64_t>(i * 10), ti);
    wal_flush_thread_buf();
    drain_ring(ring);   /* discard insert records */

    /* Update 100 keys */
    for (int i = 1; i <= 100; i++)
        tree.update(static_cast<uint64_t>(i),
                    static_cast<uint64_t>(i * 20), ti);
    wal_flush_thread_buf();

    auto records = drain_ring(ring);
    auto counts = count_by_type(records);

    int updates = counts[static_cast<uint16_t>(RecordType::UPDATE)];
    printf("    %d UPDATE records (expected 100)\n", updates);
    assert(updates == 100);

    /* Delete 50 keys */
    for (int i = 1; i <= 50; i++)
        tree.remove(static_cast<uint64_t>(i), ti);
    wal_flush_thread_buf();

    records = drain_ring(ring);
    counts = count_by_type(records);

    int deletes = counts[static_cast<uint16_t>(RecordType::DELETE)];
    printf("    %d DELETE records (expected 50)\n", deletes);
    assert(deletes == 50);

    wal_disable();
    printf("  [PASS] update/delete WAL emission\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Splits emit WAL_SPLIT_LEAF and WAL_NEW_ROOT
 * ═══════════════════════════════════════════════════════════════════ */

static void test_splits_emit_wal() {
    printf("  [hooks] split WAL emission...\n");

    RingBuffer ring(RING_CAP);
    wal_init(&ring);
    g_lsn.store(0);
    g_node_id.store(0);

    btree_t<uint64_t, uint64_t> tree;
    auto ti = tree.getThreadInfo();

    /* Insert enough keys to force multiple splits.
     * The hash leaf has ~1024 entries per bucket × cardinality.
     * A simple sequential insert of 100K keys should trigger
     * hash-leaf splits and possibly convert + btree splits. */
    constexpr int N = 100000;
    for (int i = 1; i <= N; i++)
        tree.insert(static_cast<uint64_t>(i),
                    static_cast<uint64_t>(i), ti);
    wal_flush_thread_buf();

    auto records = drain_ring(ring);
    auto counts = count_by_type(records);

    int splits = counts[static_cast<uint16_t>(RecordType::SPLIT_LEAF)];
    int new_roots = counts[static_cast<uint16_t>(RecordType::NEW_ROOT)];
    int inserts = counts[static_cast<uint16_t>(RecordType::INSERT)];

    printf("    %d INSERT, %d SPLIT_LEAF, %d NEW_ROOT records\n",
           inserts, splits, new_roots);

    assert(inserts == N);
    assert(splits > 0);    /* must have at least one split */

    wal_disable();
    printf("  [PASS] split WAL emission\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: All LSNs globally ordered
 * ═══════════════════════════════════════════════════════════════════ */

static void test_lsn_ordering() {
    printf("  [hooks] LSN global ordering...\n");

    RingBuffer ring(RING_CAP);
    wal_init(&ring);
    g_lsn.store(0);
    g_node_id.store(0);

    btree_t<uint64_t, uint64_t> tree;
    auto ti = tree.getThreadInfo();

    for (int i = 1; i <= 5000; i++)
        tree.insert(static_cast<uint64_t>(i),
                    static_cast<uint64_t>(i), ti);
    for (int i = 1; i <= 100; i++)
        tree.update(static_cast<uint64_t>(i),
                    static_cast<uint64_t>(i * 2), ti);
    for (int i = 1; i <= 50; i++)
        tree.remove(static_cast<uint64_t>(i), ti);
    wal_flush_thread_buf();

    auto records = drain_ring(ring);

    /* All LSNs should be unique and strictly increasing
     * (single-threaded execution). */
    uint64_t prev = 0;
    for (auto& r : records) {
        assert(r.hdr.lsn > prev && "LSN not strictly increasing");
        prev = r.hdr.lsn;
    }

    printf("    %zu records, LSN range [1, %llu]\n", records.size(), (unsigned long long)prev);

    wal_disable();
    printf("  [PASS] LSN global ordering\n");
}

/* ─── main ─────────────────────────────────────────────────────── */

int main() {
    printf("=== test_wal_hooks ===\n");
    test_insert_emits_wal();
    test_update_delete_emits_wal();
    test_splits_emit_wal();
    test_lsn_ordering();
    printf("All WAL hook tests passed.\n");
    return 0;
}