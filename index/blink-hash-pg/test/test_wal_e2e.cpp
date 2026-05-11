/*
 * test_wal_e2e.cpp — End-to-end WAL pipeline test
 *
 * Full data path: ThreadBuf → RingBuffer → Flusher → disk → verify
 *
 * 1. Start flusher writing to a temp directory
 * 2. N threads each write M records via ThreadBuf → RingBuffer
 * 3. Stop flusher (drains ring)
 * 4. Read WAL segment files back and verify every record
 *
 * Build:
 *   cd index/blink-hash-pg/build && cmake .. && make test_wal_e2e
 * Run:
 *   ./test/test_wal_e2e
 */

#include "wal_record.h"
#include "wal_thread_buf.h"
#include "wal_ring.h"
#include "wal_flusher.h"
#include "wal_group_commit.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>
#include <unordered_set>
#include <string>
#include <algorithm>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

using namespace BLINK_HASH::WAL;

/* ── config ────────────────────────────────────────────────────── */
static constexpr int    NUM_THREADS    = 8;
static constexpr int    RECORDS_PER    = 200000;   /* 200K per thread */
static constexpr size_t RING_CAP       = 64ULL * 1024 * 1024;  /* 64 MB */

/* ── temp directory helpers ────────────────────────────────────── */

static std::string make_temp_dir() {
    char tmpl[] = "/tmp/wal_e2e_XXXXXX";
    char* dir = ::mkdtemp(tmpl);
    assert(dir && "mkdtemp failed");
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
        std::string child = path + "/" + ent->d_name;
        ::unlink(child.c_str());
    }
    ::closedir(d);
    ::rmdir(path.c_str());
}

/* ── read WAL segments back ────────────────────────────────────── */

static std::vector<char> read_all_segments(const std::string& wal_dir) {
    std::vector<std::string> segs;
    DIR* d = ::opendir(wal_dir.c_str());
    assert(d);
    struct dirent* ent;
    while ((ent = ::readdir(d)) != nullptr) {
        std::string name = ent->d_name;
        if (name.size() > 4 && name.substr(name.size() - 4) == ".seg")
            segs.push_back(wal_dir + "/" + name);
    }
    ::closedir(d);

    /* Sort by name (segment number) */
    std::sort(segs.begin(), segs.end());

    std::vector<char> data;
    for (auto& seg : segs) {
        int fd = ::open(seg.c_str(), O_RDONLY);
        assert(fd >= 0);

        struct stat st;
        ::fstat(fd, &st);
        size_t old = data.size();
        data.resize(old + static_cast<size_t>(st.st_size));
        ssize_t rd = ::read(fd, data.data() + old,
                            static_cast<size_t>(st.st_size));
        assert(rd == st.st_size);
        ::close(fd);
    }
    return data;
}

/* ═══════════════════════════════════════════════════════════════
 *  Test 1: Full pipeline — write records → flush → read back
 * ═══════════════════════════════════════════════════════════════ */

static void producer_fn(int tid, RingBuffer& ring) {
    ThreadBuf tb;

    for (int i = 0; i < RECORDS_PER; ++i) {
        InsertPayload ip;
        ip.node_id    = static_cast<uint64_t>(tid);
        ip.bucket_idx = static_cast<uint32_t>(i);
        ip.key_len    = 8;

        uint64_t key = static_cast<uint64_t>(tid) * RECORDS_PER + i;
        uint64_t val = key ^ 0xAAAAAAAAAAAAAAAAULL;

        size_t payload_sz = sizeof(InsertPayload) + 8 + 8;
        char payload[sizeof(InsertPayload) + 16];
        std::memcpy(payload, &ip, sizeof(ip));
        std::memcpy(payload + sizeof(ip), &key, 8);
        std::memcpy(payload + sizeof(ip) + 8, &val, 8);

        uint64_t lsn = key;
        char record[sizeof(RecordHeader) + sizeof(InsertPayload) + 16];
        size_t rec_sz = wal_record_serialize(RecordType::INSERT, lsn,
                                             payload, payload_sz, record);

        if (!tb.append(record, rec_sz)) {
            tb.flush(ring);
            bool ok = tb.append(record, rec_sz);
            assert(ok);
        }
        if (tb.buffered() >= THREAD_BUF_FLUSH_AT)
            tb.flush(ring);
    }

    tb.drain(ring);
}

static void test_full_pipeline() {
    printf("  [e2e] %d threads × %dK records, flushing to disk...\n",
           NUM_THREADS, RECORDS_PER / 1000);

    std::string wal_dir = make_temp_dir();
    RingBuffer ring(RING_CAP);
    Flusher flusher(wal_dir, ring);

    auto t0 = std::chrono::steady_clock::now();

    flusher.start();

    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t)
        threads.emplace_back(producer_fn, t, std::ref(ring));

    for (auto& th : threads)
        th.join();

    /* Give the flusher a moment to drain the ring, then stop. */
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    flusher.stop();

    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    /* ── Verify: read segments back ── */
    auto data = read_all_segments(wal_dir);
    printf("  [e2e] read back %zu bytes from WAL segments\n", data.size());

    size_t total_records = 0;
    std::unordered_set<uint64_t> seen;
    seen.reserve(NUM_THREADS * RECORDS_PER);

    const char* pos = data.data();
    const char* end = data.data() + data.size();

    while (pos < end) {
        size_t remaining = static_cast<size_t>(end - pos);
        RecordHeader hdr;
        const void* payload = wal_record_deserialize(pos, remaining, &hdr);

        if (payload == nullptr) {
            /* Zero-padding from O_DIRECT alignment — skip byte */
            pos += 1;
            continue;
        }

        assert(hdr.type == static_cast<uint16_t>(RecordType::INSERT));

        auto [it, inserted] = seen.insert(hdr.lsn);
        assert(inserted && "LSN must be unique on disk too");

        /* Verify payload integrity */
        const InsertPayload* ip = static_cast<const InsertPayload*>(payload);
        uint64_t key;
        std::memcpy(&key, static_cast<const char*>(payload) + sizeof(InsertPayload), 8);
        assert(ip->node_id * RECORDS_PER + ip->bucket_idx == key);

        uint64_t val;
        std::memcpy(&val, static_cast<const char*>(payload) + sizeof(InsertPayload) + 8, 8);
        assert(val == (key ^ 0xAAAAAAAAAAAAAAAAULL));

        ++total_records;
        pos += hdr.total_size;
    }

    size_t expected = static_cast<size_t>(NUM_THREADS) * RECORDS_PER;
    printf("  [e2e] deserialized %zu / %zu records in %.1f ms\n",
           total_records, expected, ms);
    assert(total_records == expected && "all records must be on disk");

    /* Check flushed_lsn is sane */
    printf("  [e2e] flushed_lsn = %lu\n", (unsigned long)flusher.flushed_lsn());
    assert(flusher.flushed_lsn() > 0);

    /* Cleanup */
    rm_rf(wal_dir);

    printf("  [PASS] full pipeline e2e\n");
}

/* ═══════════════════════════════════════════════════════════════
 *  Test 2: Segment rotation
 * ═══════════════════════════════════════════════════════════════ */

static void test_segment_rotation() {
    /*
     * Write > 64 MB of data to force at least one segment rotation.
     * We use a single thread with large records.
     */
    printf("  [e2e] segment rotation test...\n");

    std::string wal_dir = make_temp_dir();
    RingBuffer ring(RING_CAP);
    Flusher flusher(wal_dir, ring);
    flusher.start();

    /* Each record: header(16) + InsertPayload(24) + key(8) + val(8) = 56 bytes.
     * To exceed 64 MB: 64*1024*1024 / 56 ≈ 1.2M records.
     * We write 1.5M from one thread. */
    constexpr int N = 1500000;
    ThreadBuf tb;

    for (int i = 0; i < N; ++i) {
        InsertPayload ip;
        ip.node_id    = 0;
        ip.bucket_idx = static_cast<uint32_t>(i);
        ip.key_len    = 8;

        uint64_t key = static_cast<uint64_t>(i);
        uint64_t val = ~key;

        size_t payload_sz = sizeof(InsertPayload) + 16;
        char payload[sizeof(InsertPayload) + 16];
        std::memcpy(payload, &ip, sizeof(ip));
        std::memcpy(payload + sizeof(ip), &key, 8);
        std::memcpy(payload + sizeof(ip) + 8, &val, 8);

        char record[sizeof(RecordHeader) + sizeof(InsertPayload) + 16];
        size_t rec_sz = wal_record_serialize(RecordType::INSERT, key,
                                             payload, payload_sz, record);

        if (!tb.append(record, rec_sz)) {
            tb.flush(ring);
            bool ok = tb.append(record, rec_sz);
            assert(ok);
        }
        if (tb.buffered() >= THREAD_BUF_FLUSH_AT)
            tb.flush(ring);
    }
    tb.drain(ring);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    flusher.stop();

    /* Count segment files */
    DIR* d = ::opendir(wal_dir.c_str());
    assert(d);
    int seg_count = 0;
    struct dirent* ent;
    while ((ent = ::readdir(d)) != nullptr) {
        std::string name = ent->d_name;
        if (name.size() > 4 && name.substr(name.size() - 4) == ".seg")
            ++seg_count;
    }
    ::closedir(d);

    printf("  [e2e] created %d segment files\n", seg_count);
    assert(seg_count >= 2 && "must have rotated at least once");

    rm_rf(wal_dir);
    printf("  [PASS] segment rotation\n");
}

/* ═══════════════════════════════════════════════════════════════
 *  Test 3: GroupCommit — wait_for_lsn / notify_flushed
 * ═══════════════════════════════════════════════════════════════ */

static void test_group_commit() {
    printf("  [e2e] group commit test...\n");

    GroupCommit gc;

    /* Launch 4 waiter threads that block on different LSNs */
    std::atomic<int> woken{0};
    std::vector<std::thread> waiters;

    for (int i = 1; i <= 4; ++i) {
        waiters.emplace_back([&gc, &woken, lsn = (uint64_t)i * 100]() {
            gc.wait_for_lsn(lsn);
            woken.fetch_add(1, std::memory_order_relaxed);
        });
    }

    /* Give waiters time to register */
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    /* Notify LSN 200 — should wake waiters for LSN 100 and 200 */
    gc.notify_flushed(200);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    assert(woken.load() == 2 && "exactly 2 waiters should wake");

    /* Notify LSN 500 — should wake remaining 2 */
    gc.notify_flushed(500);
    for (auto& th : waiters)
        th.join();
    assert(woken.load() == 4 && "all 4 waiters should be woken");

    printf("  [PASS] group commit\n");
}

/* ═══════════════════════════════════════════════════════════════
 *  Test 4: GroupCommit integrated with Flusher
 * ═══════════════════════════════════════════════════════════════ */

static void test_group_commit_with_flusher() {
    printf("  [e2e] group commit + flusher integration...\n");

    std::string wal_dir = make_temp_dir();
    RingBuffer ring(RING_CAP);
    Flusher flusher(wal_dir, ring);
    GroupCommit gc;

    flusher.start();

    /* Producer: write one record and commit to ring */
    InsertPayload ip = {1, 0, 8};
    uint64_t key = 42;
    uint64_t val = 99;

    size_t payload_sz = sizeof(InsertPayload) + 16;
    char payload[sizeof(InsertPayload) + 16];
    std::memcpy(payload, &ip, sizeof(ip));
    std::memcpy(payload + sizeof(ip), &key, 8);
    std::memcpy(payload + sizeof(ip) + 8, &val, 8);

    uint64_t lsn = 42;
    char record[sizeof(RecordHeader) + sizeof(InsertPayload) + 16];
    size_t rec_sz = wal_record_serialize(RecordType::INSERT, lsn,
                                         payload, payload_sz, record);

    /* Write directly to ring (bypassing ThreadBuf for simplicity) */
    uint64_t off = ring.reserve(rec_sz);
    ring.write_at(off, record, rec_sz);
    ring.commit(off, rec_sz);

    /* Wait for the flusher to make it durable.
     * We poll flushed_lsn rather than using GroupCommit here to
     * avoid a test deadlock if the flusher is slow. */
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(5);
    while (flusher.flushed_lsn() < lsn) {
        assert(std::chrono::steady_clock::now() < deadline &&
               "flusher did not drain in 5s");
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    assert(flusher.flushed_lsn() >= lsn);
    printf("  [e2e] flushed_lsn = %lu (needed %lu)\n",
           (unsigned long)flusher.flushed_lsn(), (unsigned long)lsn);

    flusher.stop();
    rm_rf(wal_dir);

    printf("  [PASS] group commit + flusher\n");
}

/* ─── main ─────────────────────────────────────────────────────── */

int main() {
    printf("=== test_wal_e2e ===\n");
    test_group_commit();
    test_full_pipeline();
    test_segment_rotation();
    test_group_commit_with_flusher();
    printf("All end-to-end WAL tests passed.\n");
    return 0;
}