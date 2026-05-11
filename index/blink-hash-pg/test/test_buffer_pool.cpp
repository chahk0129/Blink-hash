/*
 * test_buffer_pool.cpp — Tests for the page-based buffer pool
 *
 * Tests:
 *   1. Page allocation and basic pin/unpin
 *   2. Write + read cycle (persistence)
 *   3. Clock-sweep eviction
 *   4. Dirty page flush
 *   5. CRC-32C checksum integrity
 *   6. Pool restart (metadata persistence)
 */

#include "bh_page.h"
#include "bh_buffer_pool.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>
#include <unistd.h>
#include <sys/stat.h>

using namespace BLINK_HASH::WAL;

static std::string make_temp_file() {
    char tmpl[] = "/tmp/bh_pool_XXXXXX";
    int fd = ::mkstemp(tmpl);
    assert(fd >= 0);
    ::close(fd);
    return std::string(tmpl);
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: Basic pin / unpin
 * ═══════════════════════════════════════════════════════════════════ */

static void test_basic_pin_unpin() {
    printf("  [buffer_pool] basic pin/unpin...\n");
    std::string path = make_temp_file();

    {
        BufferPool pool(path, 64);

        /* Allocate and pin a new page */
        uint64_t pid = pool.alloc_page_id();
        bh_page_t* page = pool.pin_page(pid, true);
        assert(page != nullptr);
        assert(page->header.page_id == pid);

        /* Write something to payload */
        const char* msg = "hello blink-hash";
        std::memcpy(page->payload, msg, strlen(msg) + 1);

        pool.unpin_page(pid, true);  /* dirty */

        /* Re-pin should return same page (cached) */
        bh_page_t* page2 = pool.pin_page(pid);
        assert(page2 == page);  /* same slot */
        assert(std::strcmp(page2->payload, msg) == 0);
        pool.unpin_page(pid);

        assert(pool.resident_pages() == 1);
    }

    ::unlink(path.c_str());
    printf("  [PASS] basic pin/unpin\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Write + read cycle
 * ═══════════════════════════════════════════════════════════════════ */

static void test_persistence() {
    printf("  [buffer_pool] persistence...\n");
    std::string path = make_temp_file();
    uint64_t pid;

    /* Phase A: write */
    {
        BufferPool pool(path, 32);
        pid = pool.alloc_page_id();

        bh_page_t* page = pool.pin_page(pid, true);
        uint64_t magic = 0xDEADBEEFCAFE1234ULL;
        std::memcpy(page->payload, &magic, sizeof(magic));
        pool.unpin_page(pid, true);

        /* Destructor flushes all dirty pages */
    }

    /* Phase B: read back from fresh pool */
    {
        BufferPool pool(path, 32);
        bh_page_t* page = pool.pin_page(pid);

        uint64_t magic;
        std::memcpy(&magic, page->payload, sizeof(magic));
        assert(magic == 0xDEADBEEFCAFE1234ULL);

        pool.unpin_page(pid);
    }

    ::unlink(path.c_str());
    printf("  [PASS] persistence\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Clock-sweep eviction
 * ═══════════════════════════════════════════════════════════════════ */

static void test_eviction() {
    printf("  [buffer_pool] eviction...\n");
    std::string path = make_temp_file();

    /* Tiny pool: only 8 slots */
    BufferPool pool(path, 8);

    /* Allocate 16 pages — force evictions */
    uint64_t pids[16];
    for (int i = 0; i < 16; i++) {
        pids[i] = pool.alloc_page_id();
        bh_page_t* page = pool.pin_page(pids[i], true);

        /* Write i into the payload */
        int val = i;
        std::memcpy(page->payload, &val, sizeof(val));
        pool.unpin_page(pids[i], true);
    }

    auto s = pool.stats();
    printf("    hits=%llu misses=%llu evictions=%llu flushes=%llu\n",
           (unsigned long long)s.hits,
           (unsigned long long)s.misses,
           (unsigned long long)s.evictions,
           (unsigned long long)s.flushes);

    /* Should have had evictions since pool only holds 8 */
    assert(s.evictions > 0);

    /* Even evicted pages should be readable (re-loaded from disk) */
    for (int i = 0; i < 16; i++) {
        bh_page_t* page = pool.pin_page(pids[i]);
        int val;
        std::memcpy(&val, page->payload, sizeof(val));
        assert(val == i);
        pool.unpin_page(pids[i]);
    }

    ::unlink(path.c_str());
    printf("  [PASS] eviction\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: Dirty page tracking
 * ═══════════════════════════════════════════════════════════════════ */

static void test_dirty_tracking() {
    printf("  [buffer_pool] dirty tracking...\n");
    std::string path = make_temp_file();

    BufferPool pool(path, 32);

    /* Create 10 pages, mark 5 dirty */
    uint64_t pids[10];
    for (int i = 0; i < 10; i++) {
        pids[i] = pool.alloc_page_id();
        pool.pin_page(pids[i], true);
        pool.unpin_page(pids[i], i < 5);  /* first 5 dirty */
    }

    assert(pool.dirty_pages() == 5);

    /* Flush all dirty */
    pool.flush_all_dirty();
    assert(pool.dirty_pages() == 0);

    ::unlink(path.c_str());
    printf("  [PASS] dirty tracking\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: CRC-32C checksum
 * ═══════════════════════════════════════════════════════════════════ */

static void test_checksum() {
    printf("  [buffer_pool] checksum...\n");

    bh_page_t page;
    page.init(42);

    /* Write known data */
    std::memset(page.payload, 0xAB, BH_PAGE_PAYLOAD);
    page.header.checksum = page.compute_checksum();

    assert(page.verify_checksum());

    /* Corrupt one byte */
    page.payload[100] ^= 0xFF;
    assert(!page.verify_checksum());

    printf("  [PASS] checksum\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 6: Pool restart (metapage persistence)
 * ═══════════════════════════════════════════════════════════════════ */

static void test_restart() {
    printf("  [buffer_pool] restart...\n");
    std::string path = make_temp_file();
    uint64_t last_pid;

    {
        BufferPool pool(path, 32);
        for (int i = 0; i < 100; i++) {
            last_pid = pool.alloc_page_id();
        }
        printf("    allocated up to page_id=%llu\n",
               (unsigned long long)last_pid);
    }

    /* Restart — should continue from where we left off */
    {
        BufferPool pool2(path, 32);
        uint64_t next_pid = pool2.alloc_page_id();
        printf("    after restart, next page_id=%llu\n",
               (unsigned long long)next_pid);
        assert(next_pid > last_pid);
    }

    ::unlink(path.c_str());
    printf("  [PASS] restart\n");
}

/* ─── main ─────────────────────────────────────────────────────────── */

int main(int argc, char** argv) {
    int start = argc > 1 ? atoi(argv[1]) : 1;
    printf("=== test_buffer_pool (from test %d) ===\n", start);
    if (start <= 1) test_basic_pin_unpin();
    if (start <= 2) test_persistence();
    if (start <= 3) test_eviction();
    if (start <= 4) test_dirty_tracking();
    if (start <= 5) test_checksum();
    if (start <= 6) test_restart();
    printf("All buffer_pool tests passed.\n");
    return 0;
}