/*
 * test_pg_bridge.cpp — Tests for the blinkhash_core.cpp C bridge
 *
 * Exercises the extern "C" bridge functions that the PG AM callbacks
 * use.  Does NOT require PostgreSQL headers or a running instance.
 *
 * Tests:
 *   1. Integer tree: create, insert, lookup, update, remove
 *   2. String tree: create, insert, lookup, update, remove
 *   3. Range lookup (integer)
 *   4. TID packing/unpacking
 *   5. Large-scale insert + lookup (100K)
 *   6. Tree diagnostics (height, utilization)
 */

#include "blinkhash_core.h"
#include "bh_node_map.h"
#include "tree.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <chrono>

using namespace BLINK_HASH::WAL;
using namespace BLINK_HASH;

/* ═══════════════════════════════════════════════════════════════════
 *  TID packing helpers (replicate blinkhash_utils.c logic for testing)
 * ═══════════════════════════════════════════════════════════════════ */

static uint64_t pack_tid(uint32_t block, uint16_t offset) {
    return ((uint64_t)block << 16) | (uint64_t)offset;
}

static void unpack_tid(uint64_t val, uint32_t *block, uint16_t *offset) {
    *block  = (uint32_t)(val >> 16);
    *offset = (uint16_t)(val & 0xFFFF);
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: Integer tree CRUD
 * ═══════════════════════════════════════════════════════════════════ */

static void test_int_tree_crud() {
    printf("  [pg_bridge] integer tree CRUD...\n");

    void *tree = bh_tree_create('i');
    assert(tree != NULL);

    void *ti = bh_get_thread_info(tree, 'i');
    assert(ti != NULL);

    /* Insert 1000 keys */
    for (uint64_t i = 1; i <= 1000; i++) {
        uint64_t val = pack_tid(i, 1);
        bh_insert(tree, 'i', &i, sizeof(i), val, ti);
    }

    /* Lookup all keys */
    int found_count = 0;
    for (uint64_t i = 1; i <= 1000; i++) {
        int found = 0;
        uint64_t val = bh_lookup(tree, 'i', &i, sizeof(i), ti, &found);
        if (found) {
            uint32_t block;
            uint16_t offset;
            unpack_tid(val, &block, &offset);
            assert(block == i);
            assert(offset == 1);
            found_count++;
        }
    }
    assert(found_count == 1000);

    /* Update key 500 */
    {
        uint64_t key = 500;
        uint64_t new_val = pack_tid(9999, 42);
        int ok = bh_update(tree, 'i', &key, sizeof(key), new_val, ti);
        assert(ok == 1);

        int found = 0;
        uint64_t val = bh_lookup(tree, 'i', &key, sizeof(key), ti, &found);
        assert(found);
        uint32_t block;
        uint16_t offset;
        unpack_tid(val, &block, &offset);
        assert(block == 9999);
        assert(offset == 42);
    }

    /* Remove key 100 */
    {
        uint64_t key = 100;
        int ok = bh_remove(tree, 'i', &key, sizeof(key), ti);
        assert(ok == 1);

        int found = 0;
        bh_lookup(tree, 'i', &key, sizeof(key), ti, &found);
        assert(!found);
    }

    /* Lookup non-existent key */
    {
        uint64_t key = 99999;
        int found = 0;
        bh_lookup(tree, 'i', &key, sizeof(key), ti, &found);
        assert(!found);
    }

    bh_free_thread_info(ti);
    bh_tree_destroy(tree, 'i');
    printf("  [PASS] integer tree CRUD\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: String tree CRUD
 * ═══════════════════════════════════════════════════════════════════ */

static void test_string_tree_crud() {
    printf("  [pg_bridge] string tree CRUD...\n");

    void *tree = bh_tree_create('s');
    assert(tree != NULL);

    void *ti = bh_get_thread_info(tree, 's');
    assert(ti != NULL);

    /* Insert some string keys */
    const char* keys[] = {"alpha", "beta", "gamma", "delta", "epsilon",
                          "zeta", "eta", "theta", "iota", "kappa"};
    int nkeys = 10;

    for (int i = 0; i < nkeys; i++) {
        char key_buf[32];
        memset(key_buf, 0, 32);
        strncpy(key_buf, keys[i], 31);
        uint64_t val = pack_tid(i + 1, 1);
        bh_insert(tree, 's', key_buf, 32, val, ti);
    }

    /* Lookup all keys */
    int found_count = 0;
    for (int i = 0; i < nkeys; i++) {
        char key_buf[32];
        memset(key_buf, 0, 32);
        strncpy(key_buf, keys[i], 31);

        int found = 0;
        uint64_t val = bh_lookup(tree, 's', key_buf, 32, ti, &found);
        if (found) {
            uint32_t block;
            uint16_t offset;
            unpack_tid(val, &block, &offset);
            assert(block == (uint32_t)(i + 1));
            found_count++;
        }
    }
    assert(found_count == nkeys);

    /* Remove "gamma" */
    {
        char key_buf[32];
        memset(key_buf, 0, 32);
        strncpy(key_buf, "gamma", 31);
        int ok = bh_remove(tree, 's', key_buf, 32, ti);
        assert(ok == 1);

        int found = 0;
        bh_lookup(tree, 's', key_buf, 32, ti, &found);
        assert(!found);
    }

    bh_free_thread_info(ti);
    bh_tree_destroy(tree, 's');
    printf("  [PASS] string tree CRUD\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Range lookup (integer)
 * ═══════════════════════════════════════════════════════════════════ */

static void test_range_lookup() {
    printf("  [pg_bridge] range lookup...\n");

    void *tree = bh_tree_create('i');
    void *ti = bh_get_thread_info(tree, 'i');

    /* Insert 10000 sequential keys */
    for (uint64_t i = 1; i <= 10000; i++) {
        uint64_t val = i * 10;
        bh_insert(tree, 'i', &i, sizeof(i), val, ti);
    }

    /* Range lookup starting from key 5000, range 100 */
    uint64_t buf[200];
    uint64_t start_key = 5000;
    int count = bh_range_lookup(tree, 'i', &start_key, sizeof(start_key),
                                100, buf, ti);

    printf("    range_lookup from %llu: got %d results\n",
           (unsigned long long)start_key, count);
    assert(count > 0);
    assert(count <= 100);

    /* Verify results are valid values */
    for (int i = 0; i < count; i++) {
        assert(buf[i] != 0);
    }

    bh_free_thread_info(ti);
    bh_tree_destroy(tree, 'i');
    printf("  [PASS] range lookup\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: TID packing roundtrip
 * ═══════════════════════════════════════════════════════════════════ */

static void test_tid_packing() {
    printf("  [pg_bridge] TID packing...\n");

    /* Test various block/offset combinations */
    struct { uint32_t block; uint16_t offset; } cases[] = {
        {0, 1}, {1, 1}, {42, 7}, {0xFFFFFFFF, 0xFFFF},
        {12345, 678}, {0, 0}, {1000000, 100}
    };

    for (auto& c : cases) {
        uint64_t packed = pack_tid(c.block, c.offset);
        uint32_t block;
        uint16_t offset;
        unpack_tid(packed, &block, &offset);
        assert(block == c.block);
        assert(offset == c.offset);
    }

    printf("  [PASS] TID packing\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: Large-scale (100K)
 * ═══════════════════════════════════════════════════════════════════ */

static void test_large_scale() {
    printf("  [pg_bridge] large-scale 100K...\n");

    void *tree = bh_tree_create('i');
    void *ti = bh_get_thread_info(tree, 'i');

    constexpr int N = 100000;

    auto t0 = std::chrono::steady_clock::now();
    for (uint64_t i = 1; i <= N; i++) {
        uint64_t val = pack_tid((uint32_t)i, 1);
        bh_insert(tree, 'i', &i, sizeof(i), val, ti);
    }
    auto t1 = std::chrono::steady_clock::now();

    printf("    insert: %.2f s (%.0f ops/s)\n",
           std::chrono::duration<double>(t1 - t0).count(),
           N / std::chrono::duration<double>(t1 - t0).count());

    /* Spot-check 1000 random lookups */
    int ok = 0;
    for (int i = 0; i < 1000; i++) {
        uint64_t key = ((uint64_t)i * 7919) % N + 1;
        int found = 0;
        uint64_t val = bh_lookup(tree, 'i', &key, sizeof(key), ti, &found);
        if (found) {
            uint32_t block;
            uint16_t offset;
            unpack_tid(val, &block, &offset);
            if (block == (uint32_t)key && offset == 1) ok++;
        }
    }

    printf("    spot-check: %d / 1000 correct\n", ok);
    assert(ok == 1000);

    bh_free_thread_info(ti);
    bh_tree_destroy(tree, 'i');
    printf("  [PASS] large-scale 100K\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 6: Insert metadata
 * ═══════════════════════════════════════════════════════════════════ */

static void test_insert_metadata() {
    printf("  [pg_bridge] insert metadata...\n");

    node_map_init(64);

    void *tree_raw = bh_tree_create('i');
    assert(tree_raw != NULL);

    auto *tree = static_cast<btree_t<key64_t, value64_t>*>(tree_raw);
    void *ti = bh_get_thread_info(tree_raw, 'i');
    assert(ti != NULL);

    uint64_t key = 123456789ULL;
    uint64_t val = pack_tid(77, 9);
    uint64_t node_id = 0;
    uint32_t bucket_idx = static_cast<uint32_t>(-1);

    bh_insert_with_meta(tree_raw, 'i', &key, sizeof(key), val, ti,
                        &node_id, &bucket_idx);

    assert(node_id != 0);
    assert(bucket_idx != static_cast<uint32_t>(-1));

    auto *root = static_cast<lnode_hash_t<key64_t, value64_t>*>(tree->get_root());
    assert(root != NULL);
    assert(root->node_id == node_id);
    assert(root->find_bucket_index(key) == bucket_idx);
    assert(g_node_map != NULL);
    assert(g_node_map->resolve(node_id) != NULL);

    bh_free_thread_info(ti);
    bh_tree_destroy(tree_raw, 'i');
    node_map_destroy();

    printf("  [PASS] insert metadata\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 7: Diagnostics
 * ═══════════════════════════════════════════════════════════════════ */

static void test_diagnostics() {
    printf("  [pg_bridge] diagnostics...\n");

    void *tree = bh_tree_create('i');
    void *ti = bh_get_thread_info(tree, 'i');

    for (uint64_t i = 1; i <= 50000; i++) {
        bh_insert(tree, 'i', &i, sizeof(i), i, ti);
    }

    int h = bh_height(tree, 'i');
    double u = bh_utilization(tree, 'i');

    printf("    height=%d, utilization=%.2f%%\n", h, u);
    assert(h >= 1);
    assert(u > 0.0);

    bh_free_thread_info(ti);
    bh_tree_destroy(tree, 'i');
    printf("  [PASS] diagnostics\n");
}

/* ─── main ─────────────────────────────────────────────────────── */

int main(int argc, char** argv) {
    int start = argc > 1 ? atoi(argv[1]) : 1;
    printf("=== test_pg_bridge (from test %d) ===\n", start);
    if (start <= 1) test_int_tree_crud();
    if (start <= 2) test_string_tree_crud();
    if (start <= 3) test_range_lookup();
    if (start <= 4) test_tid_packing();
    if (start <= 5) test_large_scale();
    if (start <= 6) test_insert_metadata();
    if (start <= 7) test_diagnostics();
    printf("All pg_bridge tests passed.\n");
    return 0;
}