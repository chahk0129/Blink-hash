/*
 * test_node_map.cpp — Tests for the Node ID → node_t* map
 *
 * Tests:
 *   1. Basic register / resolve / remove
 *   2. Overwrite existing mapping
 *   3. Build from tree (level-order walk)
 *   4. Concurrent register + resolve (multi-threaded)
 *   5. Large-scale (100K nodes)
 */

#include "tree.h"
#include "bh_node_map.h"
#include "wal_emitter.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>

using namespace BLINK_HASH;
using namespace BLINK_HASH::WAL;

/* ═══════════════════════════════════════════════════════════════════
 *  Test 1: Basic CRUD
 * ═══════════════════════════════════════════════════════════════════ */

static void test_basic_crud() {
    printf("  [node_map] basic CRUD...\n");
    NodeMap map(16);

    /* Create some fake nodes (just need addressable memory) */
    node_t nodes[10];
    for (int i = 0; i < 10; i++) {
        nodes[i].node_id = i + 1;
    }

    /* Register */
    for (int i = 0; i < 10; i++) {
        map.register_node(nodes[i].node_id, &nodes[i]);
    }
    assert(map.size() == 10);

    /* Resolve */
    for (int i = 0; i < 10; i++) {
        node_t* p = map.resolve(nodes[i].node_id);
        assert(p == &nodes[i]);
    }

    /* Resolve non-existent */
    assert(map.resolve(999) == nullptr);

    /* Remove */
    node_t* removed = map.remove(5);
    assert(removed == &nodes[4]);
    assert(map.resolve(5) == nullptr);
    assert(map.size() == 9);

    /* Clear */
    map.clear();
    assert(map.size() == 0);

    printf("  [PASS] basic CRUD\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 2: Overwrite
 * ═══════════════════════════════════════════════════════════════════ */

static void test_overwrite() {
    printf("  [node_map] overwrite...\n");
    NodeMap map(8);

    node_t a, b;
    a.node_id = 42;
    b.node_id = 42;

    map.register_node(42, &a);
    assert(map.resolve(42) == &a);

    map.register_node(42, &b);
    assert(map.resolve(42) == &b);
    assert(map.size() == 1);  /* still 1 entry, not 2 */

    printf("  [PASS] overwrite\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 3: Build from tree
 * ═══════════════════════════════════════════════════════════════════ */

static void test_build_from_tree() {
    printf("  [node_map] build from tree...\n");

    g_node_id.store(0);

    btree_t<uint64_t, uint64_t> tree;
    auto ti = tree.getThreadInfo();

    /* Insert enough keys to create multiple levels */
    constexpr int N = 50000;
    for (int i = 1; i <= N; i++)
        tree.insert(static_cast<uint64_t>(i),
                    static_cast<uint64_t>(i), ti);

    /* Build node map from tree */
    NodeMap map(64);
    map.build_from_tree(tree.get_root());

    printf("    map has %zu nodes\n", map.size());
    assert(map.size() > 0);

    /* Verify: every node in the tree should be in the map */
    int verified = 0;
    map.for_each([&](uint64_t nid, node_t* ptr) {
        assert(ptr->node_id == nid);
        verified++;
    });
    assert(verified == static_cast<int>(map.size()));

    /* Verify root is in the map */
    node_t* root = tree.get_root();
    assert(root->node_id != 0);
    node_t* found_root = map.resolve(root->node_id);
    assert(found_root == root);

    printf("    verified %d nodes\n", verified);
    printf("  [PASS] build from tree\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 4: Concurrent access
 * ═══════════════════════════════════════════════════════════════════ */

static void test_concurrent() {
    printf("  [node_map] concurrent access...\n");
    constexpr int NUM_THREADS = 8;
    constexpr int PER_THREAD  = 10000;

    NodeMap map(256);

    /* Allocate nodes (need real memory, not stack) */
    std::vector<node_t*> all_nodes(NUM_THREADS * PER_THREAD);
    for (size_t i = 0; i < all_nodes.size(); i++) {
        all_nodes[i] = new node_t();
        all_nodes[i]->node_id = i + 1;
    }

    /* Concurrent register */
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&, t]() {
            int base = t * PER_THREAD;
            for (int i = 0; i < PER_THREAD; i++) {
                int idx = base + i;
                map.register_node(all_nodes[idx]->node_id,
                                  all_nodes[idx]);
            }
        });
    }
    for (auto& th : threads) th.join();

    assert(map.size() == NUM_THREADS * PER_THREAD);

    /* Verify all entries */
    for (size_t i = 0; i < all_nodes.size(); i++) {
        node_t* p = map.resolve(all_nodes[i]->node_id);
        assert(p == all_nodes[i]);
    }

    /* Cleanup */
    for (auto* n : all_nodes) delete n;

    printf("    registered %d nodes from %d threads\n",
           NUM_THREADS * PER_THREAD, NUM_THREADS);
    printf("  [PASS] concurrent access\n");
}

/* ═══════════════════════════════════════════════════════════════════
 *  Test 5: Large-scale (100K)
 * ═══════════════════════════════════════════════════════════════════ */

static void test_large_scale() {
    printf("  [node_map] large-scale 100K...\n");
    constexpr int N = 100000;
    NodeMap map(512);

    /* Allocate nodes */
    std::vector<node_t*> nodes(N);
    for (int i = 0; i < N; i++) {
        nodes[i] = new node_t();
        nodes[i]->node_id = i + 1;
        map.register_node(nodes[i]->node_id, nodes[i]);
    }
    assert(map.size() == N);

    /* Spot-check 1000 random resolves */
    int ok = 0;
    for (int i = 0; i < 1000; i++) {
        uint64_t nid = (static_cast<uint64_t>(i) * 7919) % N + 1;
        node_t* p = map.resolve(nid);
        if (p == nodes[nid - 1]) ok++;
    }
    assert(ok == 1000);

    /* Remove half */
    for (int i = 0; i < N / 2; i++) {
        map.remove(nodes[i]->node_id);
    }
    assert(map.size() == N / 2);

    /* Cleanup */
    for (auto* n : nodes) delete n;

    printf("    %d entries, removed half, spot-check OK\n", N);
    printf("  [PASS] large-scale 100K\n");
}

/* ─── main ─────────────────────────────────────────────────────────── */

int main(int argc, char** argv) {
    int start = argc > 1 ? atoi(argv[1]) : 1;
    printf("=== test_node_map (from test %d) ===\n", start);
    if (start <= 1) test_basic_crud();
    if (start <= 2) test_overwrite();
    if (start <= 3) test_build_from_tree();
    if (start <= 4) test_concurrent();
    if (start <= 5) test_large_scale();
    printf("All node_map tests passed.\n");
    return 0;
}