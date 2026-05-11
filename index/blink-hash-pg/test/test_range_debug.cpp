/*
 * Minimal test: insert 10000 keys, do point lookup, then range_lookup.
 * Compile with the same flags as the library: -DFINGERPRINT -DSAMPLING -DLINKED -DADAPTATION
 */
#include "tree.h"
#include <cstdio>
#include <cstdint>
#include <cstring>

using namespace BLINK_HASH;
using Key_t = uint64_t;
using Value_t = uint64_t;

int main() {
    btree_t<Key_t, Value_t> tree;
    auto ti = tree.getThreadInfo();

    const int N = 10000;

    /* Test 1: simple unsigned keys 1..10000 */
    printf("=== Test 1: Simple unsigned keys 1..%d ===\n", N);
    for (int i = 1; i <= N; i++) {
        tree.insert((uint64_t)i, (uint64_t)(i * 100), ti);
    }

    printf("Inserted %d keys\n", N);

    /* Point lookup for key=5000 */
    {
        auto val = tree.lookup(5000, ti);
        printf("Point lookup key=5000: val=%llu  %s\n",
               (unsigned long long)val,
               val != 0 ? "FOUND" : "NOT FOUND");
    }

    /* Range lookup from key=9000 */
    {
        uint64_t buf[2000];
        memset(buf, 0, sizeof(buf));
        int count = tree.range_lookup(9000, 2000, buf, ti);
        printf("Range lookup key>=9000: count=%d\n", count);
        if (count > 0) {
            printf("  buf[0]=%llu  buf[last]=%llu\n",
                   (unsigned long long)buf[0],
                   (unsigned long long)buf[count-1]);
        }
    }

    /* Range lookup from key=1 (almost full scan) */
    {
        uint64_t buf[11000];
        memset(buf, 0, sizeof(buf));
        int count = tree.range_lookup(1, 11000, buf, ti);
        printf("Range lookup key>=1: count=%d\n", count);
    }

    /* Test 2: XOR-transformed keys */
    printf("\n=== Test 2: XOR-transformed keys ===\n");
    btree_t<Key_t, Value_t> tree2;
    auto ti2 = tree2.getThreadInfo();

    for (int i = 1; i <= N; i++) {
        int64_t sv = (int64_t)i;
        uint64_t key = (uint64_t)(sv ^ ((int64_t)1 << 63));
        tree2.insert(key, (uint64_t)(i * 100), ti2);
    }

    {
        int64_t sv = 5000;
        uint64_t k = (uint64_t)(sv ^ ((int64_t)1 << 63));
        auto val = tree2.lookup(k, ti2);
        printf("Point lookup ts=5000 (key=0x%016llx): val=%llu  %s\n",
               (unsigned long long)k, (unsigned long long)val,
               val != 0 ? "FOUND" : "NOT FOUND");
    }

    {
        int64_t sv = 9000;
        uint64_t k = (uint64_t)(sv ^ ((int64_t)1 << 63));
        uint64_t buf[2000];
        memset(buf, 0, sizeof(buf));
        int count = tree2.range_lookup(k, 2000, buf, ti2);
        printf("Range lookup ts>=9000 (key=0x%016llx): count=%d\n",
               (unsigned long long)k, count);
    }

    /* Check tree height and utilization */
    printf("\nTree1 height=%d  util=%.2f%%\n", tree.check_height(), tree.utilization()*100);
    printf("Tree2 height=%d  util=%.2f%%\n", tree2.check_height(), tree2.utilization()*100);

    return 0;
}
