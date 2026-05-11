
#include "tree.h" 

#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <random>

// ============================================================================
// Type aliases matching the library's instantiated templates
// ============================================================================
using Key_t   = BLINK_HASH::key64_t;    // uint64_t
using Value_t = BLINK_HASH::value64_t;  // uint64_t
using Bucket  = BLINK_HASH::bucket_t<Key_t, Value_t>;

// ============================================================================
// Minimalist test framework
// ============================================================================
static int g_run  = 0;
static int g_pass = 0;
static int g_fail = 0;   // incremented by CHECK on failure

#define CHECK(cond, msg)                                                    \
    do {                                                                    \
        ++g_run;                                                            \
        if (!(cond)) {                                                      \
            ++g_fail;                                                       \
            std::cout << "    FAIL [" << __func__ << "] " << (msg) << "\n";\
        } else {                                                            \
            ++g_pass;                                                       \
        }                                                                   \
    } while (0)

// Per-test RAII guard.
// Captures the g_fail count on construction (test start) and on destruction
// (test end) prints "PASS" if no new failures occurred, or "FAIL" otherwise.
struct TestGuard {
    const char* name_;
    int         snap_;   // g_fail value at the start of this test
    TestGuard(const char* n) : name_(n), snap_(g_fail) {}
    ~TestGuard() {
        if (g_fail == snap_)
            std::cout << "  PASS  " << name_ << "\n";
        else
            std::cout << "  FAIL  " << name_ << "  (" << (g_fail - snap_) << " check(s) failed)\n";
    }
};
// Place BEGIN_TEST() as the very first statement in each test function.
#define BEGIN_TEST() TestGuard _tg(__func__)

// ============================================================================
// SIMD-portable bucket helpers
//
// bkt_insert / bkt_remove abstract over the three compile-time paths so that
// the test body is written once regardless of AVX_256 / AVX_128 / scalar.
// ============================================================================
static bool bkt_insert(Bucket& b, Key_t key, Value_t value, uint8_t fp)
{
#ifdef FINGERPRINT
  #ifdef AVX_256
    return b.insert(key, value, fp, _mm256_setzero_si256());
  #elif defined AVX_128
    return b.insert(key, value, fp, _mm_setzero_si128());
  #else
    return b.insert(key, value, fp, uint8_t{0});
  #endif
#else
    (void)fp;
    return b.insert(key, value);
#endif
}

static bool bkt_remove(Bucket& b, Key_t key, uint8_t fp)
{
#ifdef FINGERPRINT
  #ifdef AVX_256
    return b.remove(key, _mm256_set1_epi8(static_cast<char>(fp)));
  #elif defined AVX_128
    return b.remove(key, _mm_set1_epi8(static_cast<char>(fp)));
  #else
    return b.remove(key, fp);
  #endif
#else
    (void)fp;
    return b.remove(key);
#endif
}

// ============================================================================
// ground_truth()
//
// Scan every slot directly, bypassing metadata, to compute the actual
// {count, min, max}.  Used to cross-check the incremental metadata.
// ============================================================================
struct BktStats { int count; Key_t actual_min; Key_t actual_max; };

static BktStats ground_truth(const Bucket& b)
{
    BktStats s{0, 0, 0};
    for (int i = 0; i < BLINK_HASH::entry_num; ++i) {
#ifdef FINGERPRINT
        if (b.fingerprints[i] == 0) continue;          // empty slot in FINGERPRINT mode
#else
        if (b.entry[i].key == BLINK_HASH::EMPTY<Key_t>) continue;  // empty in baseline mode
#endif
        if (s.count++ == 0) {
            s.actual_min = b.entry[i].key;
            s.actual_max = b.entry[i].key;
        } else {
            if (b.entry[i].key < s.actual_min) s.actual_min = b.entry[i].key;
            if (b.entry[i].key > s.actual_max) s.actual_max = b.entry[i].key;
        }
    }
    return s;
}

// ============================================================================
// Helpers to check whether every live entry is within [min_key, max_key]
// ============================================================================
static bool entries_within_bounds(const Bucket& b)
{
    for (int i = 0; i < BLINK_HASH::entry_num; ++i) {
#ifdef FINGERPRINT
        if (b.fingerprints[i] == 0) continue;
#else
        if (b.entry[i].key == BLINK_HASH::EMPTY<Key_t>) continue;
#endif
        if (b.entry[i].key < b.min_key || b.entry[i].key > b.max_key)
            return false;
    }
    return true;
}

// ============================================================================
// ============================================================================
//  SECTION 1 — Initial state
// ============================================================================
// ============================================================================

// After construction: live_count == 0, min_key == Key_t{}, max_key == Key_t{}.
static void test_initial_state()
{
    BEGIN_TEST();
    Bucket b;

    // live_count must start at zero — no entries yet
    CHECK((int)b.live_count == 0,
          "initial live_count must be 0, got " + std::to_string((int)b.live_count));

    // min_key and max_key are undefined when the bucket is empty — they are
    // set to Key_t{} (zero-initialised) as a sentinel.
    CHECK(b.min_key == Key_t{},
          "initial min_key must be Key_t{}, got " + std::to_string(b.min_key));
    CHECK(b.max_key == Key_t{},
          "initial max_key must be Key_t{}, got " + std::to_string(b.max_key));
}

// ============================================================================
//  SECTION 2 — Insert tracking
// ============================================================================

// live_count increments by exactly 1 for every successful insert.
// A failed insert (bucket full) must not change live_count.
static void test_insert_live_count()
{
    BEGIN_TEST();
    Bucket b;
    const uint8_t fp = 0x01;  // arbitrary non-zero odd fingerprint (all inserts use same fp)

    // Insert entry_num distinct keys — the bucket should accept them all.
    for (int i = 1; i <= BLINK_HASH::entry_num; ++i) {
        bool ok = bkt_insert(b, (Key_t)i, (Value_t)(i * 10), fp);

        CHECK(ok, "insert #" + std::to_string(i) + " must succeed (bucket not yet full)");
        CHECK((int)b.live_count == i,
              "live_count after " + std::to_string(i) + " inserts: expected " +
              std::to_string(i) + ", got " + std::to_string((int)b.live_count));
    }

    // Bucket is now full — the next insert must fail and must NOT touch live_count.
    bool overflow = bkt_insert(b, (Key_t)9999, (Value_t)9999, fp);
    CHECK(!overflow,
          "insert into fully-occupied bucket must return false");
    CHECK((int)b.live_count == BLINK_HASH::entry_num,
          "live_count must not grow beyond entry_num; got " +
          std::to_string((int)b.live_count));
}

// min_key == smallest key after ascending inserts, max_key == largest.
static void test_min_max_ascending()
{
    BEGIN_TEST();
    Bucket b;
    const uint8_t fp = 0x03;

    // Insert keys 1, 2, ..., entry_num in strictly ascending order.
    for (int i = 1; i <= BLINK_HASH::entry_num; ++i)
        bkt_insert(b, (Key_t)i, (Value_t)i, fp);

    // After ascending inserts the first key is the minimum and the last is
    // the maximum.  The metadata must match exactly.
    CHECK(b.min_key == (Key_t)1,
          "min_key ascending: expected 1, got " + std::to_string(b.min_key));
    CHECK(b.max_key == (Key_t)BLINK_HASH::entry_num,
          "max_key ascending: expected " + std::to_string(BLINK_HASH::entry_num) +
          ", got " + std::to_string(b.max_key));
}

// min/max correct even when keys arrive in descending order.
static void test_min_max_descending()
{
    BEGIN_TEST();
    Bucket b;
    const uint8_t fp = 0x05;

    // Insert entry_num, entry_num-1, ..., 1.
    for (int i = BLINK_HASH::entry_num; i >= 1; --i)
        bkt_insert(b, (Key_t)i, (Value_t)i, fp);

    CHECK(b.min_key == (Key_t)1,
          "min_key descending: expected 1, got " + std::to_string(b.min_key));
    CHECK(b.max_key == (Key_t)BLINK_HASH::entry_num,
          "max_key descending: expected " + std::to_string(BLINK_HASH::entry_num) +
          ", got " + std::to_string(b.max_key));
}

// min/max correct for arbitrary insertion order.
static void test_min_max_random_order()
{
    BEGIN_TEST();
    Bucket b;
    const uint8_t fp = 0x07;

    // Generate entry_num distinct keys with guaranteed spacing, then shuffle.
    std::vector<Key_t> keys;
    for (int i = 0; i < BLINK_HASH::entry_num; ++i)
        keys.push_back((Key_t)((i + 1) * 97 + 13));   // e.g. 110, 207, 304, …

    std::mt19937 rng(42);
    std::shuffle(keys.begin(), keys.end(), rng);

    Key_t expected_min = *std::min_element(keys.begin(), keys.end());
    Key_t expected_max = *std::max_element(keys.begin(), keys.end());

    for (Key_t k : keys)
        bkt_insert(b, k, k, fp);

    CHECK(b.min_key == expected_min,
          "min_key random: expected " + std::to_string(expected_min) +
          ", got " + std::to_string(b.min_key));
    CHECK(b.max_key == expected_max,
          "max_key random: expected " + std::to_string(expected_max) +
          ", got " + std::to_string(b.max_key));

    // All live entries must sit within the tracked bounds.
    CHECK(entries_within_bounds(b),
          "every live entry must satisfy min_key <= key <= max_key");
}

// ============================================================================
//  SECTION 3 — Remove tracking
// ============================================================================

// Removing a middle key: live_count decrements, min/max stay exact.
static void test_remove_middle_key()
{
    BEGIN_TEST();
    Bucket b;
    const uint8_t fp = 0x09;
    bkt_insert(b, (Key_t) 5, (Value_t) 50, fp);
    bkt_insert(b, (Key_t)10, (Value_t)100, fp);
    bkt_insert(b, (Key_t)20, (Value_t)200, fp);

    CHECK((int)b.live_count == 3, "live_count before remove: expected 3");

    // Remove a key that is neither the minimum (5) nor the maximum (20).
    bool ok = bkt_remove(b, (Key_t)10, fp);
    CHECK(ok, "bkt_remove of existing key 10 must return true");
    CHECK((int)b.live_count == 2,
          "live_count after removing middle key: expected 2, got " +
          std::to_string((int)b.live_count));

    // Because 10 was neither min nor max, both bounds remain exact.
    CHECK(b.min_key == (Key_t)5,
          "min_key after removing middle key: expected 5, got " +
          std::to_string(b.min_key));
    CHECK(b.max_key == (Key_t)20,
          "max_key after removing middle key: expected 20, got " +
          std::to_string(b.max_key));
}

// Removing the sole entry resets live_count to 0 and both keys to Key_t{}.
static void test_remove_last_entry()
{
    BEGIN_TEST();
    Bucket b;
    const uint8_t fp = 0x0B;
    bkt_insert(b, (Key_t)42, (Value_t)420, fp);
    CHECK((int)b.live_count == 1, "live_count after single insert: expected 1");

    bkt_remove(b, (Key_t)42, fp);

    CHECK((int)b.live_count == 0,
          "live_count after removing only entry: expected 0, got " +
          std::to_string((int)b.live_count));
    CHECK(b.min_key == Key_t{},
          "min_key after bucket becomes empty: expected Key_t{}, got " +
          std::to_string(b.min_key));
    CHECK(b.max_key == Key_t{},
          "max_key after bucket becomes empty: expected Key_t{}, got " +
          std::to_string(b.max_key));
}

// Safety invariant I2: after removing the minimum key, min_key stays
// <= the new actual minimum (conservative lower bound → no false right-prune).
static void test_remove_min_lower_bound_invariant()
{
    BEGIN_TEST();
    Bucket b;
    const uint8_t fp = 0x0D;
    bkt_insert(b, (Key_t) 1, (Value_t) 10, fp);  // current min
    bkt_insert(b, (Key_t) 5, (Value_t) 50, fp);
    bkt_insert(b, (Key_t)10, (Value_t)100, fp);

    // Remove the current minimum.  After this, the actual minimum becomes 5.
    bkt_remove(b, (Key_t)1, fp);
    BktStats gt = ground_truth(b);

    // live_count must still be exact.
    CHECK((int)b.live_count == gt.count,
          "live_count must match ground truth after removing min");

    // min_key is allowed to be stale (still 1) or updated (5), but it must
    // satisfy: min_key <= actual_min.  This ensures no entry is ever missed
    // during a "pure-right" pruning check.
    CHECK(b.min_key <= gt.actual_min,
          "Safety I2: min_key must be <= actual_min after removing min; "
          "min_key=" + std::to_string(b.min_key) +
          ", actual_min=" + std::to_string(gt.actual_min));

    // max_key is unaffected here; it must still be >= actual_max.
    CHECK(b.max_key >= gt.actual_max,
          "Safety I3: max_key must be >= actual_max; "
          "max_key=" + std::to_string(b.max_key) +
          ", actual_max=" + std::to_string(gt.actual_max));
}

// Safety invariant I3: after removing the maximum key, max_key stays
// >= the new actual maximum (conservative upper bound → no false left-prune).
static void test_remove_max_upper_bound_invariant()
{
    BEGIN_TEST();
    Bucket b;
    const uint8_t fp = 0x0F;
    bkt_insert(b, (Key_t) 1, (Value_t) 10, fp);
    bkt_insert(b, (Key_t) 5, (Value_t) 50, fp);
    bkt_insert(b, (Key_t)10, (Value_t)100, fp);  // current max

    // Remove the current maximum.  After this, the actual maximum becomes 5.
    bkt_remove(b, (Key_t)10, fp);
    BktStats gt = ground_truth(b);

    CHECK((int)b.live_count == gt.count,
          "live_count must match ground truth after removing max");

    // max_key is allowed to be stale (still 10) but must satisfy:
    // max_key >= actual_max.  This ensures no entry is ever missed during
    // a "pure-left" pruning check.
    CHECK(b.max_key >= gt.actual_max,
          "Safety I3: max_key must be >= actual_max after removing max; "
          "max_key=" + std::to_string(b.max_key) +
          ", actual_max=" + std::to_string(gt.actual_max));

    // min_key is unaffected here.
    CHECK(b.min_key <= gt.actual_min,
          "Safety I2: min_key must be <= actual_min; "
          "min_key=" + std::to_string(b.min_key) +
          ", actual_min=" + std::to_string(gt.actual_min));
}

// ============================================================================
//  SECTION 4 — recompute_meta()
// ============================================================================

// After arbitrary entry modifications that bypass insert/remove (e.g. bulk
// migration during split), recompute_meta() must restore exact values.
static void test_recompute_meta_gives_exact_values()
{
    BEGIN_TEST();
    Bucket b;
    const uint8_t fp = 0x11;
    bkt_insert(b, (Key_t)50, (Value_t)500, fp);
    bkt_insert(b, (Key_t)10, (Value_t)100, fp);
    bkt_insert(b, (Key_t)90, (Value_t)900, fp);
    bkt_insert(b, (Key_t)30, (Value_t)300, fp);

    // Corrupt metadata to simulate what a raw bulk-migration does.
    b.live_count = 0;
    b.min_key    = Key_t{};
    b.max_key    = Key_t{};

    // recompute_meta() scans all slots and rebuilds exact values.
    b.recompute_meta();
    BktStats gt = ground_truth(b);

    CHECK((int)b.live_count == gt.count,
          "recompute_meta live_count: expected " + std::to_string(gt.count) +
          ", got " + std::to_string((int)b.live_count));
    CHECK(b.min_key == gt.actual_min,
          "recompute_meta min_key: expected " + std::to_string(gt.actual_min) +
          ", got " + std::to_string(b.min_key));
    CHECK(b.max_key == gt.actual_max,
          "recompute_meta max_key: expected " + std::to_string(gt.actual_max) +
          ", got " + std::to_string(b.max_key));
}

// recompute_meta() on a fresh (all-empty) bucket must produce zeroed metadata.
static void test_recompute_meta_on_empty_bucket()
{
    BEGIN_TEST();
    Bucket b;
    b.recompute_meta();

    CHECK((int)b.live_count == 0,
          "recompute_meta on empty bucket: live_count must be 0");
    CHECK(b.min_key == Key_t{},
          "recompute_meta on empty bucket: min_key must be Key_t{}");
    CHECK(b.max_key == Key_t{},
          "recompute_meta on empty bucket: max_key must be Key_t{}");
}

// After removing the minimum and then calling recompute_meta(), the stale
// min_key must be corrected to the new exact minimum.
static void test_recompute_meta_fixes_stale_min()
{
    BEGIN_TEST();
    Bucket b;
    const uint8_t fp = 0x13;
    bkt_insert(b, (Key_t) 1, (Value_t) 10, fp);  // will be removed (stale)
    bkt_insert(b, (Key_t) 5, (Value_t) 50, fp);
    bkt_insert(b, (Key_t)10, (Value_t)100, fp);

    bkt_remove(b, (Key_t)1, fp);   // min_key may still be 1 (stale)
    b.recompute_meta();             // must correct it to 5

    BktStats gt = ground_truth(b);
    CHECK((int)b.live_count == gt.count,
          "recompute_meta live_count after remove+recompute");
    CHECK(b.min_key == gt.actual_min,
          "recompute_meta must set exact min_key after stale removal; "
          "expected " + std::to_string(gt.actual_min) +
          ", got "     + std::to_string(b.min_key));
    CHECK(b.max_key == gt.actual_max,
          "recompute_meta must set exact max_key after removal");
}

// ============================================================================
//  SECTION 5 — Split pruning classification
// ============================================================================

// Pure-LEFT:  max_key < split_key  =>  all live entries are < split_key.
// Verify both that the classification fires AND that it is correct (no entry
// >= split_key inside the bucket).
static void test_split_prune_pure_left()
{
    BEGIN_TEST();
    Bucket b;
    const uint8_t fp = 0x15;

    // Insert 5 small keys that are all well below the split point.
    for (int k = 1; k <= 5; ++k)
        bkt_insert(b, (Key_t)k, (Value_t)k, fp);

    const Key_t split_key = 100;   // far above all inserted keys

    // Metadata-based classification.
    bool classified_left = (b.max_key < split_key);
    CHECK(classified_left,
          "bucket with all keys < split_key must be classified pure-left "
          "(max_key=" + std::to_string(b.max_key) +
          ", split_key=" + std::to_string(split_key) + ")");

    // Correctness: if classified as pure-left, scanning must confirm it.
    bool all_left = true;
    for (int i = 0; i < BLINK_HASH::entry_num; ++i) {
#ifdef FINGERPRINT
        if (b.fingerprints[i] != 0 && b.entry[i].key >= split_key) all_left = false;
#else
        if (b.entry[i].key != BLINK_HASH::EMPTY<Key_t> && b.entry[i].key >= split_key)
            all_left = false;
#endif
    }
    CHECK(all_left,
          "correctness check: no live entry must be >= split_key in a pure-left bucket");
}

// Pure-RIGHT:  min_key >= split_key  =>  all live entries are >= split_key.
static void test_split_prune_pure_right()
{
    BEGIN_TEST();
    Bucket b;
    const uint8_t fp = 0x17;

    // Insert entry_num keys all strictly above the split point.
    for (int k = 200; k < 200 + BLINK_HASH::entry_num; ++k)
        bkt_insert(b, (Key_t)k, (Value_t)k, fp);

    const Key_t split_key = 100;   // far below all inserted keys

    bool classified_right = (b.min_key >= split_key);
    CHECK(classified_right,
          "bucket with all keys >= split_key must be classified pure-right "
          "(min_key=" + std::to_string(b.min_key) +
          ", split_key=" + std::to_string(split_key) + ")");

    bool all_right = true;
    for (int i = 0; i < BLINK_HASH::entry_num; ++i) {
#ifdef FINGERPRINT
        if (b.fingerprints[i] != 0 && b.entry[i].key < split_key) all_right = false;
#else
        if (b.entry[i].key != BLINK_HASH::EMPTY<Key_t> && b.entry[i].key < split_key)
            all_right = false;
#endif
    }
    CHECK(all_right,
          "correctness check: no live entry must be < split_key in a pure-right bucket");
}

// MIXED:  min_key < split_key  AND  max_key >= split_key  =>  must scan.
// Verify the bucket is NOT classified as pure-left or pure-right, and that
// both sides genuinely contain entries.
static void test_split_prune_mixed()
{
    BEGIN_TEST();
    Bucket b;
    const uint8_t fp = 0x19;

    bkt_insert(b, (Key_t)10, (Value_t)100, fp);   // left  side
    bkt_insert(b, (Key_t)50, (Value_t)500, fp);   // right side

    const Key_t split_key = 30;

    bool is_pure_left  = (b.max_key < split_key);
    bool is_pure_right = (b.min_key >= split_key);
    bool is_mixed      = !is_pure_left && !is_pure_right;

    CHECK(is_mixed,
          "bucket with keys on both sides of split_key must be classified MIXED");

    // Ground-truth: confirm entries on both sides really exist.
    bool has_left_entry = false, has_right_entry = false;
    for (int i = 0; i < BLINK_HASH::entry_num; ++i) {
#ifdef FINGERPRINT
        if (b.fingerprints[i] == 0) continue;
#else
        if (b.entry[i].key == BLINK_HASH::EMPTY<Key_t>) continue;
#endif
        if (b.entry[i].key <  split_key) has_left_entry  = true;
        if (b.entry[i].key >= split_key) has_right_entry = true;
    }
    CHECK(has_left_entry,  "mixed bucket must contain at least one left-side entry");
    CHECK(has_right_entry, "mixed bucket must contain at least one right-side entry");
}

// Edge case: all keys are exactly equal to split_key → pure-right
// (semantics: entries with key == split_key belong to the right/new node).
static void test_split_prune_all_keys_equal_split_key()
{
    BEGIN_TEST();
    Bucket b;
    const uint8_t fp = 0x1B;
    const Key_t split_key = 42;

    for (int i = 0; i < 5; ++i)
        bkt_insert(b, split_key, (Value_t)(i + 1), fp);

    bool is_pure_right = (b.min_key >= split_key);
    bool is_pure_left  = (b.max_key <  split_key);

    CHECK(is_pure_right,
          "bucket where all keys == split_key must be classified pure-right");
    CHECK(!is_pure_left,
          "bucket where all keys == split_key must NOT be classified pure-left");
}

// ============================================================================
//  SECTION 6 — Bounds invariant at EVERY insert step
// ============================================================================

// After EACH individual insert, every live entry must satisfy
// min_key <= entry.key <= max_key.
// This is the core correctness guarantee for split pruning.
static void test_bounds_invariant_at_every_insert_step()
{
    BEGIN_TEST();
    Bucket b;
    const uint8_t fp = 0x1D;

    // Use distinct, well-spaced keys inserted in a shuffled order.
    std::vector<Key_t> keys;
    for (int i = 0; i < BLINK_HASH::entry_num; ++i)
        keys.push_back((Key_t)((i + 1) * 317 + 11));   // 328, 645, 962, …

    std::mt19937 rng(2025);
    std::shuffle(keys.begin(), keys.end(), rng);

    bool invariant_holds = true;
    for (Key_t k : keys) {
        bkt_insert(b, k, k, fp);

        // Immediately check that ALL current live entries are within [min, max].
        if (!entries_within_bounds(b))
            invariant_holds = false;
    }

    CHECK(invariant_holds,
          "min_key <= entry.key <= max_key must hold immediately after every insert");
}

// ============================================================================
//  SECTION 7 — Integration: validate metadata across a full lnode_hash_t
// ============================================================================

// Insert N keys through lnode_hash_t::insert(), then walk all cardinality
// buckets and verify the four safety invariants:
//   I1.  live_count == ground-truth occupied-slot count
//   I2.  min_key    <= actual minimum key  (safe lower bound)
//   I3.  max_key    >= actual maximum key  (safe upper bound)
//   I4.  every live entry satisfies min_key <= key <= max_key
static void test_lnode_hash_metadata_consistency()
{
    BEGIN_TEST();
    using LNode = BLINK_HASH::lnode_hash_t<Key_t, Value_t>;

    auto* node = new LNode();

    // In a freshly constructed node, node_t::lock == 0 (not write-locked, not
    // obsolete).  lnode_hash_t::insert() validates that (version == node_lock),
    // so passing version = 0 is correct for single-threaded testing — the node
    // lock never changes during pure inserts.
    const uint64_t version = 0;

    // Insert 512 distinct keys (far below the ~15,296-slot capacity so no
    // split will be triggered and every insert returns 0 == success).
    const int N = 512;
    int success_count = 0;
    for (int i = 1; i <= N; ++i) {
        int ret = node->insert((Key_t)i, (Value_t)(i * 2), version);
        if (ret == 0) ++success_count;
    }

    // All inserts must succeed in single-threaded mode.
    CHECK(success_count == N,
          "all " + std::to_string(N) + " inserts must succeed; got " +
          std::to_string(success_count));

    // Walk every bucket and apply the four invariants.
    int buckets_checked = 0;
    bool i1_ok = true, i2_ok = true, i3_ok = true, i4_ok = true;

    for (int j = 0; j < (int)LNode::cardinality; ++j) {
        const Bucket& bkt = node->get_bucket(j);
        if (bkt.live_count == 0) continue;
        ++buckets_checked;

        BktStats gt = ground_truth(bkt);

        // I1 — live_count must be exact.
        if ((int)bkt.live_count != gt.count) {
            std::cout << "    I1 FAIL bucket[" << j << "]: live_count="
                      << (int)bkt.live_count << " expected=" << gt.count << "\n";
            i1_ok = false;
        }

        // I2 — min_key must not exceed the actual minimum.
        if (bkt.min_key > gt.actual_min) {
            std::cout << "    I2 FAIL bucket[" << j << "]: min_key="
                      << bkt.min_key << " > actual_min=" << gt.actual_min << "\n";
            i2_ok = false;
        }

        // I3 — max_key must not be less than the actual maximum.
        if (bkt.max_key < gt.actual_max) {
            std::cout << "    I3 FAIL bucket[" << j << "]: max_key="
                      << bkt.max_key << " < actual_max=" << gt.actual_max << "\n";
            i3_ok = false;
        }

        // I4 — every live entry must be within [min_key, max_key].
        for (int i = 0; i < BLINK_HASH::entry_num; ++i) {
#ifdef FINGERPRINT
            if (bkt.fingerprints[i] == 0) continue;
#else
            if (bkt.entry[i].key == BLINK_HASH::EMPTY<Key_t>) continue;
#endif
            if (bkt.entry[i].key < bkt.min_key || bkt.entry[i].key > bkt.max_key) {
                std::cout << "    I4 FAIL bucket[" << j << "] entry[" << i
                          << "]: key=" << bkt.entry[i].key
                          << " outside [" << bkt.min_key << ", " << bkt.max_key << "]\n";
                i4_ok = false;
            }
        }
    }

    CHECK(i1_ok, "I1: live_count == actual count in every non-empty bucket");
    CHECK(i2_ok, "I2: min_key <= actual_min in every non-empty bucket");
    CHECK(i3_ok, "I3: max_key >= actual_max in every non-empty bucket");
    CHECK(i4_ok, "I4: every live entry satisfies min_key <= key <= max_key");

    std::cout << "    (inserted=" << success_count
              << ", non-empty buckets=" << buckets_checked
              << " / " << LNode::cardinality << " total)\n";

    delete node;
}

// ============================================================================
//  main
// ============================================================================
int main()
{
    std::cout << "================================================================\n";
    std::cout << "  Bucket Metadata Tests — split-pruning invariants\n";
    std::cout << "  entry_num=" << BLINK_HASH::entry_num;
#ifdef FINGERPRINT
    std::cout << "  FINGERPRINT=ON";
#else
    std::cout << "  FINGERPRINT=OFF";
#endif
#ifdef AVX_256
    std::cout << "  SIMD=AVX_256";
#elif defined AVX_128
    std::cout << "  SIMD=AVX_128";
#else
    std::cout << "  SIMD=scalar";
#endif
#ifdef LINKED
    std::cout << "  LINKED=ON";
#endif
    std::cout << "\n================================================================\n";

    // ----- Section 1 ----  initial state -----
    std::cout << "\n[Section 1]  Initial State\n";
    test_initial_state();

    // ----- Section 2 ---- insert tracking -----
    std::cout << "\n[Section 2]  Insert Tracking\n";
    test_insert_live_count();
    test_min_max_ascending();
    test_min_max_descending();
    test_min_max_random_order();

    // ----- Section 3 ---- remove tracking -----
    std::cout << "\n[Section 3]  Remove Tracking\n";
    test_remove_middle_key();
    test_remove_last_entry();
    test_remove_min_lower_bound_invariant();
    test_remove_max_upper_bound_invariant();

    // ----- Section 4 ---- recompute_meta -----
    std::cout << "\n[Section 4]  recompute_meta()\n";
    test_recompute_meta_gives_exact_values();
    test_recompute_meta_on_empty_bucket();
    test_recompute_meta_fixes_stale_min();

    // ----- Section 5 ---- split pruning -----
    std::cout << "\n[Section 5]  Split Pruning Classification\n";
    test_split_prune_pure_left();
    test_split_prune_pure_right();
    test_split_prune_mixed();
    test_split_prune_all_keys_equal_split_key();

    // ----- Section 6 ---- bounds invariant -----
    std::cout << "\n[Section 6]  Bounds Invariant at Every Insert Step\n";
    test_bounds_invariant_at_every_insert_step();

    // ----- Section 7 ---- integration -----
    std::cout << "\n[Section 7]  Integration — lnode_hash_t\n";
    test_lnode_hash_metadata_consistency();

    // ----- summary -----
    std::cout << "\n================================================================\n";
    std::cout << "  Results: " << g_pass << " / " << g_run << " checks passed";
    if (g_fail > 0)
        std::cout << "  (" << g_fail << " FAILED)";
    std::cout << "\n";
    std::cout << "================================================================\n";
    return (g_fail == 0) ? 0 : 1;
}
