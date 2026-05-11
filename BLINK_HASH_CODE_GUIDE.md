# Blink-hash — Code Walkthrough

A step-by-step guide that traces execution paths through the actual source code.
For each major operation the key lines are shown, annotated, and explained.

---

## 0. Mental model in two sentences

Blink-hash is a **B-link tree whose leaf level starts as hash nodes and can adapt into sorted B-tree nodes**.
Internal nodes route by key (like any B+ tree), leaf nodes distribute concurrent writes across independent buckets (unlike any B+ tree), and range scans pull them back into sorted order on demand.

---

## 1. Source file map

Read files in this order to build understanding bottom-up:

```
node.h           — base class: lock word + pointer fields
entry.h          — entry_t<Key, Value> plain struct
bucket.h         — bucket_t: fixed-slot, per-bucket lock, SIMD ops
hash.h / .cpp    — hash functions (MurmurHash2, Jenkins, xxHash)
Epoche.h / .cpp  — epoch-based GC: ThreadInfo, EpocheGuard
inode.h / .cpp   — internal nodes (sorted keys → child pointers)
lnode.h          — leaf base class + type enum
lnode_btree.cpp  — sorted leaf implementation
lnode_hash.cpp   — hash leaf implementation
lnode.cpp        — lnode dispatcher (calls btree or hash by type)
tree.h / .cpp    — top-level btree_t: all public operations
```

All code is in namespace `BLINK_HASH`.
The only instantiated concrete type is `btree_t<key64_t, value64_t>` (`uint64_t` / `uint64_t`).

---

## 2. Class hierarchy

```
node_t                       ← base: lock, sibling_ptr, leftmost_ptr, cnt, level
 ├── inode_t<Key_t>          ← internal node: sorted entry_t<Key, node_t*>[]
 └── lnode_t<Key_t, Value_t> ← leaf base: adds type enum + high_key
      ├── lnode_btree_t      ← sorted leaf: entry_t<Key, Value>[]
      └── lnode_hash_t       ← hash leaf:   bucket_t<Key, Value>[]
```

`btree_t<Key_t, Value_t>` is **not** a node — it is the index root container.
It holds a single `node_t* root` pointer and an `Epoche epoche{256}` instance.

---

## 3. The lock word — read this before anything else

Every node (internal and leaf alike) inherits this from `node_t` (`node.h`):

```cpp
std::atomic<uint64_t> lock;
```

The value carries two flags in its low bits:

```
bit 1 (0b10) — write-lock held
bit 0 (0b01) — node is obsolete (deleted / replaced)
```

The rest of the 64 bits act as a **version counter** that increments on
every write-unlock. This enables optimistic reads:

```cpp
// Reader records version before reading
uint64_t cur_vstart = cur->try_readlock(need_restart);

// ... reads data (no lock held) ...

// Reader validates that nothing changed
auto cur_vend = cur->get_version(need_restart);
if (need_restart || (cur_vstart != cur_vend))
    goto restart;
```

`try_readlock` just loads the value; it sets `need_restart` if the lock bit
or obsolete bit is set. No CAS is issued — reading is always free.

`try_upgrade_writelock` does a CAS from `version` to `version + 0b10`.
If it fails (another writer got there first), the caller restarts.

`write_unlock` does `lock.fetch_add(0b10)` — increments the version and
clears the lock bit simultaneously.

`write_unlock_obsolete` does `lock.fetch_sub(0b11)` — clears both flags
and decrements the counter, permanently marking the node invalid.

---

## 4. Entry and bucket layout

### `entry_t` (`entry.h`)

```cpp
template <typename Key_t, typename Value_t>
struct entry_t {
    Key_t   key;
    Value_t value;
};
```

Plain 16-byte struct (8 + 8 for `uint64_t` types). No padding.

### `bucket_t` (`bucket.h`)

```cpp
template <typename Key_t, typename Value_t>
struct bucket_t {
    std::atomic<uint32_t> lock;           // per-bucket spinlock
    uint8_t fingerprints[entry_num];      // 32 one-byte fingerprints (#ifdef FINGERPRINT)
    entry_t<Key_t, Value_t> entry[32];    // 32 key-value slots
};
```

`entry_num = 32` (from `entry.h`). With `uint64_t` keys and values each
bucket is `4 + 32 + 32*16 = 548` bytes before alignment.

The fingerprint is `(_hash(hash_of_key) | 1)` — the `| 1` ensures it is
never zero, because zero is the sentinel for "empty slot".

Lookup in a bucket under `AVX_128` (the default compile target):

```cpp
// broadcast the 1-byte target fingerprint across a 128-bit register
__m128i fingerprint = _mm_set1_epi8(_hash(hash_key) | 1);

// load 16 fingerprints at a time, compare all 16 in one instruction
__m128i fp_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(fingerprints + m*16));
__m128i cmp = _mm_cmpeq_epi8(fingerprint, fp_);
uint32_t bitfield = _mm_movemask_epi8(cmp);  // 1-bit per match

for (int i = 0; i < 16; i++) {
    if ((bitfield >> i) & 0x1) {
        if (entry[m*16 + i].key == key) {  // confirm exact match
            value = entry[m*16 + i].value;
            return true;
        }
    }
}
```

Two passes cover all 32 slots (`m = 0` then `m = 1`).

---

## 5. Hash leaf node layout

`lnode_hash_t` (`lnode.h` + `lnode_hash.cpp`):

```cpp
static constexpr size_t cardinality =
    (LEAF_HASH_SIZE - sizeof(lnode_t<Key_t, Value_t>) - sizeof(lnode_t<...>*))
    / sizeof(bucket_t<Key_t, Value_t>);
```

With `LEAF_HASH_SIZE = 256 KB` this produces in the range of **hundreds of buckets**.
Each bucket is independent — it has its own lock. This is what enables many
threads to insert concurrently without contenting each other.

```
lnode_hash_t layout in memory:
┌─────────────────────────────────────────────────────┐
│  node_t   (lock, sibling_ptr, leftmost_ptr, cnt, level)  │  from lnode_t
│  type     (HASH_NODE enum)                               │
│  high_key                                               │
│  left_sibling_ptr                                       │
├─────────────────────────────────────────────────────┤
│  bucket[0]   (lock + 32 fingerprints + 32 entries)  │
│  bucket[1]                                          │
│  ...                                                │
│  bucket[cardinality-1]                              │
└─────────────────────────────────────────────────────┘
```

---

## 6. Internal node layout

`inode_t<Key_t>` (`inode.h` + `inode.cpp`):

```
┌──────────────────────────────────────────────────────┐
│  node_t base  (lock, sibling_ptr, leftmost_ptr, ...)  │
│  high_key                                             │
│  entry[0..cardinality-1]:  { Key_t key, node_t* value }│
└──────────────────────────────────────────────────────┘
```

`leftmost_ptr` holds the child pointer for keys less than `entry[0].key`.
`entry[i].value` holds the child pointer for keys in `[entry[i].key, entry[i+1].key)`.

`scan_node` picks the right child:

```cpp
node_t* inode_t<Key_t>::scan_node(Key_t key) {
    if (sibling_ptr && (high_key < key))
        return sibling_ptr;          // B-link move-right
    int idx = find_lowerbound(key);
    if (idx > -1)
        return entry[idx].value;
    else
        return leftmost_ptr;
}
```

`find_lowerbound` is the linear scan through `entry[]` (binary search activates
when `LEAF_BTREE_SIZE >= 2048`, which is not the default build size).

---

## 7. Epoch-based reclamation (`Epoche.h` / `Epoche.cpp`)

Before touching the tree, every caller obtains a `ThreadInfo`:

```cpp
// In btree_t:
ThreadInfo getThreadInfo() {
    return ThreadInfo(this->epoche);
}
```

Every mutable operation wraps its work in an `EpocheGuard`:

```cpp
void btree_t::insert(Key_t key, Value_t value, ThreadInfo& epocheThreadInfo) {
    EpocheGuard epocheGuard(epocheThreadInfo);  // calls enterEpoche
    // ...
}  // destructor calls exitEpocheAndCleanup
```

Read-only operations use `EpocheGuardReadonly` (enters epoch, never cleans up).

Nodes that must be deleted (after a split replaces a leaf) are queued:

```cpp
threadEpocheInfo.getEpoche().markNodeForDeletion(prev, threadEpocheInfo);
```

They are only freed when no thread's epoch overlaps the deletion epoch —
standard RCU-style deferred reclamation backed by TBB thread-local storage.

---

## 8. Walkthrough — INSERT

**Entry point:** `btree_t::insert(key, value, epocheInfo)` in `tree.cpp`

### Step 1 — enter epoch, snapshot root

```cpp
EpocheGuard epocheGuard(epocheThreadInfo);
restart:
auto cur = root;
int stack_cnt = 0;
inode_t<Key_t>* stack[root->level];   // local stack of visited parents

bool need_restart = false;
auto cur_vstart = cur->try_readlock(need_restart);
if (need_restart) goto restart;
```

No lock is acquired. `cur_vstart` is just the current lock-word value.

### Step 2 — descend internal levels

```cpp
while (cur->level != 0) {
    auto child = static_cast<inode_t<Key_t>*>(cur)->scan_node(key);
    auto child_vstart = child->try_readlock(need_restart);
    if (need_restart) goto restart;

    auto cur_vend = cur->get_version(need_restart);
    if (need_restart || (cur_vstart != cur_vend))
        goto restart;             // parent changed under us → restart

    if (child != cur->sibling_ptr)
        stack[stack_cnt++] = static_cast<inode_t<Key_t>*>(cur);

    cur = child;
    cur_vstart = child_vstart;
}
```

Every level: read child pointer, validate parent version. If the parent was
modified while we were reading, throw away all local state and go back to `restart`.

Only **non-sibling children** are pushed to the stack — siblings are a sign of
split-in-progress, not a stable parent relationship.

### Step 3 — move right on leaf level

```cpp
auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);

while (leaf->sibling_ptr && (leaf->high_key < key)) {
    auto sibling = static_cast<lnode_t<Key_t, Value_t>*>(leaf->sibling_ptr);
    auto sibling_v = sibling->try_readlock(need_restart);
    if (need_restart) goto restart;

    auto leaf_vend = static_cast<node_t*>(leaf)->get_version(need_restart);
    if (need_restart || (leaf_vstart != leaf_vend)) goto restart;

    leaf = sibling;
    leaf_vstart = sibling_v;
}
```

If a concurrent split moved the target key to a right sibling, this loop finds it.

### Step 4 — leaf insert with three outcome codes

```cpp
auto ret = leaf->insert(key, value, leaf_vstart);
```

`lnode_t::insert` dispatches to either `lnode_btree_t::insert` or `lnode_hash_t::insert`.

Return values are a convention used throughout the codebase:

| return | meaning |
|--------|---------|
| `-1` | node was modified while we tried to acquire its lock — retry |
| `0` | success |
| `1` | leaf is full — caller must split |

### Step 5a — hash leaf insert detail (`lnode_hash.cpp`)

```cpp
for (int k = 0; k < HASH_FUNCS_NUM; k++) {       // try 2 hash functions
    auto hash_key = h(&key, sizeof(Key_t), k);
    uint8_t fingerprint = _hash(hash_key) | 1;

    for (int j = 0; j < NUM_SLOT; j++) {          // probe up to 4 buckets
        auto loc = (hash_key + j) % cardinality;

        if (!bucket[loc].try_lock())
            return -1;                             // bucket busy → retry tree op

        auto _version = static_cast<node_t*>(this)->get_version(need_restart);
        if (need_restart || (version != _version)) {
            bucket[loc].unlock();
            return -1;                             // leaf changed → retry tree op
        }

        if (bucket[loc].insert(key, value, fingerprint, empty)) {
            bucket[loc].unlock();
            return 0;                              // SUCCESS
        }

        bucket[loc].unlock();
    }
}
return 1;                                          // all probed buckets full → SPLIT
```

Up to `HASH_FUNCS_NUM * NUM_SLOT = 8` bucket locations are tried.
The fingerprint is stored alongside the entry for fast SIMD-filtered retrieval.

### Step 5b — sorted leaf insert detail (`lnode_btree.cpp`)

```cpp
int lnode_btree_t::insert(Key_t key, Value_t value, uint64_t version) {
    this->try_upgrade_writelock(version, need_restart);   // CAS lock
    if (need_restart) return -1;

    if (this->cnt < cardinality) {
        int pos = find_lowerbound(key);                   // binary or linear search
        memmove(&entry[pos+1], &entry[pos],
                sizeof(entry_t<...>) * (cnt - pos));      // shift right
        entry[pos] = {key, value};
        this->cnt++;
        write_unlock();
        return 0;
    }
    return 1;    // full — need split, write lock kept for caller to observe
}
```

The sorted leaf acquires an exclusive write-lock; the hash leaf only acquires
a per-bucket lock. This is the core performance difference between the two.

### Step 6 — leaf split (if ret == 1)

```cpp
Key_t split_key;
auto new_leaf = leaf->split(split_key, key, value, leaf_vstart);
if (new_leaf == nullptr)
    goto restart;   // another thread beat us to the split
```

For a **B-tree leaf** the split is at the midpoint, entries above go to `new_leaf`.

For a **hash leaf**:
1. collect all live keys to find the median
2. `this->high_key = median` (left node keeps keys ≤ median)
3. `new_right->high_key = old high_key` (right node gets keys > median)
4. scatter all entries into their correct node using their hash positions
5. mark all buckets `LINKED_RIGHT` (or the new node's buckets as `LINKED_LEFT`)
   so that any thread that accesses them during the transition lazily stabilizes

### Step 7 — propagate split key up to parent

```cpp
while (stack_idx > -1) {
    old_parent = stack[stack_idx];
    old_parent->try_upgrade_writelock(parent_vstart, need_restart);
    // ... unlock the child we just split ...
    if (!old_parent->is_full()) {
        old_parent->insert(split_key, new_node);  // normal case: fits in parent
        old_parent->write_unlock();
        return;
    }
    // parent also full: split it and keep walking up the stack
    Key_t _split_key;
    auto new_parent = old_parent->split(_split_key);
    ...
    stack_idx--;
}
```

If the stack is exhausted (we're at root), a new `inode_t` root is created.

---

## 9. Walkthrough — LOOKUP

**Entry point:** `btree_t::lookup(key, epocheInfo)` in `tree.cpp`

```cpp
EpocheGuardReadonly epocheGuard(threadEpocheInfo);  // read-only epoch
restart:
// Traverse internals — same pattern as insert but no stack needed
while (cur->level != 0) {
    auto child = static_cast<inode_t<Key_t>*>(cur)->scan_node(key);
    auto child_vstart = child->try_readlock(need_restart);
    // validate parent, advance cur
}

// Move right until key is within this leaf's key range
while (leaf->sibling_ptr && (leaf->high_key < key)) { ... }

// Leaf lookup
auto ret = leaf->find(key, need_restart);
if (need_restart) goto restart;

// Final validation: make sure the leaf didn't change while we read from it
auto leaf_vend = leaf->get_version(need_restart);
if (need_restart || (leaf_vstart != leaf_vend)) goto restart;

return ret;
```

No write-locks are ever acquired during lookup.

### Hash leaf `find` detail

```cpp
for (int k = 0; k < HASH_FUNCS_NUM; k++) {
    auto hash_key = h(&key, sizeof(Key_t), k);
    __m128i fingerprint = _mm_set1_epi8(_hash(hash_key) | 1);  // broadcast

    for (int j = 0; j < NUM_SLOT; j++) {
        auto loc = (hash_key + j) % cardinality;

        auto bucket_vstart = bucket[loc].get_version(need_restart);  // no lock
        if (need_restart) return 0;

        Value_t ret;
        if (bucket[loc].find(key, ret, fingerprint)) {
            // validate bucket version after reading
            auto bucket_vend = bucket[loc].get_version(need_restart);
            if (need_restart || (bucket_vstart != bucket_vend)) {
                need_restart = true; return 0;
            }
            return ret;
        }
        // check version even on miss, for safety
    }
}
return 0;
```

Reads probe each bucket with SIMD fingerprint filter — no buckets are locked.

---

## 10. Walkthrough — RANGE LOOKUP

**Entry point:** `btree_t::range_lookup(min_key, range, buf, epocheInfo)` in `tree.cpp`

```cpp
// Descend to starting leaf (same as lookup)
// ...

int count = 0;
bool continued = false;
while (count < range) {
    // move-right if necessary (same as insert/lookup)

    auto ret = leaf->range_lookup(min_key, buf, count, range, continued);

    if (ret == -1) goto restart;      // version conflict during bucket scan
    if (ret == -2) {                  // hash leaf signalled: trigger conversion
        convert(leaf, leaf_vstart, threadEpocheInfo);
        goto restart;
    }

    continued = true;
    if (ret == range) return ret;
    if (!leaf->sibling_ptr) break;    // end of leaf chain

    // advance to next leaf sibling
    leaf = static_cast<lnode_t<...>*>(leaf->sibling_ptr);
    count = ret;
}
return count;
```

### B-tree leaf range detail (`lnode_btree.cpp`)

```cpp
int lnode_btree_t::range_lookup(Key_t key, Value_t* buf, int count, int range, bool continued) {
    auto _count = count;
    if (continued) {
        // subsequent leaf: copy all entries
        for (int i = 0; i < this->cnt; i++) {
            buf[_count++] = entry[i].value;
            if (_count == range) return _count;
        }
    } else {
        // first leaf: start from position of min_key
        int pos = find_lowerbound(key);
        for (int i = pos + 1; i < this->cnt; i++) {
            buf[_count++] = entry[i].value;
            if (_count == range) return _count;
        }
    }
    return _count;
}
```

### Hash leaf range detail (`lnode_hash.cpp`)

The hash leaf has no sorted order so range lookup has to:

1. Drain **all** buckets into a temporary unsorted buffer
2. Sort that buffer by key
3. Then slice out the requested range

```cpp
entry_t<Key_t, Value_t> _buf[cardinality * entry_num];
int idx = 0;

for (int j = 0; j < cardinality; j++) {
    bucket[j].collect(key, _buf, idx, empty);  // skip entries below min_key
}

std::sort(_buf, _buf + idx, [](auto& a, auto& b){ return a.key < b.key; });

for (int i = 0; i < idx; i++) {
    if (count >= range) break;
    buf[count++] = _buf[i].value;
}
```

This sort cost is why the `ADAPTATION` flag (enabled by default) triggers
conversion to a sorted B-tree leaf whenever a hash leaf is range-scanned.

---

## 11. Walkthrough — HASH → B-TREE CONVERSION

Triggered automatically inside `range_lookup` via `ret == -2`, or explicitly
with `btree_t::convert_all()`.

**`btree_t::convert` in `tree.cpp`:**

```cpp
bool btree_t::convert(lnode_t* leaf, uint64_t leaf_version, ThreadInfo& threadEpocheInfo) {
    int num = 0;
    // 1. call lnode_hash_t::convert — sorts all entries, creates new btree leaves
    auto nodes = static_cast<lnode_hash_t<...>*>(leaf)->convert(num, leaf_version);
    if (nodes == nullptr) return false;

    // 2. collect split keys between new leaves
    Key_t split_key[num];
    split_key[0] = nodes[0]->high_key;
    for (int i = 1; i < num; i++)
        split_key[i] = nodes[i-1]->high_key;

    // 3. batch-insert all new leaves into the parent structure
    batch_insert(split_key, reinterpret_cast<node_t**>(nodes), num, leaf, threadEpocheInfo);
    return true;
}
```

**`lnode_hash_t::convert` in `lnode_hash.cpp`:**

The function:
1. acquires the convert-lock (same CAS as split-lock but for conversion purpose)
2. dumps and sorts all live `entry_t` pairs from all buckets
3. packs them into a series of new `lnode_btree_t` leaves at `FILL_FACTOR`
4. sets sibling pointers to chain the new leaves
5. returns the array of new leaf nodes back to the caller

The old hash leaf is then marked obsolete by `batch_insert` and queued
for epoch-based deletion via `markNodeForDeletion`.

---

## 12. Compile-time knobs

All flags are set in `index/blink-hash/lib/CMakeLists.txt`.
The default `blinkhash` library target uses all of them:

| Flag | Effect when defined |
|------|---------------------|
| `FINGERPRINT` | Store 8-bit fingerprints per entry; use them to pre-filter before key compare |
| `AVX_128` | Use 128-bit SIMD (`_mm_cmpeq_epi8`) for bucket operations |
| `AVX_256` | Use 256-bit SIMD (`_mm256_cmpeq_epi8`) instead |
| `SAMPLING` | Collect a sample of keys for median estimation during split (faster than full scan) |
| `LINKED` | Mark buckets `LINKED_LEFT`/`LINKED_RIGHT` during split; lazy stabilization |
| `ADAPTATION` | Trigger hash→btree conversion during range scans |
| `BLINK_DEBUG` | Enable `blink_printf` debug logging |
| `BREAKDOWN` | Expose per-operation timing counters |

Without `FINGERPRINT`: every bucket slot must be linearly compared by key.
Without `LINKED`: all entries are eagerly migrated during split.
Without `ADAPTATION`: hash leaves are never converted; range scans always sort.

---

## 13. How to run the range benchmark (`test/range.cpp`)

```sh
# from the blink-hash build directory
./range <num_data> <num_threads>

# e.g. 10 million keys, 16 threads
./range 10000000 16
```

The test:
1. **warmup** — 64 threads insert all keys (randomly shuffled)
2. optionally **convert_all** (if `#define CONVERT` is active at compile time)
3. **scan** — N threads each run range queries of size 50

If compiled without `CONVERT`, range queries will hit hash leaves and
adaptive conversion fires during the scan phase, re-shaping the leaf level
as the scan progresses.

---

## 14. Quick reference — return codes

The three-value convention used throughout leaf operations:

| Value | Meaning | Caller action |
|-------|---------|---------------|
| `-1` | Conflict (lock, version mismatch, unstable bucket) | `goto restart` |
| `0` | Success | return normally |
| `1` | Node full (needs split) | perform split then propagate |
| `-2` | Hash leaf needs conversion (range only) | call `convert()` then `goto restart` |

---

## 15. Common confusion points

**Why does insert call `goto restart` so many times?**
Because it holds *no locks* during tree traversal. Any write from another
thread can invalidate the local path. The version check detects this and
retries from root. This is correct and lock-free for reads.

**Why is `lnode_hash_t` so large (256 KB)?**
To hold enough independent buckets that N concurrent threads almost never
hash to the same bucket. Contention becomes O(1/buckets). A 512-byte page
with 1–2 buckets would just serialize everyone.

**What exactly is `high_key`?**
The fence key of a node. Every key in this node satisfies `key <= high_key`.
Anything larger belongs to the sibling. A `nullptr` sibling means no upper bound.
This is the B-link tree "high key" invariant that makes move-right safe.

**What does `write_unlock_obsolete` do vs `write_unlock`?**
`write_unlock` just increments the version (node stays in the tree).
`write_unlock_obsolete` clears both bits and decrements — the odd counter
triggers `is_obsolete()` on every future reader, causing them to restart
and re-traverse from root, never touching the dead node again.

**What is `leftmost_ptr` in `node_t`?**
For internal nodes it is the child pointer for the leftmost key range
(keys less than `entry[0].key`). It is also used during batch insert to
link new leaf chains.

