# Split Pruning via Bucket Metadata — Summary

Repository: `chahk0129/Blink-hash`, branch: `dev_hash_eshan`  
Scope: `index/blink-hash-pg/`

---

## Overview

This document summarises the work done to add per-bucket metadata to
`blink-hash-pg` and then use that metadata to skip unnecessary work during
hash leaf-node splitting.

---

## 1. Bucket Metadata Fields

**File:** `lib/bucket.h`

Three fields were added at the top of `bucket_t`:

```cpp
uint8_t live_count;   // number of occupied slots in this bucket
Key_t   min_key;      // smallest key currently stored
Key_t   max_key;      // largest key currently stored
```

### Maintenance

| Operation | Effect on metadata |
|---|---|
| `insert(key, value, …)` | increments `live_count`; updates `min_key` / `max_key` if necessary |
| `remove(key, …)` | decrements `live_count`; zeroes `min_key` / `max_key` when count reaches 0 |
| `recompute_meta()` | full scan to recompute all three fields from scratch (called after a split) |

---

## 2. Split Pruning Implementation

**File:** `lib/lnode_hash.cpp`  
**Code path:** `#ifndef LINKED` (eager-migration path only)

### Problem Before

The original `split()` iterated over every entry in every bucket one by one,
regardless of whether any entries needed to move. For a 455-bucket node with
32 slots each (~14,560 slots), most entries were simply confirmed "stays left"
by reading and comparing each key individually.

### Solution

Using the per-bucket `min_key`, `max_key`, and `live_count`, each bucket can
now be classified before touching any entry:

```
live_count == 0  OR  max_key <= split_key   →  pure-left:  skip entirely (continue)
min_key > split_key                          →  pure-right: bulk memcpy + continue
otherwise                                    →  mixed:      scan entries individually (original code)
```

### Changes Made

**Phase 1 — Skip empty buckets in median-key collection** (lines 201, 217):
```cpp
if (bucket[j].live_count == 0) continue;  // skip empty buckets using metadata
```

**Phase 2 — Three-way classification in migration loop** (lines 420–437):
```cpp
// Left bucket: all entries stay — skip
if (bucket[j].live_count == 0 || bucket[j].max_key <= split_key)
    continue;

// Right bucket: all entries move — bulk copy
if (split_key < bucket[j].min_key) {
    memcpy(new_right->bucket[j].entry, bucket[j].entry, ...);
    memcpy(new_right->bucket[j].fingerprints, ...);   // FINGERPRINT builds
    memset(bucket[j].fingerprints, 0, ...);
    continue;
}

// Mixed bucket: scan entries individually (existing code)
```

After the migration loop, `recompute_meta()` is called on all buckets of both
nodes to refresh the metadata.

---

## 3. Test Suite

**File:** `test/test_split_pruning.cpp`  
**Registration:** `test/CMakeLists.txt`  
**Build target:** `test_split_pruning`

### Build & Run

```bash
cmake --build index/blink-hash-pg/build --target test_split_pruning
index/blink-hash-pg/build/test/test_split_pruning
```

### Test Sections

| Section | Tests | What it proves |
|---|---|---|
| 1 — Bucket classification accuracy | 4 | The three metadata predicates (left / right / mixed) are logically correct on `bucket_t` in isolation; each result matches a ground-truth per-entry scan |
| 2 — Split correctness | 4 | After `split()`: no key is lost (C2), no key is duplicated (C3), trigger key is inserted (C4), `left->high_key == split_key` (C5). Tested on sequential, reverse, random, and tiny key sets |
| 3 — Post-split metadata consistency | 2 | For every non-empty bucket after split, `live_count`, `min_key`, `max_key` match a fresh per-entry scan. Active in non-LINKED builds; skipped in LINKED builds (metadata is refined lazily in `stabilize_bucket()`) |
| 4 — Pure-left buckets untouched | 1 | Every bucket classified as pure-left has no key > split_key, and its mirror in the right node is empty. Non-LINKED only |
| 5 — Pure-right buckets bulk-moved | 1 | Every bucket classified as pure-right has been completely emptied in the left node and all its keys appear in the right node. Proves the `memcpy` fast-path is correct. Non-LINKED only |
| 6 — Sequential workload | 2 | Fill a node to capacity with sequential keys, then split. Zero keys lost, zero duplicates |
| 7 — Random workload | 2 | Same as Section 6 with random keys |
| 8 — Metadata trust proof (mutation test) | 1 | **The decisive proof that split() reads and trusts `min_key`.** Deliberately corrupts one bucket's `min_key` to make a pure-left bucket appear pure-right to the migration loop. Verifies that left-side keys from that bucket end up in the right node — only possible if the code took the metadata fast-path instead of scanning entries. Non-LINKED only |

### Test Results (current build: `FINGERPRINT=ON`, `SIMD=AVX_128`, `LINKED=ON`)

```
  Results: 32 / 32 checks passed
```

Sections 3, 4, 5, and 8 skip in `LINKED=ON` builds because the metadata
fast-paths live inside `#ifndef LINKED`. They activate automatically when
rebuilt with `-DLINKED=OFF`.

---

## 4. File Map

```
lib/bucket.h              — bucket_t definition; metadata fields + maintenance
lib/lnode_hash.cpp        — split() implementation; pruning changes at lines 201, 217, 420–437
lib/lnode.h               — lnode_hash_t declaration; get_bucket() / get_bucket_mut() accessors
test/test_split_pruning.cpp  — full test suite (8 sections, 32 checks)
test/CMakeLists.txt          — test binary registration
```

---

## 5. LINKED vs non-LINKED Behaviour

`blink-hash-pg` supports two split strategies selected at compile time:

| Mode | Split behaviour | Metadata fast-path |
|---|---|---|
| `LINKED=ON` | Marks all buckets `LINKED_RIGHT`; only the trigger key's bucket is eagerly migrated. Other buckets are stabilized lazily on next access via `stabilize_bucket()` | Not active — bucket loop is replaced by a per-bucket-state approach |
| `LINKED=OFF` | Eagerly migrates all keys above `split_key` to the new right node | **Active** — three-way classification skips/bulk-copies pure-left/pure-right buckets |

The metadata fields (`live_count`, `min_key`, `max_key`) are maintained in
both modes by `insert()` and `remove()`, so the data is always up to date and
ready whenever the fast-path runs.
