# Integrating $B^{link}$-hash into PostgreSQL — Engineering Guide

## 1. Executive Summary

$B^{link}$-hash (VLDB 2023) is an **in-memory hybrid index** that uses a B-link tree with **hash leaf nodes** to mitigate high contention from monotonic (time-series) insertions. Its core innovation is distributing concurrent thread accesses across multiple hash buckets within a leaf node, rather than serializing them on a single sorted leaf. When range queries arrive, hash leaf nodes **adaptively convert** to sorted B-tree leaf nodes.

This document describes **what must be built** to embed $B^{link}$-hash as a first-class PostgreSQL access method (index AM), preserving all paper contributions: hash leaves, lazy linked splitting, fingerprinting, SIMD probing, adaptive hash→B-tree conversion, and epoch-based reclamation.

---

## 2. Current Codebase Architecture (What Already Exists)

### 2.1 Core Data Structures (`index/blink-hash/lib/`)

| File | Role |
|---|---|
| `tree.h / tree.cpp` | Top-level `btree_t<Key_t, Value_t>` — insert, lookup, update, remove, range_lookup, convert, batch_insert |
| `node.h` | Base `node_t` — optimistic lock word (version + lock bits), sibling/leftmost pointers, level |
| `inode.h / inode.cpp` | Internal nodes — sorted entries, binary/linear search, batch insert, split |
| `lnode.h / lnode.cpp` | Leaf node polymorphic dispatcher (`BTREE_NODE` vs `HASH_NODE`) |
| `lnode_btree.cpp` | Sorted B-tree leaf — sorted insert, split, range scan |
| `lnode_hash.cpp` | **Hash leaf** — multi-bucket hashing, fingerprint + SIMD probe, lazy linked split, convert to B-tree leaves |
| `bucket.h` | Per-bucket structure — 32-entry slots, per-bucket spinlock, fingerprint array, AVX-128/256 SIMD find/insert/collect |
| `entry.h` | `entry_t<Key_t, Value_t>` — key-value pair (16 bytes for 8-byte key + 8-byte value) |
| `hash.h / hash.cpp` | Hash functions — MurmurHash2, Jenkins, xxHash |
| `Epoche.h / Epoche.cpp` | Epoch-based memory reclamation (using TBB `enumerable_thread_specific`) |
| `common.h` | Type aliases: `key64_t`, `value64_t` |

### 2.2 Key Design Parameters (Constants)

```
PAGE_SIZE           = 512 bytes (internal node)
LEAF_BTREE_SIZE     = 512 bytes (sorted leaf)
LEAF_HASH_SIZE      = 256 KB   (hash leaf — large to absorb burst inserts)
entry_num           = 32       (entries per bucket)
HASH_FUNCS_NUM      = 2        (two hash functions for cuckoo-style probing)
NUM_SLOT            = 4        (4 candidate buckets per hash function)
FILL_FACTOR         = 0.8
```

### 2.3 Currently Supported Operations

| Operation | Method | Thread-Safety |
|---|---|---|
| Point Insert | `btree_t::insert(key, value, ThreadInfo&)` | Optimistic locking + per-bucket locks |
| Point Lookup | `btree_t::lookup(key, ThreadInfo&)` | Lock-free (version validation) |
| Update | `btree_t::update(key, value, ThreadInfo&)` | Optimistic read + upgrade |
| Delete | `btree_t::remove(key, ThreadInfo&)` | Optimistic read + upgrade |
| Range Scan | `btree_t::range_lookup(min_key, range, buf, ThreadInfo&)` | Version validation per leaf |
| Hash→B-tree Convert | `btree_t::convert()` / `convert_all()` | Triggered on range scan of hash leaf |
| Epoch GC | `Epoche::enterEpoche/exitEpocheAndCleanup` | Per-thread deletion lists |

### 2.4 What Does NOT Exist Yet

- **No disk persistence** — everything is `new`/`delete` in volatile DRAM
- **No WAL integration** — no crash recovery
- **No multi-version concurrency control (MVCC)** — only current-value semantics
- **No PostgreSQL Index AM interface** — no `aminsert`, `ambeginscan`, etc.
- **No variable-length key support beyond `uint64_t`** (the `blink-hash-str` variant handles `GenericKey<32/128>` but not arbitrary varlena)
- **No NULL handling**
- **No operator class / comparator registration**
- **No VACUUM / visibility map integration**
- **No cost estimation functions**
- **No serialized on-disk format** for `pg_dump`/restore

---

## 3. PostgreSQL Index AM Architecture — What Must Be Implemented

PostgreSQL's pluggable index interface (since v10 via `IndexAmRoutine`) requires implementing ~15 callback functions. Below is the full mapping with $B^{link}$-hash-specific considerations.

### 3.1 Required Index AM Callbacks

| Callback | Purpose | Implementation Notes |
|---|---|---|
| `ambuild` | Bulk-load the index from existing heap data | Use sorted bulk insertion. Build hash leaves for the rightmost (hot) portion; B-tree leaves for historical data. |
| `ambuildempty` | Create an empty index (for UNLOGGED tables) | Initialize a single empty `lnode_hash_t` as root. |
| `aminsert` | Insert a single index entry | Map to `btree_t::insert()`. Must translate PostgreSQL `Datum` → `Key_t` and store `ItemPointer` (TID = block + offset) as `Value_t`. |
| `ambeginscan` | Initialize a scan descriptor | Allocate scan state, store search key and scan direction. |
| `amrescan` | Restart a scan with new keys | Reset iterator position. |
| `amgettuple` | Return next matching tuple (TID) | For point lookups: call `btree_t::lookup()`. For range scans: use `btree_t::range_lookup()` with an iterator wrapper that yields one TID at a time. |
| `amgetbitmap` | Return a bitmap of all matching TIDs | Collect all range_lookup results into a `TIDBitmap`. |
| `amendscan` | Clean up scan state | Free scan-local memory, release epoch guard. |
| `ammarkpos` / `amrestrpos` | Mark/restore scan position (for merge joins) | Save/restore the current leaf pointer + position within leaf. |
| `amcostestimate` | Provide cost estimates to query planner | Model: tree traversal cost ≈ $O(\log n)$ for B-tree internal levels, hash leaf probe cost ≈ $O(1)$ amortized. Range scan on hash leaf triggers conversion (one-time cost). |
| `amvalidate` | Validate operator class compatibility | Standard B-tree operator class validation (support `<`, `<=`, `=`, `>=`, `>`). |
| `amoptions` | Parse `WITH (...)` storage options | Expose tunable parameters (see §6). |
| `amcanreturn` | Can the index return indexed values directly? (Index-Only Scans) | Yes — the value is stored inline in `entry_t`. Implement for covering indexes. |
| `amvacuumcleanup` / `ambulkdelete` | VACUUM support | See §5.3. |

### 3.2 Index AM Capability Flags

```c
IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);
amroutine->amstrategies     = BTMaxStrategyNumber;  // 5 strategies (B-tree compatible)
amroutine->amsupport        = BTNProcs;
amroutine->amcanorder       = true;      // supports ORDER BY
amroutine->amcanorderbyop   = false;
amroutine->amcanbackward    = true;      // B-tree leaves are doubly linked
amroutine->amcanunique      = true;      // can enforce unique constraints
amroutine->amcanmulticol    = true;      // multi-column indexes (composite key)
amroutine->amoptionalkey    = true;
amroutine->amsearcharray    = false;     // could add later
amroutine->amsearchnulls    = true;      // must handle NULLs
amroutine->amstorage        = false;
amroutine->amclusterable    = true;
amroutine->ampredlocks      = true;      // needed for serializable isolation
amroutine->amcanparallel    = true;      // B^link-hash is designed for concurrency
amroutine->amcaninclude     = true;      // INCLUDE columns for covering indexes
amroutine->amkeytype        = InvalidOid;
```

---

## 4. Implementation Roadmap — Phase by Phase

### Phase 1: Extension Skeleton & Key/Value Adaptation

#### 4.1.1 Create the Extension Scaffold

```
pg_blinkhash/
├── Makefile (or Meson)          # PGXS build
├── blinkhash--1.0.sql           # CREATE ACCESS METHOD, opclass, opfamily
├── blinkhash.control            # Extension metadata
├── src/
│   ├── blinkhash_am.c           # IndexAmRoutine + all AM callbacks
│   ├── blinkhash_handler.c      # amhandler entry point
│   ├── blinkhash_build.c        # ambuild / ambuildempty
│   ├── blinkhash_insert.c       # aminsert
│   ├── blinkhash_scan.c         # beginscan / gettuple / endscan
│   ├── blinkhash_vacuum.c       # ambulkdelete / amvacuumcleanup
│   ├── blinkhash_cost.c         # amcostestimate
│   ├── blinkhash_validate.c     # amvalidate
│   ├── blinkhash_utils.c        # datum ↔ key conversion, comparators
│   ├── blinkhash_wal.c          # WAL record definitions & redo
│   └── blinkhash_page.c         # Buffer manager page layout
└── lib/                         # Adapted B^link-hash core (C++ → C or C++ with extern "C")
    ├── tree.h / tree.cpp        # (adapted from index/blink-hash/lib/)
    ├── node.h
    ├── inode.h / inode.cpp
    ├── lnode.h / lnode.cpp
    ├── lnode_btree.cpp
    ├── lnode_hash.cpp
    ├── bucket.h
    ├── hash.h / hash.cpp
    └── epoch.h / epoch.cpp      # Replace TBB-based epoch with PG-compatible version
```

#### 4.1.2 Key/Value Type Adaptation

**Current state**: `btree_t<uint64_t, uint64_t>` — fixed 8-byte key, 8-byte value.

**Required**: PostgreSQL keys are `Datum` (variable-length, any type). Values are `ItemPointerData` (6 bytes: block# + offset).

```c
// What needs to change:
typedef struct BhKey {
    Datum       datum;       // or serialized fixed-length representation
    Oid         typid;       // for comparison dispatch
    int16       typlen;
    bool        typbyval;
} BhKey;

typedef ItemPointerData BhValue;  // 6-byte heap TID
```

**Approach**:
- For **fixed-length types** (int4, int8, float8, timestamp): store raw bytes directly in `entry_t` — this preserves the SIMD-friendly layout and fingerprinting.
- For **variable-length types** (text, varchar): store a hash/prefix in the leaf entry for the fingerprint comparison, with the full key stored out-of-band (see §5.1).
- **Paper preservation**: The bucket, fingerprint, and SIMD probing code operates on the fixed portion. For variable-length keys, the fingerprint still filters candidates; full comparison fetches the actual key from an overflow area.

#### 4.1.3 Template Removal or Specialization

PostgreSQL extensions are typically plain C. Two options:

| Approach | Trade-offs |
|---|---|
| **Option A: C++ with `extern "C"` wrappers** | Keep templates intact; wrap with C-callable functions. Build with `g++`, link into PG. Slightly more complex build, but preserves all optimizations. |
| **Option B: De-templatize to C** | Maximum compatibility, but significant rewrite effort. Lose compile-time specialization. |

**Recommendation**: Option A — use C++ internally, expose a C API via `extern "C"`. PostgreSQL supports linking C++ object files. This preserves SIMD intrinsics and template specializations with minimal changes.

---

### Phase 2: Buffer Manager Integration (Disk Persistence)

This is the most substantial engineering challenge. The current codebase is purely in-memory with `new`/`delete`.

#### 4.2.1 Page Layout Design

Map $B^{link}$-hash nodes to PostgreSQL 8 KB buffer pages:

```
┌──────────────────────────────────────────────────┐
│  PageHeaderData (24 bytes)                       │
│  - pd_lsn, pd_checksum, pd_flags, pd_lower,     │
│    pd_upper, pd_special, pd_pagesize_version     │
├──────────────────────────────────────────────────┤
│  BhPageOpaqueData (special area, ~32 bytes)      │
│  - node_type (INTERNAL / BTREE_LEAF / HASH_LEAF) │
│  - level                                         │
│  - high_key                                      │
│  - sibling_blkno (right-link)                    │
│  - flags (ROOT / LEAF / DELETED / HALF_DEAD)     │
├──────────────────────────────────────────────────┤
│  Entries area (key-value pairs)                  │
│  Internal: entry_t<Key, BlockNumber>             │
│  BTree leaf: entry_t<Key, ItemPointer> sorted    │
│  Hash leaf: bucket array (see below)             │
└──────────────────────────────────────────────────┘
```

**Hash leaf challenge**: The current `LEAF_HASH_SIZE = 256 KB` (a single hash leaf node) is **32× larger** than a PostgreSQL page (8 KB). Two approaches:

| Approach | Description | Paper Impact |
|---|---|---|
| **Multi-page hash leaf** | A hash leaf spans multiple physically contiguous pages, managed as a unit (like a GiST "split" page tuple). Uses a "hash leaf header page" pointing to sub-pages. | Preserves the paper's large hash node layout exactly. More complex buffer management. |
| **Reduced hash leaf** | Shrink `LEAF_HASH_SIZE` to fit a small number of buckets per page (e.g., 3–5 buckets × 32 entries per 8 KB page). Hash leaf = a set of linked pages. | Simpler buffer integration. Slightly changes contention distribution — **still valid** since even 3–5 buckets already distribute threads better than a single sorted leaf. The paper's insight holds at any bucket count > 1. |

**Recommendation**: Start with **reduced hash leaf** (fits in 1–4 pages). The paper's evaluation (§5.2) shows that even a modest number of buckets dramatically reduces contention. You can later experiment with multi-page leaves for maximum throughput.

#### 4.2.2 Buffer Manager Access Pattern

Replace all `new node_t(...)` / pointer dereference patterns with PostgreSQL buffer manager calls:

```c
// BEFORE (in-memory):
auto child = static_cast<inode_t<Key_t>*>(cur)->scan_node(key);

// AFTER (buffer-managed):
Buffer childBuf = ReadBuffer(rel, child_blkno);
LockBuffer(childBuf, BH_SHARED);  // or use optimistic approach
Page childPage = BufferGetPage(childBuf);
BhInternalPage *ipage = (BhInternalPage *)PageGetContents(childPage);
BlockNumber next_blkno = bh_internal_scan(ipage, key);
UnlockReleaseBuffer(childBuf);
```

**Critical: Preserve Optimistic Locking** 

The paper's key concurrency contribution is optimistic lock coupling (OLC) — readers don't acquire locks, just validate version numbers. PostgreSQL's buffer manager uses heavyweight locks (`LWLock`). To preserve the paper's approach:

- **Internal nodes**: Use PostgreSQL `LWLock` in shared mode for reads (acceptable since traversal is fast and internal nodes are few).
- **Hash leaf nodes**: Implement a **lightweight version counter** in the page's opaque data, separate from PG's buffer pin/lock. Readers check the version before and after reading, restarting on mismatch. Writers increment the version. This preserves the paper's lock-free read path.
- **Bucket-level locks**: Store per-bucket lock words within the page. These are spinlocks on the page content itself (not PG LWLocks). Safe because we hold an exclusive buffer lock while acquiring bucket locks.

#### 4.2.3 Node Pointer → Block Number Translation

Replace all `node_t*` pointers with `BlockNumber` (uint32). This affects:
- `node_t::sibling_ptr` → `sibling_blkno`
- `node_t::leftmost_ptr` → `leftmost_blkno`
- `inode_t::entry[].value` → `BlockNumber`
- `lnode_hash_t::left_sibling_ptr` → `left_sibling_blkno`

---

### Phase 3: WAL (Write-Ahead Logging) & Crash Recovery

Without WAL, a crash loses the entire index. PostgreSQL requires every page modification to be WAL-logged.

#### 4.3.1 WAL Record Types

Define custom WAL resource manager (`RM_BLINKHASH_ID`) with these record types:

| Record Type | Description |
|---|---|
| `XLOG_BLINKHASH_INSERT_LEAF` | Insert entry into a B-tree leaf or hash bucket |
| `XLOG_BLINKHASH_SPLIT_INTERNAL` | Internal node split |
| `XLOG_BLINKHASH_SPLIT_BTREE_LEAF` | B-tree leaf split |
| `XLOG_BLINKHASH_SPLIT_HASH_LEAF` | Hash leaf split (with lazy linked bucket state) |
| `XLOG_BLINKHASH_CONVERT` | Hash leaf → B-tree leaf conversion |
| `XLOG_BLINKHASH_DELETE` | Delete an entry |
| `XLOG_BLINKHASH_NEWROOT` | New root page created |
| `XLOG_BLINKHASH_STABILIZE` | Lazy stabilization of linked buckets after split |
| `XLOG_BLINKHASH_VACUUM` | Mark entries as dead during VACUUM |

#### 4.3.2 Redo Logic

Each WAL record must carry enough information to **replay** the operation:
- For inserts: page ID, offset within page, key bytes, TID
- For splits: old page ID, new page ID, split key, migrated entries
- For conversions: old hash page IDs → new B-tree page IDs + all entries

#### 4.3.3 Full Page Writes

On first modification after a checkpoint, write the entire page image (full page write / FPW). This is standard PostgreSQL practice and requires calling `MarkBufferDirty()` + `XLogInsert()` within a critical section.

---

### Phase 4: MVCC & Visibility

PostgreSQL uses MVCC — a tuple may be visible to some transactions and not others. The index must cooperate.

#### 4.4.1 No Versioned Index Entries (Standard Approach)

Following nbtree's approach:
- **Insert all versions** into the index. The index entry points to a heap tuple, and the **heap tuple contains visibility information** (xmin, xmax).
- During scans, after retrieving a TID from the index, PostgreSQL checks heap tuple visibility.
- The index itself does **not** store transaction IDs.

This means the $B^{link}$-hash insert/lookup logic is unchanged — it just maps keys to TIDs. Visibility is handled by the executor layer.

#### 4.4.2 Index-Only Scans

For index-only scans, the visibility map must be consulted:
- If a heap page is all-visible, the index tuple's value can be returned directly without fetching the heap page.
- Implement `amcanreturn` = true.

#### 4.4.3 Predicate Locking (SSI)

For Serializable Snapshot Isolation, the index must register predicate locks during scans:
- Call `PredicateLockPage()` or `PredicateLockRelation()` during range scans.
- Call `CheckForSerializableConflictIn()` during inserts.

---

### Phase 5: VACUUM, Deletion, and Maintenance

#### 5.1 `ambulkdelete`

PostgreSQL VACUUM calls `ambulkdelete` with a callback that tests whether each TID is dead.

For **B-tree leaves**: scan linearly, remove dead entries, compact.
For **hash leaves**: scan all buckets, clear fingerprints and entries for dead TIDs.

#### 5.2 `amvacuumcleanup`

After `ambulkdelete`, perform any needed structural cleanup:
- Track empty pages for reuse with the Free Space Map (FSM)
- Update index statistics (`IndexBulkDeleteResult`)
- Do NOT merge under-full nodes (same as nbtree — this is acceptable)

#### 5.3 Epoch-Based Reclamation Replacement

The current codebase uses epoch-based reclamation (`Epoche.h`) with TBB thread-local storage to defer node deallocation. In PostgreSQL:
- **Node memory is managed by the buffer manager** — no explicit `delete` of pages.
- **Dead pages** go into the FSM for reuse.
- **Replace** the `Epoche`/`EpocheGuard` mechanism with PostgreSQL's snapshot management. Since nodes are pages, not heap allocations, simply avoid freeing pages while any backend might reference them — use PG's buffer pin mechanism instead.

---

## 5. Adaptation Specifics — Preserving Paper Contributions

### 5.1 Hash Leaf Nodes — The Core Innovation

**What the paper does**: New entries go to a large hash leaf node containing multiple buckets. Each bucket has 32 entries. Concurrent inserters hash to different buckets, avoiding contention.

**How to preserve in PostgreSQL**:
- A hash leaf node maps to 1 or more pages with `node_type = HASH_LEAF`.
- Within the page(s), bucket structures are stored contiguously.
- The multi-hash-function probing (`HASH_FUNCS_NUM = 2`, `NUM_SLOT = 4`) is applied to find a target bucket.
- **Fingerprint arrays** and **SIMD probing** work on page contents — the memory layout within a pinned buffer page is the same as in-memory.

### 5.2 Lazy Linked Splitting

**What the paper does**: When a hash leaf splits, instead of immediately migrating all entries, buckets are marked `LINKED_LEFT` or `LINKED_RIGHT`. Stabilization happens lazily when a thread next accesses a linked bucket.

**How to preserve in PostgreSQL**:
- Store the `state_t` (STABLE / LINKED_LEFT / LINKED_RIGHT) per bucket in the page.
- WAL-log the state change.
- On access, if a bucket is LINKED, pin both the current and sibling page, stabilize entries, WAL-log the stabilization, then proceed.

### 5.3 Adaptive Conversion (Hash → B-tree Leaf)

**What the paper does**: When a range scan hits a hash leaf, trigger conversion to sorted B-tree leaves. This replaces one hash leaf with multiple sorted leaves, updating parent pointers via batch insert.

**How to preserve in PostgreSQL**:
- Conversion is triggered in `amgettuple` / `amgetbitmap` when a scan encounters a `HASH_LEAF` page.
- Allocate new pages for B-tree leaves, sort the entries from the hash leaf, populate the new pages, and update the parent internal node(s) using `batch_insert`.
- WAL-log the entire conversion as an atomic operation (`XLOG_BLINKHASH_CONVERT`).
- After conversion, the old hash leaf page is marked deleted and added to FSM.
- **Important**: The conversion should be done under an exclusive lock on the hash leaf page(s) to prevent concurrent modifications during the sort-and-redistribute.

### 5.4 Fingerprinting + SIMD

**What the paper does**: Each bucket stores an 8-bit fingerprint per entry. Lookups use AVX-128/256 SIMD to compare all 32 fingerprints in parallel.

**How to preserve in PostgreSQL**:
- The `fingerprints[]` array is stored in the page, in the bucket structure — no change needed.
- SIMD intrinsics (`_mm_cmpeq_epi8`, `_mm256_cmpeq_epi8`) operate on page content via the pinned buffer pointer — works exactly as before.
- Compile the extension with `-mavx2 -mavx` flags.
- Add a runtime CPU feature check (`__builtin_cpu_supports("avx2")`) with a scalar fallback for portability.

### 5.5 Optimistic Lock Coupling

**What the paper does**: Readers don't acquire locks — they read a version counter, do the operation, then verify the version hasn't changed. If it has, restart.

**How to preserve in PostgreSQL**:
- For internal node traversal: use PG buffer pin + shared lock, or implement a lightweight OLC on the page's version counter.
- For hash leaf reads: the version counter is stored in the page opaque data. Pin the buffer (no lock), read version, read data, verify version, unpin. If version changed, re-pin and retry.
- This is compatible with PG's buffer manager since a pinned buffer won't be evicted.

---

## 6. Tunable Parameters (GUC / WITH Options)

Expose these via `amoptions` / `reloptions`:

| Parameter | Default | Description |
|---|---|---|
| `hash_leaf_buckets` | 8 | Number of buckets per hash leaf page |
| `entries_per_bucket` | 32 | Entries per bucket (affects page size utilization) |
| `hash_func_num` | 2 | Number of hash functions for probing |
| `probe_slots` | 4 | Number of candidate buckets per hash function |
| `fill_factor` | 80 | Page fill factor percentage |
| `use_fingerprint` | true | Enable fingerprint-based SIMD probing |
| `use_simd` | auto | SIMD instruction set (`avx256`, `avx128`, `scalar`) |
| `enable_conversion` | true | Enable adaptive hash→B-tree conversion on range scans |
| `conversion_threshold` | 1 | Number of range scans before triggering conversion |

---

## 7. SQL Interface

### 7.1 Extension Installation

```sql
CREATE EXTENSION blinkhash;
```

### 7.2 Index Creation

```sql
-- Basic index
CREATE INDEX idx_ts ON measurements USING blinkhash (timestamp);

-- With options
CREATE INDEX idx_ts ON measurements 
    USING blinkhash (timestamp)
    WITH (hash_leaf_buckets = 16, fill_factor = 90);

-- Composite key
CREATE INDEX idx_ts_sensor ON measurements 
    USING blinkhash (sensor_id, timestamp);

-- Covering index (index-only scans)
CREATE INDEX idx_ts_covering ON measurements 
    USING blinkhash (timestamp) INCLUDE (value);
```

### 7.3 Operator Classes

Register a B-tree-compatible operator class for each supported type:

```sql
-- int8 (default for timestamps stored as bigint)
CREATE OPERATOR CLASS blinkhash_int8_ops
    DEFAULT FOR TYPE int8 USING blinkhash AS
    OPERATOR 1 <,
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 btint8cmp(int8, int8);

-- timestamp, float8, int4, text, etc. similarly
```

---

## 8. Build System

### 8.1 PGXS Makefile

```makefile
MODULE_big = blinkhash
OBJS = src/blinkhash_handler.o src/blinkhash_am.o src/blinkhash_build.o \
       src/blinkhash_insert.o src/blinkhash_scan.o src/blinkhash_vacuum.o \
       src/blinkhash_cost.o src/blinkhash_validate.o src/blinkhash_utils.o \
       src/blinkhash_wal.o src/blinkhash_page.o \
       lib/tree.o lib/inode.o lib/lnode.o lib/hash.o lib/epoch.o

EXTENSION = blinkhash
DATA = blinkhash--1.0.sql
PGFILEDESC = "blinkhash - B^link-hash hybrid index for time-series"

PG_CXXFLAGS = -std=c++17 -mavx -mavx2 -march=native -fPIC
PG_CFLAGS = -mavx -mavx2 -march=native

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# C++ compilation rules
%.o: %.cpp
	$(CXX) $(PG_CXXFLAGS) $(CPPFLAGS) -I$(shell $(PG_CONFIG) --includedir-server) -c -o $@ $<
```

### 8.2 Dependencies to Replace

| Current Dependency | PostgreSQL Replacement |
|---|---|
| Intel TBB (`tbb::enumerable_thread_specific`) | PostgreSQL backend thread-local storage or `pthread_key_t` |
| `tcmalloc` | PostgreSQL `palloc` / `MemoryContext` |
| `std::atomic` / CAS intrinsics | Same — works in PG backend processes |
| `_mm_pause()` / SIMD intrinsics | Same — works in PG backend processes |
| POSIX threads (`pthread_setaffinity_np`) | PostgreSQL manages its own processes (fork-based, not threaded) — remove CPU pinning |

---

## 9. PostgreSQL Process Model Considerations

### 9.1 Multi-Process vs Multi-Thread

**Critical difference**: PostgreSQL uses a **multi-process** model (one process per connection), not threads. The current $B^{link}$-hash code assumes shared-memory multi-threading where all threads see the same pointers.

**Impact and solution**:
- **Buffer manager pages are in shared memory** — all backends can see the same index pages. This is the standard way indexes work in PG.
- **Remove all `thread_local` / TBB thread-specific storage** — replace with PG backend-local memory (`palloc` in `CurrentMemoryContext`).
- **Epoch-based reclamation**: Instead of per-thread epoch counters in shared memory, use PG's `SnapshotData` or implement a simple shared-memory epoch counter with per-backend local epoch (allocated in shared memory during `_PG_init`).
- **Spinlocks on pages**: These work across processes because pages are in shared memory. The `std::atomic` operations on page contents work correctly since the page buffer is `mmap`'d into each backend's address space.

### 9.2 Shared Memory Allocation

Request shared memory at startup for:
- Epoch counters (one per max-backends)
- Any global index metadata

```c
void _PG_init(void) {
    RequestAdditionalSharedMemory(sizeof(BhSharedState));
    RequestNamedLWLockTranche("blinkhash", NUM_PARTITIONS);
}
```

---

## 10. Testing Strategy

| Test Level | What to Test |
|---|---|
| **Unit tests** | Core data structure operations with PG page layout (insert, lookup, split, convert) |
| **Regression tests** | `CREATE INDEX ... USING blinkhash`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `VACUUM`, crash recovery |
| **Concurrency tests** | `pgbench` with custom scripts: high-concurrency monotonic inserts (time-series pattern), concurrent inserts + range scans |
| **Crash recovery tests** | Kill backend mid-operation, verify index consistency after restart |
| **Performance benchmarks** | Compare against nbtree + BRIN on time-series workloads (monotonic inserts with intermixed range queries) |

---

## 11. Summary — Complete Task List

| # | Task | Effort | Paper Contribution Preserved |
|---|---|---|---|
| 1 | Create PG extension skeleton (PGXS, control file, SQL definitions) | Small | — |
| 2 | Define page layout for internal, B-tree leaf, and hash leaf pages | Medium | Hash leaf buckets layout |
| 3 | Adapt `node_t` pointers → `BlockNumber` | Medium | — |
| 4 | Implement `extern "C"` wrapper API for core `btree_t` operations | Medium | All — core is unchanged |
| 5 | Replace `new`/`delete` with buffer manager `ReadBuffer`/`NewBuffer` | Large | — |
| 6 | Replace TBB epoch with PG-compatible shared-memory epoch | Medium | Epoch-based reclamation concept |
| 7 | Implement `ambuild` (bulk load) | Medium | — |
| 8 | Implement `aminsert` (point insert → `btree_t::insert`) | Medium | Hash leaf insert, bucket probing |
| 9 | Implement scan callbacks (`beginscan`/`gettuple`/`endscan`) | Medium | Range scan, adaptive conversion |
| 10 | Implement WAL record types and redo functions | Large | Lazy linked splitting needs WAL |
| 11 | Implement `ambulkdelete` / `amvacuumcleanup` | Medium | — |
| 12 | Implement `amcostestimate` | Small | — |
| 13 | Implement operator classes for common types | Small | — |
| 14 | Datum ↔ key serialization for variable-length types | Medium | — |
| 15 | Add SIMD runtime detection + scalar fallback | Small | SIMD fingerprinting |
| 16 | Implement hash→B-tree conversion WAL logging | Large | Adaptive conversion |
| 17 | Predicate locking for serializable isolation | Medium | — |
| 18 | Test suite (regression + concurrency + crash recovery) | Large | — |

**Estimated total effort**: 3–6 engineer-months for a production-quality extension.

---

## 12. What You Must NOT Change (Paper Invariants)

To preserve the scientific contributions of the VLDB 2023 paper:

1. **Hash leaf nodes must contain multiple independently-lockable buckets** — this is the core contention reduction mechanism.
2. **Lazy linked splitting** — do not eagerly migrate all entries during split. The `LINKED_LEFT`/`LINKED_RIGHT` states with on-demand stabilization must be preserved.
3. **Adaptive conversion** — range scans on hash leaves must trigger conversion to sorted B-tree leaves. Do not pre-convert or disable this.
4. **Fingerprinting + SIMD probing** — maintain the 8-bit fingerprint per entry and SIMD comparison. Degrade to scalar only on CPUs without AVX support.
5. **Multi-hash-function probing** — keep the 2-function × 4-slot probing scheme for high load factor utilization.
6. **Epoch-based safe memory reclamation** — the mechanism can change (TBB → PG shared memory), but the concept of deferred page reclamation must remain.
7. **Optimistic lock coupling for reads** — readers should validate versions, not acquire exclusive locks. Converting to pessimistic reader locks would negate the concurrency benefits.
