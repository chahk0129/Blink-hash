# B^link-hash — Implementation Sequence & Engineering Plan  ---------------------------

Step-by-step ordering for building a persistent, WAL-backed, PG-integrated
B^link-hash engine.  Each phase is self-contained and testable before moving on.

---

## Phase 0 — Pre-work: unify the core library (WEEK 1)

**Goal:** Merge the uint64 path (`blink-hash`) and string-key path (`blink-hash-str`)
into a single library in `blink-hash-pg/lib/` that templates over both.

| # | Task | Input | Output | How to verify |
|---|------|-------|--------|---------------|
| 0.1 | Copy `GenericKey<N>` from `include/indexkey.h` into `blink-hash-pg/lib/bh_key.h` | `indexkey.h` | Self-contained header with `GenericKey<32>`, `GenericKey<128>`, and `uint64_t` paths | `static_assert(sizeof(GenericKey<32>) == 32)` compiles |
| 0.2 | Adapt `common.h` to typedef both `key64_t` and `StringKey` | `blink-hash-str/lib/common.h` | Unified common.h | Both `btree_t<key64_t, value64_t>` and `btree_t<StringKey, value64_t>` compile |
| 0.3 | Verify the ARM/macOS portability patches from `blink-hash` are in place | Previous session's patches | All `_mm_pause` → `cpu_pause`, `mfence` → portable, no bare `<x86intrin.h>` | `cmake .. && make -j` succeeds on Apple Silicon |
| 0.4 | Add `#include "bh_key.h"` path to CMakeLists | — | Updated include dirs | `make` clean build |

**Deliverable:** `blink-hash-pg/` builds on both x86-64 and arm64, templated
over `uint64_t` and `GenericKey<32>` key types.

---

## Phase 1 — WAL infrastructure: lock-free ring buffer + LSN (WEEKS 2–3)

**Rationale:** WAL is the foundation — nothing is durable until records hit disk.
Build the memory-side pipeline first (ring buffer, LSN assignment), test it
in isolation, then add the I/O engine.

### 1.1  WAL record format

| # | Task | Details |
|---|------|---------|
| 1.1.1 | Define `wal_record_type_t` enum | `INSERT`, `DELETE`, `UPDATE`, `SPLIT_LEAF`, `SPLIT_INTERNAL`, `CONVERT`, `NEW_ROOT`, `CHECKPOINT_BEGIN`, `CHECKPOINT_END`, `STABILIZE` |
| 1.1.2 | Define `wal_record_t` header struct | `{ uint64_t lsn; uint32_t size; uint16_t type; uint16_t crc16; }` — 16 bytes |
| 1.1.3 | Define per-type payloads | `wal_insert_t { key_bytes[]; value_bytes[]; uint64_t node_id; uint32_t bucket_idx; }`, etc. |
| 1.1.4 | Write serializer/deserializer | `wal_record_serialize(record, buf)` / `wal_record_deserialize(buf, record)` |

### 1.2  Thread-local log buffers

| # | Task | Details |
|---|------|---------|
| 1.2.1 | Create `wal_thread_buf_t` — per-thread 64 KB staging buffer | Pre-allocated via `posix_memalign(4096)` for O_DIRECT alignment |
| 1.2.2 | `wal_thread_buf_append(buf, record)` — memcpy record into thread-local buf | Returns immediately; no lock, no atomic |
| 1.2.3 | `wal_thread_buf_flush(buf, ring)` — when buf is ≥ 48 KB, reserve space in global ring via `fetch_add` and memcpy the block | Single atomic op; no lock |

### 1.3  Global MPSC lock-free ring buffer

| # | Task | Details |
|---|------|---------|
| 1.3.1 | Allocate ring buffer — 64 MB, hugepage-backed (`mmap(MAP_HUGETLB)`) | Falls back to regular `mmap` on macOS |
| 1.3.2 | `ring_reserve(ring, size)` — `atomic<uint64_t> write_head.fetch_add(size)` | Returns offset; wrap with `% capacity` |
| 1.3.3 | `ring_commit(ring, offset, size)` — mark the block as readable (set a committed flag) | Allows flusher to know the data is fully written |
| 1.3.4 | Unit test: 16 threads each write 1M records → verify all LSNs are unique and monotonic | — |

**Deliverable:** A standalone ring buffer library (`wal/wal_ring.h`, `wal/wal_ring.cpp`)
that can ingest 50M+ records/sec on a modern machine.

---

## Phase 2 — WAL I/O engine: io_uring + O_DIRECT (WEEKS 3–4)

### 2.1  Flusher thread

| # | Task | Details |
|---|------|---------|
| 2.1.1 | Open WAL file with `O_DIRECT | O_WRONLY | O_CREAT` | 4 KB-aligned buffers required |
| 2.1.2 | Flusher main loop: spin on `ring.read_head < ring.commit_head` | Use `sched_yield()` or `_mm_pause()` when idle |
| 2.1.3 | Batch: accumulate up to 256 KB of committed ring data into a write batch | Pad to 4 KB boundary for NVMe alignment |
| 2.1.4 | Submit write via io_uring SQE (Linux) or `pwrite` (macOS fallback) | On macOS: synchronous `pwrite` + `fcntl(F_FULLFSYNC)` |
| 2.1.5 | On io_uring CQE completion → advance `flushed_lsn` | This is the durable LSN |

### 2.2  WAL file rotation

| # | Task | Details |
|---|------|---------|
| 2.2.1 | WAL segment files: `wal/wal_000001.log`, `wal/wal_000002.log`, ... | Each segment = 64 MB |
| 2.2.2 | Rotate: when current segment reaches 64 MB, `fsync` it, open next | Flusher handles rotation |
| 2.2.3 | Track `{segment_id, offset}` per LSN for recovery | Small in-memory mapping |

### 2.3  Group commit

| # | Task | Details |
|---|------|---------|
| 2.3.1 | `wal_register_commit(lsn, callback)` — worker registers its LSN + wakeup handle | Lock-free queue per flusher batch |
| 2.3.2 | When flusher completes a write → scan commit queue → wake all threads with LSN ≤ flushed_lsn | `pthread_cond_signal` or `futex` |
| 2.3.3 | Benchmark: measure commit latency (should be ≤ 50 µs per group on NVMe) | — |

**Deliverable:** End-to-end WAL pipeline. `wal_append(record)` → appears on disk
within ~50 µs. Workers can `wal_wait_durable(lsn)` for sync commits.

---

## Phase 3 — Instrument the B^link-hash tree with WAL hooks (WEEKS 4–5)

**Goal:** Every mutation in `tree.cpp` emits a WAL record *before* replying success.

### 3.1  Insert path

| # | Task | File | Details |
|---|------|------|---------|
| 3.1.1 | After `leaf->insert()` returns 0, emit `WAL_INSERT` | `tree.cpp` | Record: `{key, value, leaf_id, bucket_or_pos}` |
| 3.1.2 | After `leaf->split()`, emit `WAL_SPLIT_LEAF` | `tree.cpp` | Record: `{old_leaf_id, new_leaf_id, split_key, all_migrated_entries}` |
| 3.1.3 | After `parent->insert(split_key)`, emit `WAL_SPLIT_INTERNAL` | `tree.cpp` | Record: `{inode_id, split_key, new_child_id}` |
| 3.1.4 | After new root creation, emit `WAL_NEW_ROOT` | `tree.cpp` | Record: `{new_root_id, split_key, left_id, right_id, level}` |

### 3.2  Update / Delete path

| # | Task | File | Details |
|---|------|------|---------|
| 3.2.1 | After `leaf->update()`, emit `WAL_UPDATE` | `tree.cpp` | Record: `{key, new_value, leaf_id}` |
| 3.2.2 | After `leaf->remove()`, emit `WAL_DELETE` | `tree.cpp` | Record: `{key, leaf_id}` |

### 3.3  Convert path

| # | Task | File | Details |
|---|------|------|---------|
| 3.3.1 | After `convert()`, emit `WAL_CONVERT` | `tree.cpp` | Record: `{old_hash_leaf_id, new_btree_leaf_ids[], split_keys[], all_entries[]}` — this is a large record |

### 3.4  Stabilize path

| # | Task | File | Details |
|---|------|------|---------|
| 3.4.1 | After `stabilize_bucket()`, emit `WAL_STABILIZE` | `lnode_hash.cpp` | Record: `{leaf_id, bucket_idx, migrated_entries[]}` |

**Deliverable:** Every tree mutation produces a WAL record.  Crash at any point
leaves a replayable log.  Verify: insert 10M keys → kill process → WAL file
contains 10M `WAL_INSERT` records with correct LSN ordering.

---

## Phase 4 — Crash recovery: WAL replay (WEEKS 5–6)

### 4.1  Recovery bootstrap

| # | Task | Details |
|---|------|---------|
| 4.1.1 | `wal_recovery_open(wal_dir)` — scan segment files, find latest checkpoint LSN | Read checkpoint manifest (see Phase 5) |
| 4.1.2 | If no checkpoint: replay from LSN 0 (rebuild tree from scratch) | — |
| 4.1.3 | `wal_recovery_replay(tree, from_lsn)` — sequential read of WAL, dispatch each record type | — |

### 4.2  Per-record-type redo handlers

| # | Record type | Redo action |
|---|-------------|-------------|
| 4.2.1 | `WAL_INSERT` | `tree->insert(key, value, threadInfo)` |
| 4.2.2 | `WAL_DELETE` | `tree->remove(key, threadInfo)` |
| 4.2.3 | `WAL_UPDATE` | `tree->update(key, value, threadInfo)` |
| 4.2.4 | `WAL_SPLIT_LEAF` | Re-split not needed if tree rebuilt from inserts; but for checkpoint recovery, reconstruct leaf pair |
| 4.2.5 | `WAL_CONVERT` | Re-convert hash leaf → btree leaves |
| 4.2.6 | `WAL_NEW_ROOT` | Handled implicitly by insert sequence |

### 4.3  Verification

| # | Task | Details |
|---|------|---------|
| 4.3.1 | Insert 10M keys, `kill -9`, recover, verify all 10M keys present | — |
| 4.3.2 | Insert + delete interleaved, crash, recover, verify exact state | — |
| 4.3.3 | Measure recovery time: should be < 30 seconds for 10M records on NVMe | — |

**Deliverable:** `bh_recover(wal_dir, &tree)` reconstructs the full in-memory
tree from WAL records.

---

## Phase 5 — Fuzzy checkpointing (WEEKS 6–7)

### 5.1  Checkpoint start

| # | Task | Details |
|---|------|---------|
| 5.1.1 | Emit `WAL_CHECKPOINT_BEGIN` with `checkpoint_lsn = current_lsn` | Marks the recovery start point |
| 5.1.2 | Flip a global `checkpoint_epoch` flag (atomic bool) | Workers see this flag |

### 5.2  Copy-on-Write during checkpoint

| # | Task | Details |
|---|------|---------|
| 5.2.1 | When a worker modifies a node while `checkpoint_epoch` is active: allocate a new copy, leave old node untouched | Add `if (checkpoint_active) cow_copy(node)` to `try_upgrade_writelock` path |
| 5.2.2 | The old node pointer goes into a `cow_pending_list` — freed when checkpoint completes | Guarded by epoch GC |
| 5.2.3 | Integration: the Checkpointer thread reads old (stable) nodes; workers write to new copies | No contention between checkpoint reader and worker writers |

### 5.3  Snapshot writer

| # | Task | Details |
|---|------|---------|
| 5.3.1 | Sequential tree scan: DFS traversal starting from root | Reads all internal nodes + all leaf entries |
| 5.3.2 | For each leaf: serialize all `entry_t` pairs into a snapshot file | Format: `{key_len, key_bytes, value_bytes}` per entry |
| 5.3.3 | Use O_DIRECT + preallocated 4 KB write buffers for snapshot I/O | Same io_uring engine as WAL flusher |
| 5.3.4 | Emit `WAL_CHECKPOINT_END` with `end_lsn = current_lsn` after scan completes | — |

### 5.4  Checkpoint manifest

| # | Task | Details |
|---|------|---------|
| 5.4.1 | Write manifest file: `checkpoint_manifest.json` | `{ "snapshot_file": "snap_000005.dat", "checkpoint_lsn": 1000000, "end_lsn": 1500000 }` |
| 5.4.2 | `fsync` the manifest | Must be durable before truncating WAL |
| 5.4.3 | Delete WAL segments with max LSN < `checkpoint_lsn` | Reclaim disk space |

### 5.5  Recovery with checkpoint

| # | Task | Details |
|---|------|---------|
| 5.5.1 | On startup: read latest `checkpoint_manifest.json` | — |
| 5.5.2 | Load snapshot into memory: bulk-insert all entries | Uses `batch_insert` for fast loading |
| 5.5.3 | Replay WAL starting from `checkpoint_lsn` | Only records ≥ `checkpoint_lsn` |
| 5.5.4 | Verify: insert 100M keys → checkpoint at 50M → insert 50M more → crash → recover → all 100M present | — |

**Deliverable:** Full durability with bounded recovery time (recovery replays
only the WAL tail, not the entire history).

---

## Phase 6 — Node identity & persistence layer (WEEKS 7–8)

**Goal:** Assign persistent IDs to nodes so WAL records can reference them
across restarts.

### 6.1  Node ID allocator

| # | Task | Details |
|---|------|---------|
| 6.1.1 | `uint64_t node_id_alloc()` — monotonic 64-bit node IDs | Backed by `atomic<uint64_t>`, persisted in WAL |
| 6.1.2 | Add `uint64_t node_id` field to `node_t` base class | Set on construction |
| 6.1.3 | Node ID → pointer map: `std::unordered_map<uint64_t, node_t*>` | Used during recovery to resolve WAL references |

### 6.2  Page-based persistence (optional, for PG integration)

| # | Task | Details |
|---|------|---------|
| 6.2.1 | Define `bh_page_t` — 8 KB page wrapping a node | Header: `{page_id, node_type, level, lsn, checksum}` |
| 6.2.2 | Buffer pool: fixed-size array of `bh_page_t*` with LRU eviction | For datasets larger than RAM |
| 6.2.3 | `bh_page_read(page_id)` → check buffer pool → read from data file if miss | Pin/unpin semantics like PG |
| 6.2.4 | `bh_page_write(page_id)` → mark dirty → background writer flushes | Dirty page tracking |

**Deliverable:** Nodes have stable identities. WAL records reference nodes by ID.
The buffer pool (6.2) is optional for the in-memory-first approach — needed
only if you want datasets larger than RAM and/or PostgreSQL integration.

---

## Phase 7 — PostgreSQL Index AM integration (WEEKS 8–12)

### 7.1  Extension skeleton

| # | Task | Details |
|---|------|---------|
| 7.1.1 | Create `pg_blinkhash/` directory under `blink-hash-pg/` | See folder structure below |
| 7.1.2 | Write `blinkhash.control` | `comment = 'B^link-hash index AM'`, `default_version = '1.0'` |
| 7.1.3 | Write `blinkhash--1.0.sql` | `CREATE ACCESS METHOD blinkhash TYPE INDEX HANDLER blinkhash_handler;` |
| 7.1.4 | Write PGXS `Makefile` | C++ compilation rules for lib/*.cpp |

### 7.2  AM callbacks (in order of testability)

| # | Callback | Depends on |
|---|----------|------------|
| 7.2.1 | `blinkhash_handler` — returns `IndexAmRoutine` with all flags | Nothing |
| 7.2.2 | `amvalidate` — validate opclass | 7.2.1 |
| 7.2.3 | `ambuildempty` — create empty index | 7.2.1 |
| 7.2.4 | `ambuild` — bulk-load from heap | 7.2.3 |
| 7.2.5 | `aminsert` — single-row insert | 7.2.3 |
| 7.2.6 | `ambeginscan` / `amrescan` / `amendscan` — scan lifecycle | 7.2.5 |
| 7.2.7 | `amgettuple` — point lookup | 7.2.6 |
| 7.2.8 | `amgettuple` — range scan (forward) | 7.2.6 |
| 7.2.9 | `amgetbitmap` — bitmap scan | 7.2.6 |
| 7.2.10 | `amcostestimate` — planner cost model | 7.2.7 |
| 7.2.11 | `ambulkdelete` / `amvacuumcleanup` — VACUUM | 7.2.5 |
| 7.2.12 | `amcanreturn` — index-only scans | 7.2.7 |
| 7.2.13 | `ammarkpos` / `amrestrpos` — merge join support | 7.2.8 |

### 7.3  Datum ↔ Key translation

| # | Task | Details |
|---|------|---------|
| 7.3.1 | `bh_datum_to_key(Datum, Oid typid)` → `key64_t` or `GenericKey<N>` | For int4/int8/float8: direct cast. For text/varchar: `memcpy` into `GenericKey<32>` |
| 7.3.2 | `bh_key_to_datum(key, typid)` → `Datum` | Reverse |
| 7.3.3 | `bh_tid_to_value(ItemPointer)` → `value64_t` | Pack `{blockno, offset}` into 64 bits |
| 7.3.4 | `bh_value_to_tid(value64_t)` → `ItemPointerData` | Unpack |

### 7.4  PG shared memory + epoch adaptation

| # | Task | Details |
|---|------|---------|
| 7.4.1 | Replace TBB `enumerable_thread_specific` with PG `shmem` | `RequestAdditionalSharedMemory` in `_PG_init` |
| 7.4.2 | Per-backend epoch counter in shared memory | Indexed by `MyBackendId` |
| 7.4.3 | Replace `new/delete` node allocation with PG MemoryContext or buffer pool | `palloc` in `TopMemoryContext` for in-memory; buffer manager for on-disk |

---

## Phase 8 — PG WAL integration (WEEKS 12–14)

### 8.1  Custom WAL resource manager

| # | Task | Details |
|---|------|---------|
| 8.1.1 | Register `RM_BLINKHASH_ID` using `RegisterCustomWALResourceManager()` (PG 15+) | — |
| 8.1.2 | Map `wal_record_type_t` → PG `XLogRecData` | `XLogBeginInsert()` / `XLogRegisterData()` / `XLogInsert()` |
| 8.1.3 | Implement redo callbacks for each record type | Called during crash recovery |

### 8.2  Replace standalone WAL with PG WAL

| # | Task | Details |
|---|------|---------|
| 8.2.1 | In `aminsert`: after tree insert, call `XLogInsert(RM_BLINKHASH_ID, XLOG_BH_INSERT)` | Standard PG pattern |
| 8.2.2 | Full page writes (FPW): on first modify after checkpoint, log entire page image | `XLogRegisterBuffer(buf, REGBUF_STANDARD)` |
| 8.2.3 | Group commit: PG handles this via `XLogFlush(lsn)` at commit time | Already built into PG |

---

## Summary: which file to touch and when

```
                       Phase 0          Phase 1-2         Phase 3-5          Phase 6-8
                      ─────────        ───────────       ───────────        ───────────
blink-hash-pg/
├── lib/
│   ├── bh_key.h        ← 0.1
│   ├── common.h        ← 0.2
│   ├── node.h          ← 0.3          ← 6.1.2
│   ├── tree.cpp                                          ← 3.1–3.3
│   ├── lnode_hash.cpp                                    ← 3.4
│   └── ...
├── wal/
│   ├── wal_record.h                   ← 1.1
│   ├── wal_thread_buf.h               ← 1.2
│   ├── wal_thread_buf.cpp             ← 1.2
│   ├── wal_ring.h                     ← 1.3
│   ├── wal_ring.cpp                   ← 1.3
│   ├── wal_flusher.h                  ← 2.1
│   ├── wal_flusher.cpp                ← 2.1
│   ├── wal_writer.h                   ← 2.2
│   ├── wal_writer.cpp                 ← 2.2
│   ├── wal_group_commit.h             ← 2.3
│   ├── wal_group_commit.cpp           ← 2.3
│   ├── wal_recovery.h                             ← 4.1–4.2
│   ├── wal_recovery.cpp                           ← 4.1–4.2
│   ├── wal_checkpoint.h                           ← 5.1–5.4
│   └── wal_checkpoint.cpp                         ← 5.1–5.4
├── pg_blinkhash/                                                          ← 7.1–7.4
│   ├── Makefile
│   ├── blinkhash.control
│   ├── blinkhash--1.0.sql
│   └── src/
│       ├── blinkhash_handler.c                                            ← 7.2.1
│       ├── blinkhash_am.c                                                 ← 7.2.2–7.2.13
│       ├── blinkhash_build.c                                              ← 7.2.3–7.2.4
│       ├── blinkhash_insert.c                                             ← 7.2.5
│       ├── blinkhash_scan.c                                               ← 7.2.6–7.2.9
│       ├── blinkhash_vacuum.c                                             ← 7.2.11
│       ├── blinkhash_cost.c                                               ← 7.2.10
│       ├── blinkhash_validate.c                                           ← 7.2.2
│       ├── blinkhash_utils.c                                              ← 7.3
│       ├── blinkhash_wal.c                                                ← 8.1–8.2
│       └── blinkhash_page.c                                               ← 6.2
└── test/
    ├── wal_ring_test.cpp              ← 1.3.4
    ├── wal_e2e_test.cpp               ← 2.3.3
    ├── recovery_test.cpp                          ← 4.3
    └── checkpoint_test.cpp                        ← 5.5.4
```

---

## Critical path dependencies

```
Phase 0 (unified lib) ──┐
                         ├── Phase 1 (ring buffer) ── Phase 2 (I/O engine) ── Phase 3 (instrument tree)
                         │                                                           │
                         │                                                    Phase 4 (recovery)
                         │                                                           │
                         │                                                    Phase 5 (checkpoint)
                         │                                                           │
                         └────────────────────────── Phase 6 (node IDs) ── Phase 7 (PG AM) ── Phase 8 (PG WAL)
```

Phases 1–5 (standalone WAL) are **independent** of Phase 7–8 (PG integration).
You can build and benchmark the in-memory engine + WAL durability without
PostgreSQL.  PG integration is layered on top.

---

## Risk register

| Risk | Impact | Mitigation |
|------|--------|------------|
| io_uring not available (macOS, old Linux kernels) | No async I/O | Fallback: `pwrite` + `fdatasync` — functional but ~3× slower |
| Hash leaf 256 KB > PG 8 KB page | Can't fit in PG buffer manager | Use multi-page hash leaf (extent of 32 pages) or reduced hash leaf (3–5 buckets per page) |
| CoW during checkpoint doubles memory | OOM on large datasets | Limit CoW to leaf nodes only; internal nodes are small. Or use incremental checkpoint (scan leaves in order, CoW only the frontier) |
| `GenericKey<N>` comparison is `strcmp` — slow for SIMD fingerprint path | Fingerprint still works (it's a prefix hash), but full comparison is slow | For PG: use type-specific comparators from opclass, not generic `strcmp` |
| PG fork-based model vs shared-memory tree | Epoch counters must be in shmem | Already planned in 7.4 — each backend has its own slot |
