# Asynchronous Background Conversion for B^link-hash

## Problem Statement

When `range_lookup()` hits a hash leaf node with `ADAPTATION` enabled, the
**scanning thread** performs the full conversion synchronously:

```
tree::range_lookup()
  → lnode_t::range_lookup()        returns -2 (HASH_NODE + has sibling)
  → tree::convert(leaf, version)
      → lnode_hash_t::convert()    stabilize_all → convertlock → collect → sort → allocate btree nodes
      → tree::batch_insert()       writelock parent → push separator keys → possibly split parent recursively
  → goto restart                   re-traverse from root
```

This blocks the scanner for the full duration of:
1. `stabilize_all()` — lock every bucket, migrate cross-sibling entries
2. `try_convertlock()` — exclusive writelock on the hash node + all its buckets
3. Data copy + `std::sort` of up to `cardinality × entry_num` entries (198 × 32 = 6,336 for uint64)
4. Allocation of N new `lnode_btree_t` nodes
5. `batch_insert()` — writelock parent, push keys, possibly cascade parent splits
6. EBR `markNodeForDeletion` of the old hash node
7. Full tree re-traversal from root

**Measured impact:** On a 10M-key tree, a single conversion can take 100–500 µs,
creating a tail-latency spike that is 10–50× the median range query latency.

---

## Design: Intent-Flag + Background Worker Pool

### Core Idea

Split the conversion into two decoupled phases:

| Phase | Who | What | Blocking? |
|-------|-----|------|-----------|
| **Signal** | Scanner thread | Sets an atomic `CONVERT_PENDING` flag on the hash node, enqueues it | **No** — returns scan results immediately |
| **Execute** | Background worker | Performs the full stabilize → copy → sort → build → batch_insert | **Yes** — but on a dedicated thread, not the scanner |

The scanner still reads from the hash node (collect + sort in thread-local buffer)
and returns results to the caller. It does **not** block on the structural mutation.

### Why This Works Despite the Four Complications

#### 1. Moving Target Parent Node → Already Handled by `batch_insert()`

`tree::batch_insert()` already re-traverses from root with OLC to find the correct
parent, handling concurrent splits. The background worker calls the exact same
`batch_insert()` path. If the parent changed, `batch_insert()` restarts (`goto restart`).
**No new code needed for parent re-discovery.**

#### 2. Write-Write Conflicts → Intent Flag + Version Gate  

We add a `CONVERT_PENDING` state to the node. While this flag is set:
- **Foreground writers** (insert/update/remove): proceed normally — they only lock
  individual buckets, never the whole node. The hash node is still fully functional.
- **Background worker**: when it's ready to materialize, it calls `try_convertlock()`
  which atomically upgrades the node's OLC version. If the version changed since the
  worker snapshotted it, the worker **re-reads** the data (collect + sort again)
  under the convertlock rather than using a stale snapshot.

This means the background worker does a two-phase approach:
1. **Optimistic pre-build** (no locks): copy entries, sort, allocate btree nodes
2. **Validation + commit** (convertlock): re-verify version. If version matches,
   the pre-built nodes are valid. If not, re-collect under lock (falls back to
   current synchronous path, but now on a background thread, still off the scanner's
   critical path).

#### 3. Cascading Parent Splits → Already Handled by `batch_insert()`

`batch_insert()` already handles recursive parent splits, including creating new
root nodes. The background worker passes the same `split_key[]` and `node_t**`
arrays. **No new code needed.**

#### 4. EBR Hazards → Worker Holds EpocheGuard

The background worker thread calls `tree->getThreadInfo()` and enters an
`EpocheGuard` before starting the conversion. The old hash node is marked for
deletion inside `batch_insert()` (which calls `markNodeForDeletion`). Any
foreground scanner that entered its epoch before the parent pointer was swapped
will still see the old node safely — EBR guarantees the old node isn't freed until
all threads from that epoch have exited. **This is identical to the current
synchronous path.**

---

## Detailed Algorithm

### New State Machine for Hash Nodes

Add a single atomic flag to `lnode_hash_t`:

```
enum convert_state_t : uint8_t {
    CONVERT_NONE    = 0,   // no conversion requested
    CONVERT_PENDING = 1,   // flagged for background conversion
    CONVERT_ACTIVE  = 2,   // background worker is executing
};
std::atomic<uint8_t> convert_state{CONVERT_NONE};
```

### Modified `range_lookup` Path (Scanner Thread)

```
tree::range_lookup():
    ...
    auto ret = leaf->range_lookup(min_key, buf, count, range, continued);
    if (ret == -2) {
        // OLD: auto ret_ = convert(leaf, leaf_vstart, threadEpocheInfo);
        //      goto restart;
        
        // NEW: flag for async conversion, then re-scan the same node
        signal_convert(leaf, leaf_vstart);
        
        // Read the hash node's data directly (same as lnode_hash_t::range_lookup)
        // into buf, sort locally, and continue to next sibling
        auto scan_ret = leaf->range_lookup_hash_direct(min_key, buf, count, range);
        if (scan_ret == -1)
            goto restart;   // OLC conflict, retry
        count = scan_ret;
        continued = true;
        // fall through to sibling traversal (existing code)
        ...
    }
```

The key insight: `range_lookup` returning `-2` today means "this is a hash node
and it has a sibling (thus eligible for conversion)". But the scanner **already
collected and sorted the data** inside `lnode_hash_t::range_lookup()` before 
`lnode_t::range_lookup()` checked the `ADAPTATION` flag and returned `-2`. That
work is **thrown away** by the current `goto restart`.

In the new design, we do NOT return `-2`. Instead, the `lnode_t::range_lookup()`
dispatcher lets the hash node's `range_lookup` complete normally and returns
its result. Separately, it signals the background conversion.

### Background Worker Pool

```cpp
class ConvertWorkerPool {
    std::vector<std::thread> workers;
    // Lock-free MPSC queue (bounded ring buffer)
    struct ConvertRequest {
        lnode_hash_t<Key_t, Value_t>* node;
        uint64_t snapshot_version;
    };
    MPSCQueue<ConvertRequest> queue;
    std::atomic<bool> shutdown{false};
    btree_t<Key_t, Value_t>* tree;  // back-pointer for batch_insert

    void worker_loop() {
        while (!shutdown.load(std::memory_order_relaxed)) {
            ConvertRequest req;
            if (queue.try_dequeue(req)) {
                do_convert(req);
            } else {
                std::this_thread::yield();  // or futex wait
            }
        }
    }

    void do_convert(ConvertRequest& req) {
        auto node = req.node;
        
        // 1. Check if still pending (another worker may have grabbed it)
        uint8_t expected = CONVERT_PENDING;
        if (!node->convert_state.compare_exchange_strong(
                expected, CONVERT_ACTIVE,
                std::memory_order_acq_rel))
            return;  // already being converted or converted
        
        // 2. Enter epoch
        auto t = tree->getThreadInfo();
        EpocheGuard guard(t);
        
        // 3. Read version
        bool need_restart = false;
        auto version = static_cast<node_t*>(node)->get_version(need_restart);
        if (need_restart || static_cast<node_t*>(node)->is_obsolete(version)) {
            node->convert_state.store(CONVERT_NONE, std::memory_order_release);
            return;  // node already obsolete
        }
        
        // 4. Delegate to existing tree::convert()
        auto ret = tree->convert(static_cast<lnode_t<Key_t, Value_t>*>(node),
                                 version, t);
        
        // 5. If convert failed (OLC conflict), reset flag for retry
        if (!ret) {
            node->convert_state.store(CONVERT_PENDING, std::memory_order_release);
            // Re-enqueue for retry
            queue.enqueue({node, 0});
        }
        // If succeeded, the node is now obsolete (marked for deletion by
        // batch_insert), so convert_state is irrelevant.
    }
};
```

### Signal Function (Called by Scanner)

```cpp
template <typename Key_t, typename Value_t>
void btree_t<Key_t, Value_t>::signal_convert(
        lnode_hash_t<Key_t, Value_t>* node, uint64_t version) {
    uint8_t expected = CONVERT_NONE;
    if (node->convert_state.compare_exchange_strong(
            expected, CONVERT_PENDING,
            std::memory_order_acq_rel)) {
        // We won the race — enqueue
        convert_pool->enqueue({node, version});
    }
    // else: already PENDING or ACTIVE, nothing to do
}
```

---

## Code Changes

### File 1: `lib/node.h` — No changes needed

The base `node_t` lock word and version protocol remain unchanged.

### File 2: `lib/lnode.h` — Add convert_state to lnode_hash_t

```cpp
// In class lnode_hash_t, add after the left_sibling_ptr member:

    // --- Async conversion state ---
    enum convert_state_t : uint8_t {
        CONVERT_NONE    = 0,
        CONVERT_PENDING = 1,
        CONVERT_ACTIVE  = 2,
    };
    std::atomic<uint8_t> convert_state{CONVERT_NONE};
```

Update the constructors in `lnode_hash.cpp` to initialize `convert_state{CONVERT_NONE}`.
Both the default constructor and the split constructor should add this.

### File 3: `lib/lnode.cpp` — Modify range_lookup dispatcher

**Current code** (lines ~173-182):
```cpp
case HASH_NODE:
    #ifdef ADAPTATION
    if(sibling_ptr != nullptr) // convert flag
        return -2;
    #endif
    return (static_cast<lnode_hash_t<Key_t, Value_t>*>(this))->range_lookup(key, buf, count, range);
```

**New code:**
```cpp
case HASH_NODE:
    #ifdef ADAPTATION
    #ifdef ASYNC_ADAPT
    // Signal background conversion but DO NOT block — let the
    // hash node's range_lookup proceed and return results.
    if(sibling_ptr != nullptr){
        auto hash_node = static_cast<lnode_hash_t<Key_t, Value_t>*>(this);
        uint8_t expected = lnode_hash_t<Key_t, Value_t>::CONVERT_NONE;
        hash_node->convert_state.compare_exchange_strong(
            expected,
            lnode_hash_t<Key_t, Value_t>::CONVERT_PENDING,
            std::memory_order_acq_rel,
            std::memory_order_relaxed);
        // Flag is set; tree::range_lookup will check and enqueue.
        // Fall through to scan the hash node normally.
    }
    #else
    if(sibling_ptr != nullptr) // convert flag
        return -2;
    #endif
    #endif
    return (static_cast<lnode_hash_t<Key_t, Value_t>*>(this))->range_lookup(key, buf, count, range);
```

### File 4: `lib/tree.h` — Add ConvertWorkerPool and signal_convert

Add these new members and methods to `class btree_t`:

```cpp
// ---- New includes at top of tree.h ----
#include <queue>
#include <mutex>
#include <condition_variable>

// ---- Inside class btree_t, private section ----
#ifdef ASYNC_ADAPT

    // ---- Convert request queue (simple bounded MPSC) ----
    struct ConvertRequest {
        node_t* node;            // the hash leaf to convert
        uint64_t version_hint;   // snapshot version (hint only)
    };

    std::mutex convert_mu;
    std::condition_variable convert_cv;
    std::queue<ConvertRequest> convert_queue;
    std::vector<std::thread> convert_workers;
    std::atomic<bool> convert_shutdown{false};

    static constexpr int CONVERT_WORKERS = 2;  // tunable

    void convert_worker_loop();
    void signal_convert(node_t* node, uint64_t version);
    void start_convert_workers();
    void stop_convert_workers();

#endif
```

Add to the constructor body:
```cpp
btree_t(){
    root = static_cast<node_t*>(new lnode_hash_t<Key_t, Value_t>());
    #ifndef FINGERPRINT
    memset(&EMPTY<Key_t>, 0, sizeof(EMPTY<Key_t>));
    #endif
    #ifdef ASYNC_ADAPT
    start_convert_workers();
    #endif
}
```

Add to the destructor:
```cpp
~btree_t(){
    #ifdef ASYNC_ADAPT
    stop_convert_workers();
    #endif
}
```

### File 5: `lib/tree.cpp` — Implement async conversion methods

Add these implementations:

```cpp
#ifdef ASYNC_ADAPT

template <typename Key_t, typename Value_t>
void btree_t<Key_t, Value_t>::start_convert_workers(){
    for(int i = 0; i < CONVERT_WORKERS; i++){
        convert_workers.emplace_back([this]{ convert_worker_loop(); });
    }
}

template <typename Key_t, typename Value_t>
void btree_t<Key_t, Value_t>::stop_convert_workers(){
    {
        std::lock_guard<std::mutex> lk(convert_mu);
        convert_shutdown.store(true, std::memory_order_release);
    }
    convert_cv.notify_all();
    for(auto& w : convert_workers)
        w.join();
}

template <typename Key_t, typename Value_t>
void btree_t<Key_t, Value_t>::signal_convert(node_t* node, uint64_t version){
    auto hash_node = static_cast<lnode_hash_t<Key_t, Value_t>*>(
                         static_cast<lnode_t<Key_t, Value_t>*>(node));

    uint8_t expected = lnode_hash_t<Key_t, Value_t>::CONVERT_NONE;
    if(!hash_node->convert_state.compare_exchange_strong(
            expected,
            lnode_hash_t<Key_t, Value_t>::CONVERT_PENDING,
            std::memory_order_acq_rel,
            std::memory_order_relaxed)){
        return;  // already PENDING or ACTIVE
    }

    {
        std::lock_guard<std::mutex> lk(convert_mu);
        convert_queue.push({node, version});
    }
    convert_cv.notify_one();
}

template <typename Key_t, typename Value_t>
void btree_t<Key_t, Value_t>::convert_worker_loop(){
    while(true){
        ConvertRequest req;
        {
            std::unique_lock<std::mutex> lk(convert_mu);
            convert_cv.wait(lk, [this]{
                return convert_shutdown.load(std::memory_order_relaxed)
                       || !convert_queue.empty();
            });
            if(convert_shutdown.load(std::memory_order_relaxed)
               && convert_queue.empty())
                return;
            req = convert_queue.front();
            convert_queue.pop();
        }

        auto hash_leaf = static_cast<lnode_hash_t<Key_t, Value_t>*>(
                             static_cast<lnode_t<Key_t, Value_t>*>(req.node));

        // Transition PENDING → ACTIVE
        uint8_t expected = lnode_hash_t<Key_t, Value_t>::CONVERT_PENDING;
        if(!hash_leaf->convert_state.compare_exchange_strong(
                expected,
                lnode_hash_t<Key_t, Value_t>::CONVERT_ACTIVE,
                std::memory_order_acq_rel)){
            continue;  // someone else handled it, or node is obsolete
        }

        // Fresh version read — the hint version may be stale
        bool need_restart = false;
        auto version = (static_cast<node_t*>(hash_leaf))->get_version(need_restart);
        if(need_restart){
            // Node is locked or obsolete, reset and re-enqueue
            hash_leaf->convert_state.store(
                lnode_hash_t<Key_t, Value_t>::CONVERT_PENDING,
                std::memory_order_release);
            {
                std::lock_guard<std::mutex> lk(convert_mu);
                convert_queue.push(req);
            }
            convert_cv.notify_one();
            std::this_thread::yield();
            continue;
        }

        // Enter epoch and perform conversion
        auto threadInfo = getThreadInfo();
        {
            EpocheGuard guard(threadInfo);
            auto ret = convert(
                static_cast<lnode_t<Key_t, Value_t>*>(hash_leaf),
                version,
                threadInfo);

            if(!ret){
                // OLC conflict — reset to PENDING for retry
                hash_leaf->convert_state.store(
                    lnode_hash_t<Key_t, Value_t>::CONVERT_PENDING,
                    std::memory_order_release);
                {
                    std::lock_guard<std::mutex> lk(convert_mu);
                    convert_queue.push(req);
                }
                convert_cv.notify_one();
                std::this_thread::yield();
            }
            // On success: node is now obsolete (marked for EBR deletion).
            // convert_state is irrelevant on an obsolete node.
        }
    }
}

#endif // ASYNC_ADAPT
```

### File 6: `lib/tree.cpp` — Modify `range_lookup()` to not block on conversion

**Current code** (in `tree::range_lookup`, the `-2` handling):
```cpp
    auto ret = leaf->range_lookup(min_key, buf, count, range, continued);
    if(ret == -1)
        goto restart;
    else if(ret == -2){
        auto ret_ = convert(leaf, leaf_vstart, threadEpocheInfo);
        goto restart;
    }
    continued = true;
```

**New code:**
```cpp
    auto ret = leaf->range_lookup(min_key, buf, count, range, continued);
    if(ret == -1)
        goto restart;
#ifndef ASYNC_ADAPT
    else if(ret == -2){
        auto ret_ = convert(leaf, leaf_vstart, threadEpocheInfo);
        goto restart;
    }
#else
    // When ASYNC_ADAPT is enabled, range_lookup never returns -2.
    // The lnode_t dispatcher already flagged the node and let the
    // hash scan proceed. If the flag was set, enqueue into the
    // background worker pool (idempotent — duplicate enqueues are
    // filtered by the CAS on convert_state).
    if(leaf->type == lnode_t<Key_t, Value_t>::HASH_NODE){
        auto hash_leaf = static_cast<lnode_hash_t<Key_t, Value_t>*>(leaf);
        if(hash_leaf->convert_state.load(std::memory_order_acquire)
                == lnode_hash_t<Key_t, Value_t>::CONVERT_PENDING){
            signal_convert(static_cast<node_t*>(leaf), leaf_vstart);
        }
    }
#endif
    continued = true;
```

### File 7: `lib/CMakeLists.txt` — Add ASYNC_ADAPT build target

Add a new library target alongside `adapt` and `blinkhash`:

```cmake
add_library(blinkhash_async STATIC ${Blinkhash_SRC})
target_compile_definitions(blinkhash_async PUBLIC
    ${BLINKHASH_EXTRA_DEFS}
    -DFINGERPRINT -DSAMPLING -DLINKED -DADAPTATION -DASYNC_ADAPT)
target_link_libraries(blinkhash_async TBB::tbb)
INSTALL(TARGETS blinkhash_async
    ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})
```

---

## Sequence Diagram

```
Scanner Thread                  Background Worker              Tree
     │                               │                          │
     ├─ range_lookup(key,50)         │                          │
     ├─ traverse to hash leaf        │                          │
     ├─ lnode_t::range_lookup()      │                          │
     │   ├─ detect HASH_NODE         │                          │
     │   │  + has sibling            │                          │
     │   ├─ CAS convert_state        │                          │
     │   │  NONE → PENDING           │                          │
     │   ├─ hash::range_lookup()     │                          │
     │   │  ├─ collect buckets       │                          │
     │   │  ├─ sort entries          │                          │
     │   │  └─ copy to buf[]         │                          │
     │   └─ return count ────────────┼──────────────────────────┤
     ├─ signal_convert(leaf,ver) ────┼─► enqueue(leaf,ver)      │
     ├─ continue to next sibling     │                          │
     ├─ return results to caller     │                          │
     │                               │                          │
     │                               ├─ dequeue(leaf,ver)       │
     │                               ├─ CAS PENDING→ACTIVE      │
     │                               ├─ getThreadInfo()         │
     │                               ├─ EpocheGuard             │
     │                               ├─ read version            │
     │                               ├─ convert(leaf, ver)      │
     │                               │  ├─ stabilize_all        │
     │                               │  ├─ try_convertlock      │
     │                               │  ├─ collect+sort+alloc   │
     │                               │  └─ return btree nodes   │
     │                               ├─ batch_insert(keys,      │
     │                               │    nodes, leaf)           │
     │                               │  ├─ find parent (OLC)    │
     │                               │  ├─ writelock parent     │
     │                               │  ├─ push separators      │
     │                               │  ├─ mark old node EBR    │
     │                               │  └─ unlock parent        │
     │                               ├─ ~EpocheGuard            │
     │                               └─ done                    │
```

---

## Test Plan

### Test 1: `test/bench_range_latency.cpp` — Per-Query Latency Histogram

Measures p50, p99, p999, max latency for range queries under two modes:
synchronous (`adapt` library) vs asynchronous (`blinkhash_async` library).

```cpp
#include "tree.h"
#include <ctime>
#include <sys/time.h>
#include <vector>
#include <thread>
#include <iostream>
#include <random>
#include <algorithm>
#include <numeric>
#include <cstring>

using Key_t = uint64_t;
using Value_t = uint64_t;
using namespace BLINK_HASH;

static inline uint64_t now_ns(){
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

inline void pin_to_core(size_t thread_id){
#ifdef __linux__
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(thread_id % 64, &cpu_set);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
#else
    (void)thread_id;
#endif
}

int main(int argc, char* argv[]){
    if(argc < 4){
        std::cerr << "Usage: " << argv[0]
                  << " <num_data> <num_threads> <num_queries_per_thread>"
                  << std::endl;
        return 1;
    }

    int num_data    = atoi(argv[1]);
    int num_threads = atoi(argv[2]);
    int num_queries = atoi(argv[3]);
    int range       = 50;

    // --- Generate keys ---
    Key_t* keys = new Key_t[num_data];
    for(int i = 0; i < num_data; i++)
        keys[i] = i + 1;
    std::shuffle(keys, keys + num_data, std::mt19937{std::random_device{}()});

    // --- Build tree (insert-only, no conversion) ---
    btree_t<Key_t, Value_t>* tree = new btree_t<Key_t, Value_t>();

    // Parallel insert
    int warmup_threads = std::min(num_threads, 64);
    {
        std::vector<std::thread> threads;
        for(int t = 0; t < warmup_threads; t++){
            threads.emplace_back([&, t]{
                pin_to_core(t);
                size_t chunk = num_data / warmup_threads;
                size_t from  = chunk * t;
                size_t to    = (t == warmup_threads - 1) ? num_data : from + chunk;
                for(size_t i = from; i < to; i++){
                    auto ti = tree->getThreadInfo();
                    tree->insert(keys[i], keys[i], ti);
                }
            });
        }
        for(auto& th : threads) th.join();
    }

    std::cout << "Inserted " << num_data << " keys, height = "
              << tree->height() + 1 << std::endl;

    // --- Range query latency benchmark ---
    // Each thread does num_queries range scans and records per-query latency
    std::vector<std::vector<uint64_t>> latencies(num_threads);
    for(auto& v : latencies) v.resize(num_queries);

    {
        std::vector<std::thread> threads;
        for(int t = 0; t < num_threads; t++){
            threads.emplace_back([&, t]{
                pin_to_core(t);
                std::mt19937 rng(t * 12345 + 67890);
                std::uniform_int_distribution<Key_t> dist(1, num_data);

                for(int q = 0; q < num_queries; q++){
                    Key_t min_key = dist(rng);
                    Value_t buf[range];

                    auto ti = tree->getThreadInfo();
                    uint64_t t0 = now_ns();
                    tree->range_lookup(min_key, range, buf, ti);
                    uint64_t t1 = now_ns();

                    latencies[t][q] = t1 - t0;
                }
            });
        }
        for(auto& th : threads) th.join();
    }

    // --- Merge and report ---
    std::vector<uint64_t> all;
    all.reserve(num_threads * num_queries);
    for(auto& v : latencies)
        all.insert(all.end(), v.begin(), v.end());
    std::sort(all.begin(), all.end());

    size_t n = all.size();
    auto percentile = [&](double p) -> uint64_t {
        size_t idx = (size_t)(p / 100.0 * n);
        if(idx >= n) idx = n - 1;
        return all[idx];
    };

    double mean = std::accumulate(all.begin(), all.end(), 0.0) / n;

    std::cout << "\n=== Range Query Latency (" << n << " queries, range="
              << range << ") ===" << std::endl;
    std::cout << "  mean:  " << (uint64_t)mean << " ns" << std::endl;
    std::cout << "  p50:   " << percentile(50)  << " ns" << std::endl;
    std::cout << "  p90:   " << percentile(90)  << " ns" << std::endl;
    std::cout << "  p99:   " << percentile(99)  << " ns" << std::endl;
    std::cout << "  p99.9: " << percentile(99.9) << " ns" << std::endl;
    std::cout << "  max:   " << all.back()       << " ns" << std::endl;

    // Count spike queries (> 10× median)
    uint64_t spike_threshold = percentile(50) * 10;
    int spikes = 0;
    for(auto& l : all)
        if(l > spike_threshold) spikes++;
    std::cout << "  spikes (>10x median): " << spikes
              << " (" << (double)spikes / n * 100 << "%)" << std::endl;

    delete[] keys;
    return 0;
}
```

### Test 2: `test/bench_range_throughput.cpp` — Throughput Under Mixed Workload

Runs concurrent inserts + range queries simultaneously to stress the async
conversion under write-write conflict conditions.

```cpp
#include "tree.h"
#include <vector>
#include <thread>
#include <iostream>
#include <random>
#include <algorithm>
#include <atomic>
#include <cstring>

using Key_t = uint64_t;
using Value_t = uint64_t;
using namespace BLINK_HASH;

static inline uint64_t now_ns(){
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

inline void pin_to_core(size_t thread_id){
#ifdef __linux__
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(thread_id % 64, &cpu_set);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
#else
    (void)thread_id;
#endif
}

int main(int argc, char* argv[]){
    if(argc < 5){
        std::cerr << "Usage: " << argv[0]
                  << " <initial_keys> <scan_threads> <insert_threads>"
                  << " <duration_sec>" << std::endl;
        return 1;
    }

    int initial_keys   = atoi(argv[1]);
    int scan_threads   = atoi(argv[2]);
    int insert_threads = atoi(argv[3]);
    int duration_sec   = atoi(argv[4]);
    int range          = 50;

    // --- Generate keys ---
    Key_t* keys = new Key_t[initial_keys];
    for(int i = 0; i < initial_keys; i++)
        keys[i] = i + 1;
    std::shuffle(keys, keys + initial_keys, std::mt19937{std::random_device{}()});

    // --- Build initial tree ---
    btree_t<Key_t, Value_t>* tree = new btree_t<Key_t, Value_t>();
    {
        std::vector<std::thread> threads;
        for(int t = 0; t < std::min(insert_threads, 64); t++){
            threads.emplace_back([&, t]{
                pin_to_core(t);
                size_t chunk = initial_keys / std::min(insert_threads, 64);
                size_t from  = chunk * t;
                size_t to    = (t == std::min(insert_threads, 64) - 1)
                               ? initial_keys : from + chunk;
                for(size_t i = from; i < to; i++){
                    auto ti = tree->getThreadInfo();
                    tree->insert(keys[i], keys[i], ti);
                }
            });
        }
        for(auto& th : threads) th.join();
    }

    std::cout << "Initial: " << initial_keys << " keys, height = "
              << tree->height() + 1 << std::endl;

    // --- Mixed workload ---
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> total_scans{0};
    std::atomic<uint64_t> total_inserts{0};
    std::atomic<Key_t> next_key{(Key_t)initial_keys + 1};

    // Scan threads
    std::vector<std::thread> scanners;
    for(int t = 0; t < scan_threads; t++){
        scanners.emplace_back([&, t]{
            pin_to_core(t);
            std::mt19937 rng(t * 111 + 222);
            uint64_t local_count = 0;
            while(!stop.load(std::memory_order_relaxed)){
                Key_t min_key = rng() % (next_key.load(std::memory_order_relaxed) - 1) + 1;
                Value_t buf[range];
                auto ti = tree->getThreadInfo();
                tree->range_lookup(min_key, range, buf, ti);
                local_count++;
            }
            total_scans.fetch_add(local_count, std::memory_order_relaxed);
        });
    }

    // Insert threads
    std::vector<std::thread> inserters;
    for(int t = 0; t < insert_threads; t++){
        inserters.emplace_back([&, t]{
            pin_to_core(scan_threads + t);
            uint64_t local_count = 0;
            while(!stop.load(std::memory_order_relaxed)){
                Key_t k = next_key.fetch_add(1, std::memory_order_relaxed);
                auto ti = tree->getThreadInfo();
                tree->insert(k, k, ti);
                local_count++;
            }
            total_inserts.fetch_add(local_count, std::memory_order_relaxed);
        });
    }

    // Timer
    std::this_thread::sleep_for(std::chrono::seconds(duration_sec));
    stop.store(true, std::memory_order_relaxed);

    for(auto& th : scanners)  th.join();
    for(auto& th : inserters) th.join();

    std::cout << "\n=== Mixed Workload (" << duration_sec << "s) ===" << std::endl;
    std::cout << "  scan throughput:   "
              << total_scans.load() / (double)duration_sec / 1e6
              << " Mops/sec" << std::endl;
    std::cout << "  insert throughput: "
              << total_inserts.load() / (double)duration_sec / 1e6
              << " Mops/sec" << std::endl;
    std::cout << "  total keys:        "
              << next_key.load() - 1 << std::endl;
    std::cout << "  height:            "
              << tree->height() + 1 << std::endl;

    delete[] keys;
    return 0;
}
```

### Test 3: `test/bench_convert_count.cpp` — Conversion Counter Verification

Confirms background conversions actually happen by counting them.

```cpp
#include "tree.h"
#include <vector>
#include <thread>
#include <iostream>
#include <random>
#include <algorithm>
#include <atomic>

using Key_t = uint64_t;
using Value_t = uint64_t;
using namespace BLINK_HASH;

// Global counter — increment inside convert() or convert_worker_loop
// when a conversion succeeds. Defined in tree.cpp under ASYNC_ADAPT.
#ifdef ASYNC_ADAPT
namespace BLINK_HASH {
    extern std::atomic<uint64_t> async_convert_count;
}
#endif

inline void pin_to_core(size_t thread_id){
#ifdef __linux__
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(thread_id % 64, &cpu_set);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
#else
    (void)thread_id;
#endif
}

int main(int argc, char* argv[]){
    if(argc < 3){
        std::cerr << "Usage: " << argv[0]
                  << " <num_data> <num_threads>" << std::endl;
        return 1;
    }

    int num_data    = atoi(argv[1]);
    int num_threads = atoi(argv[2]);
    int range       = 50;

    Key_t* keys = new Key_t[num_data];
    for(int i = 0; i < num_data; i++)
        keys[i] = i + 1;
    std::shuffle(keys, keys + num_data, std::mt19937{std::random_device{}()});

    btree_t<Key_t, Value_t>* tree = new btree_t<Key_t, Value_t>();

    // Insert all keys
    {
        std::vector<std::thread> threads;
        for(int t = 0; t < num_threads; t++){
            threads.emplace_back([&, t]{
                pin_to_core(t);
                size_t chunk = num_data / num_threads;
                size_t from  = chunk * t;
                size_t to    = (t == num_threads - 1) ? num_data : from + chunk;
                for(size_t i = from; i < to; i++){
                    auto ti = tree->getThreadInfo();
                    tree->insert(keys[i], keys[i], ti);
                }
            });
        }
        for(auto& th : threads) th.join();
    }

    std::cout << "Inserted " << num_data << " keys" << std::endl;
    std::cout << "Height before scans: " << tree->height() + 1 << std::endl;

    // Run range queries — this should trigger conversions
    std::atomic<uint64_t> total_queries{0};
    {
        std::vector<std::thread> threads;
        for(int t = 0; t < num_threads; t++){
            threads.emplace_back([&, t]{
                pin_to_core(t);
                std::mt19937 rng(t);
                std::uniform_int_distribution<Key_t> dist(1, num_data);
                for(int q = 0; q < 100000; q++){
                    Key_t min_key = dist(rng);
                    Value_t buf[range];
                    auto ti = tree->getThreadInfo();
                    tree->range_lookup(min_key, range, buf, ti);
                }
                total_queries.fetch_add(100000);
            });
        }
        for(auto& th : threads) th.join();
    }

    std::cout << "Total queries: " << total_queries.load() << std::endl;
    std::cout << "Height after scans: " << tree->height() + 1 << std::endl;

#ifdef ASYNC_ADAPT
    // Allow background workers to finish pending conversions
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    std::cout << "Async conversions completed: "
              << async_convert_count.load() << std::endl;
#else
    std::cout << "(Synchronous mode — conversions happen inline)" << std::endl;
#endif

    delete[] keys;
    return 0;
}
```

### `test/CMakeLists.txt` — Build targets for both modes

Add these targets to the existing test CMakeLists:

```cmake
# ---- Latency benchmark (sync baseline) ----
add_executable(bench_range_latency_sync bench_range_latency.cpp)
target_link_libraries(bench_range_latency_sync adapt pthread)

# ---- Latency benchmark (async) ----
add_executable(bench_range_latency_async bench_range_latency.cpp)
target_link_libraries(bench_range_latency_async blinkhash_async pthread)

# ---- Throughput benchmark (sync baseline) ----
add_executable(bench_range_throughput_sync bench_range_throughput.cpp)
target_link_libraries(bench_range_throughput_sync adapt pthread)

# ---- Throughput benchmark (async) ----
add_executable(bench_range_throughput_async bench_range_throughput.cpp)
target_link_libraries(bench_range_throughput_async blinkhash_async pthread)

# ---- Conversion counter (async only) ----
add_executable(bench_convert_count bench_convert_count.cpp)
target_link_libraries(bench_convert_count blinkhash_async pthread)
```

---

## How to Run the Comparison

```bash
cd build && cmake .. && make -j

# 1. Latency histogram — synchronous baseline
./test/bench_range_latency_sync 5000000 4 100000

# 2. Latency histogram — async conversion
./test/bench_range_latency_async 5000000 4 100000

# 3. Mixed throughput — sync
./test/bench_range_throughput_sync 5000000 2 2 10

# 4. Mixed throughput — async
./test/bench_range_throughput_async 5000000 2 2 10

# 5. Verify async conversions happen
./test/bench_convert_count 5000000 4
```

### What to Look For

| Metric | Sync (baseline) | Async (expected) |
|--------|-----------------|------------------|
| p50 latency | ~1–5 µs | ~1–5 µs (same) |
| p99 latency | ~10–50 µs | ~5–15 µs (lower) |
| p99.9 latency | **100–500 µs** (spike) | **10–30 µs** (no spike) |
| max latency | **200–1000 µs** | **30–80 µs** |
| spikes (>10× median) | 0.1–1% | **~0%** |
| scan throughput (mixed) | X Mops | ≥X Mops (same or better) |
| insert throughput (mixed) | Y Mops | ≥Y Mops (same or better) |
| async conversions | N/A | >0 (confirms workers ran) |

---

## Implementation Checklist

- [ ] Add `convert_state` atomic to `lnode_hash_t` (lnode.h)
- [ ] Initialize `convert_state` in both constructors (lnode_hash.cpp)
- [ ] Add `#ifdef ASYNC_ADAPT` block in `lnode_t::range_lookup` dispatcher (lnode.cpp)
- [ ] Add `ConvertWorkerPool` members to `btree_t` (tree.h)
- [ ] Add worker lifecycle to constructor/destructor (tree.h)
- [ ] Implement `signal_convert`, `convert_worker_loop`, `start/stop` (tree.cpp)
- [ ] Modify `tree::range_lookup` `-2` handling for `ASYNC_ADAPT` (tree.cpp)
- [ ] Add `async_convert_count` global counter for diagnostics (tree.cpp)
- [ ] Add `blinkhash_async` library target (lib/CMakeLists.txt)
- [ ] Create `bench_range_latency.cpp` (test/)
- [ ] Create `bench_range_throughput.cpp` (test/)
- [ ] Create `bench_convert_count.cpp` (test/)
- [ ] Add test targets to `test/CMakeLists.txt`
- [ ] Build, run sync baseline, run async, compare

---

## Optional Future Enhancements

1. **Lock-free MPSC ring** instead of `std::mutex` + `std::queue` for the
   convert request queue — eliminates mutex contention in signal_convert.

2. **Adaptive worker count** — monitor queue depth and spin up/down workers.

3. **Batched conversions** — if multiple hash nodes are flagged, the worker
   can process them in a batch to amortize re-traversal cost.

4. **Conversion priority** — nodes with higher scan frequency get converted
   first. Requires an atomic counter on each hash node tracking scan hits.

5. **Hybrid fallback** — if the background queue is full (backpressure),
   fall back to synchronous conversion on the scanner thread to prevent
   unbounded queue growth.
