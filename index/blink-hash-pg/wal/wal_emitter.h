#ifndef BLINK_HASH_WAL_EMITTER_H__
#define BLINK_HASH_WAL_EMITTER_H__

/*
 * wal_emitter.h — High-level WAL emission API for tree mutations
 *
 * Each wal_emit_*() function:
 *   1. Serializes the payload into a stack buffer.
 *   2. Calls wal_record_serialize() to wrap it in a RecordHeader.
 *   3. Appends the record to the caller's ThreadBuf.
 *   4. The ThreadBuf auto-flushes to the RingBuffer at threshold.
 *
 * Thread-safety: each worker owns its own ThreadBuf (thread-local).
 * No locking is needed in the emit path.
 */

#include "wal_record.h"
#include "wal_thread_buf.h"
#include "wal_ring.h"

#include <atomic>
#include <cstdint>
#include <cstring>

namespace BLINK_HASH {
namespace WAL {

/*
 * Monotonically increasing LSN.  Every WAL record gets a unique LSN.
 * We use relaxed ordering for the fetch_add because the ring buffer's
 * commit ordering already provides the necessary visibility guarantees.
 */
extern std::atomic<uint64_t> g_lsn;

inline uint64_t next_lsn() {
    return g_lsn.fetch_add(1, std::memory_order_relaxed) + 1;
}

/*
 * Every node (internal or leaf) receives a unique, persistent 64-bit ID.
 * This ID is used in WAL records to reference nodes across restarts.
 *
 * On recovery, the allocator is re-seeded to max(recovered_node_id) + 1.
 */
extern std::atomic<uint64_t> g_node_id;

inline uint64_t alloc_node_id() {
    return g_node_id.fetch_add(1, std::memory_order_relaxed) + 1;
}

inline void reseed_node_id(uint64_t max_seen) {
    uint64_t cur = g_node_id.load(std::memory_order_relaxed);
    while (cur <= max_seen) {
        if (g_node_id.compare_exchange_weak(cur, max_seen + 1,
                std::memory_order_relaxed))
            break;
    }
}

inline void reseed_lsn(uint64_t max_seen) {
    uint64_t cur = g_lsn.load(std::memory_order_relaxed);
    while (cur <= max_seen) {
        if (g_lsn.compare_exchange_weak(cur, max_seen + 1,
                std::memory_order_relaxed))
            break;
    }
}



/*
 * The global ring buffer that all emitters write into.
 * Set by wal_init() before any tree operations.
 */
extern RingBuffer*  g_wal_ring;
extern bool         g_wal_enabled;

inline void wal_init(RingBuffer* ring) {
    g_wal_ring   = ring;
    g_wal_enabled = true;
}

inline void wal_disable() {
    g_wal_enabled = false;
}




ThreadBuf& wal_get_thread_buf();


inline void wal_flush_thread_buf() {
    if (g_wal_enabled && g_wal_ring)
        wal_get_thread_buf().flush(*g_wal_ring);
}

/*
 * Maximum serialized record size.  The largest record is CONVERT,
 * which contains all entries from a hash leaf.  Hash leaf capacity
 * is 256 KB, but the WAL record only contains (key, value) pairs.
 * With entry_num=32 and cardinality 32 buckets, max 1024 entries.
 * Each entry is sizeof(Key_t) + sizeof(Value_t) ≤ 128+8 = 136 bytes.
 * Plus header + payload header ≈ 256 bytes.
 * Worst case: 140 KB.  We cap at 256 KB (FLUSH_BATCH_MAX).
 */
static constexpr size_t WAL_RECORD_BUF_SIZE = 4096;

uint64_t wal_emit(RecordType type,
                  const void* payload, size_t payload_len);

/*
 * INSERT: after a successful leaf->insert().
 *
 * @param node_id    persistent ID of the leaf node
 * @param bucket_idx bucket index (for hash leaves) or position (btree)
 * @param key        pointer to key bytes
 * @param key_len    length of key in bytes
 * @param value      the 64-bit value
 */
uint64_t wal_emit_insert(uint64_t node_id, uint32_t bucket_idx,
                         const void* key, uint32_t key_len,
                         uint64_t value);

/*
 * DELETE: after a successful leaf->remove().
 */
uint64_t wal_emit_delete(uint64_t node_id,
                         const void* key, uint32_t key_len);

/*
 * UPDATE: after a successful leaf->update().
 */
uint64_t wal_emit_update(uint64_t node_id,
                         const void* key, uint32_t key_len,
                         uint64_t new_value);

/*
 * SPLIT_LEAF: after a leaf split.
 *
 * @param old_leaf_id    node_id of the original leaf
 * @param new_leaf_id    node_id of the newly created leaf
 * @param split_key      pointer to the split key bytes
 * @param split_key_len  length of the split key
 */
uint64_t wal_emit_split_leaf(uint64_t old_leaf_id,
                             uint64_t new_leaf_id,
                             const void* split_key,
                             uint32_t split_key_len);

/*
 * SPLIT_INTERNAL: after an internal node split.
 *
 * @param inode_id       node_id of the original internal node
 * @param new_child_id   node_id of the new child created by the split
 * @param split_key      the promoted key
 * @param split_key_len  length of the promoted key
 */
uint64_t wal_emit_split_internal(uint64_t inode_id,
                                 uint64_t new_child_id,
                                 const void* split_key,
                                 uint32_t split_key_len);

/*
 * NEW_ROOT: after a new root is created due to a split.
 *
 * @param new_root_id    node_id of the new root
 * @param left_child_id  node_id of the left child
 * @param right_child_id node_id of the right child
 * @param split_key      the key separating left and right
 * @param split_key_len  length of the split key
 * @param level          level of the new root
 */
uint64_t wal_emit_new_root(uint64_t new_root_id,
                           uint64_t left_child_id,
                           uint64_t right_child_id,
                           const void* split_key,
                           uint32_t split_key_len,
                           uint8_t level);

/*
 * CONVERT: after a hash leaf is converted to btree leaves.
 *
 * @param old_hash_leaf_id   node_id of the hash leaf being replaced
 * @param new_leaf_ids       array of node_ids for the new btree leaves
 * @param num_new_leaves     count of new btree leaves
 * @param entries            flat array of (key_bytes, value) pairs
 * @param num_entries        total number of entries
 * @param key_len            fixed key length for this tree type
 *
 * The record stores all entries to enable full reconstruction on replay.
 */
uint64_t wal_emit_convert(uint64_t old_hash_leaf_id,
                          const uint64_t* new_leaf_ids,
                          uint32_t num_new_leaves,
                          uint32_t num_entries,
                          uint32_t key_len);

/*
 * STABILIZE: after a hash bucket is stabilized (lazy migration).
 */
uint64_t wal_emit_stabilize(uint64_t leaf_id,
                            uint32_t bucket_idx);

}
} 

#endif 