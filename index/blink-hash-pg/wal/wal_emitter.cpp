#include "wal_emitter.h"
#include <cassert>

namespace BLINK_HASH {
namespace WAL {
std::atomic<uint64_t> g_lsn{0};
std::atomic<uint64_t> g_node_id{0};
RingBuffer*  g_wal_ring    = nullptr;
bool         g_wal_enabled = false;

static thread_local ThreadBuf tl_thread_buf;

ThreadBuf& wal_get_thread_buf() {
    return tl_thread_buf;
}



uint64_t wal_emit(RecordType type,
                  const void* payload, size_t payload_len) {
    if (!g_wal_enabled || !g_wal_ring)
        return 0;

    uint64_t lsn = next_lsn();

    char buf[WAL_RECORD_BUF_SIZE];
    assert(sizeof(RecordHeader) + payload_len <= WAL_RECORD_BUF_SIZE);

    size_t rec_sz = wal_record_serialize(type, lsn,
                                         payload, payload_len, buf);

    auto& tb = wal_get_thread_buf();
    tb.append_and_maybe_flush(buf, rec_sz, *g_wal_ring);

    return lsn;
}

/* typed emitters */

uint64_t wal_emit_insert(uint64_t node_id, uint32_t bucket_idx,
                         const void* key, uint32_t key_len,
                         uint64_t value) {
    /*
     * Payload layout:
     *   InsertPayload { node_id, bucket_idx, key_len }
     *   key bytes [key_len]
     *   value     [8 bytes]
     */
    char payload[sizeof(InsertPayload) + 256 + 8];
    InsertPayload ip;
    ip.node_id    = node_id;
    ip.bucket_idx = bucket_idx;
    ip.key_len    = key_len;

    size_t off = 0;
    std::memcpy(payload + off, &ip, sizeof(ip));   off += sizeof(ip);
    std::memcpy(payload + off, key, key_len);      off += key_len;
    std::memcpy(payload + off, &value, 8);         off += 8;

    return wal_emit(RecordType::INSERT, payload, off);
}

uint64_t wal_emit_delete(uint64_t node_id,
                         const void* key, uint32_t key_len) {
    char payload[sizeof(DeletePayload) + 256];
    DeletePayload dp;
    dp.node_id = node_id;
    dp.key_len = key_len;
    dp._pad    = 0;

    size_t off = 0;
    std::memcpy(payload + off, &dp, sizeof(dp));   off += sizeof(dp);
    std::memcpy(payload + off, key, key_len);      off += key_len;

    return wal_emit(RecordType::DELETE, payload, off);
}

uint64_t wal_emit_update(uint64_t node_id,
                         const void* key, uint32_t key_len,
                         uint64_t new_value) {
    char payload[sizeof(UpdatePayload) + 256 + 8];
    UpdatePayload up;
    up.node_id = node_id;
    up.key_len = key_len;
    up._pad    = 0;

    size_t off = 0;
    std::memcpy(payload + off, &up, sizeof(up));     off += sizeof(up);
    std::memcpy(payload + off, key, key_len);        off += key_len;
    std::memcpy(payload + off, &new_value, 8);       off += 8;

    return wal_emit(RecordType::UPDATE, payload, off);
}

uint64_t wal_emit_split_leaf(uint64_t old_leaf_id,
                             uint64_t new_leaf_id,
                             const void* split_key,
                             uint32_t split_key_len) {
    char payload[sizeof(SplitLeafPayload) + 256];
    SplitLeafPayload sp;
    sp.old_leaf_id   = old_leaf_id;
    sp.new_leaf_id   = new_leaf_id;
    sp.split_key_len = split_key_len;
    sp.num_migrated  = 0;  /* logical WAL — we don't log migrated entries */

    size_t off = 0;
    std::memcpy(payload + off, &sp, sizeof(sp));          off += sizeof(sp);
    std::memcpy(payload + off, split_key, split_key_len); off += split_key_len;

    return wal_emit(RecordType::SPLIT_LEAF, payload, off);
}

uint64_t wal_emit_split_internal(uint64_t inode_id,
                                 uint64_t new_child_id,
                                 const void* split_key,
                                 uint32_t split_key_len) {
    char payload[sizeof(SplitInternalPayload) + 256];
    SplitInternalPayload sp;
    sp.inode_id       = inode_id;
    sp.new_child_id   = new_child_id;
    sp.split_key_len  = split_key_len;
    sp._pad           = 0;

    size_t off = 0;
    std::memcpy(payload + off, &sp, sizeof(sp));          off += sizeof(sp);
    std::memcpy(payload + off, split_key, split_key_len); off += split_key_len;

    return wal_emit(RecordType::SPLIT_INTERNAL, payload, off);
}

uint64_t wal_emit_new_root(uint64_t new_root_id,
                           uint64_t left_child_id,
                           uint64_t right_child_id,
                           const void* split_key,
                           uint32_t split_key_len,
                           uint8_t level) {
    char payload[sizeof(NewRootPayload) + 256];
    NewRootPayload nr;
    nr.new_root_id    = new_root_id;
    nr.left_child_id  = left_child_id;
    nr.right_child_id = right_child_id;
    nr.split_key_len  = split_key_len;
    nr.level          = level;
    std::memset(nr._pad, 0, sizeof(nr._pad));

    size_t off = 0;
    std::memcpy(payload + off, &nr, sizeof(nr));          off += sizeof(nr);
    std::memcpy(payload + off, split_key, split_key_len); off += split_key_len;

    return wal_emit(RecordType::NEW_ROOT, payload, off);
}

uint64_t wal_emit_convert(uint64_t old_hash_leaf_id,
                          const uint64_t* new_leaf_ids,
                          uint32_t num_new_leaves,
                          uint32_t num_entries,
                          uint32_t key_len) {
    /*
     * For logical WAL, the convert record only stores metadata.
     * The actual entries are already represented by preceding INSERT
     * records (or will be reconstructed by replaying inserts into
     * the tree, which triggers conversion naturally).
     *
     * We record the node IDs so recovery can track node identity.
     */
    size_t payload_sz = sizeof(ConvertPayload)
                      + num_new_leaves * sizeof(uint64_t);

    char payload[1024];
    assert(payload_sz <= sizeof(payload));

    ConvertPayload cp;
    cp.old_hash_leaf_id = old_hash_leaf_id;
    cp.num_new_leaves   = num_new_leaves;
    cp.total_entries    = num_entries;

    size_t off = 0;
    std::memcpy(payload + off, &cp, sizeof(cp));                     off += sizeof(cp);
    std::memcpy(payload + off, new_leaf_ids, num_new_leaves * 8);    off += num_new_leaves * 8;

    return wal_emit(RecordType::CONVERT, payload, off);
}

uint64_t wal_emit_stabilize(uint64_t leaf_id,
                            uint32_t bucket_idx) {
    StabilizePayload sp;
    sp.leaf_id       = leaf_id;
    sp.bucket_idx    = bucket_idx;
    sp.num_migrated  = 0;  /* logical — actual migration is deterministic */

    return wal_emit(RecordType::STABILIZE, &sp, sizeof(sp));
}

} 
} 