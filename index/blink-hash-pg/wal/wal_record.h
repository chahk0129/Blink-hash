#ifndef BLINK_HASH_WAL_RECORD_H__
#define BLINK_HASH_WAL_RECORD_H__

/*
 * wal_record.h — WAL record format for B^link-hash
 *
 * Every mutation in the tree produces one or more WAL records.
 * Records are appended to thread-local buffers, drained through
 * the MPSC ring buffer, and flushed to disk by the I/O engine.
 *
 * Implements: Phase 1.1 of IMPLEMENTATION_SEQUENCE.md
 */

#include <cstdint>
#include <cstring>
#include <atomic>

namespace BLINK_HASH {
namespace WAL {

/* ─── record types ─────────────────────────────────────────────────── */

enum class RecordType : uint16_t {
    INVALID            = 0,
    INSERT             = 1,
    DELETE             = 2,
    UPDATE             = 3,
    SPLIT_LEAF         = 4,
    SPLIT_INTERNAL     = 5,
    CONVERT            = 6,      /* hash-leaf → btree-leaves */
    NEW_ROOT           = 7,
    STABILIZE          = 8,
    CHECKPOINT_BEGIN   = 10,
    CHECKPOINT_END     = 11,
};

/* ─── header — prefixes every record on disk ───────────────────────── */

struct RecordHeader {
    uint64_t  lsn;           /* log sequence number (byte offset) */
    uint32_t  total_size;    /* header + payload, in bytes         */
    uint16_t  type;          /* RecordType cast to uint16_t        */
    uint16_t  crc16;         /* CRC-16/ARC of the payload bytes    */
};
static_assert(sizeof(RecordHeader) == 16, "WAL header must be 16 bytes");

/* ─── per-type payloads ───────────────────────────────────────────── */

struct InsertPayload {
    uint64_t  node_id;
    uint32_t  bucket_idx;
    uint32_t  key_len;       /* actual key bytes (≤ key capacity)  */
    /* followed by: key_len bytes of key data                       */
    /* followed by: 8 bytes of value (uint64_t)                     */
};

struct DeletePayload {
    uint64_t  node_id;
    uint32_t  key_len;
    uint32_t  _pad;
    /* followed by: key_len bytes of key data                       */
};

struct UpdatePayload {
    uint64_t  node_id;
    uint32_t  key_len;
    uint32_t  _pad;
    /* followed by: key_len bytes of key, 8 bytes of new value      */
};

struct SplitLeafPayload {
    uint64_t  old_leaf_id;
    uint64_t  new_leaf_id;
    uint32_t  split_key_len;
    uint32_t  num_migrated;
    /* followed by: split_key, then num_migrated×(key+value) pairs  */
};

struct SplitInternalPayload {
    uint64_t  inode_id;
    uint64_t  new_child_id;
    uint32_t  split_key_len;
    uint32_t  _pad;
    /* followed by: split_key_len bytes of key                      */
};

struct NewRootPayload {
    uint64_t  new_root_id;
    uint64_t  left_child_id;
    uint64_t  right_child_id;
    uint32_t  split_key_len;
    uint8_t   level;
    uint8_t   _pad[3];
    /* followed by: split_key_len bytes of key                      */
};

struct ConvertPayload {
    uint64_t  old_hash_leaf_id;
    uint32_t  num_new_leaves;
    uint32_t  total_entries;
    /* followed by: num_new_leaves × new_leaf_id (uint64_t each)
     * followed by: (num_new_leaves - 1) × split_key
     * followed by: total_entries × (key + value) pairs             */
};

struct StabilizePayload {
    uint64_t  leaf_id;
    uint32_t  bucket_idx;
    uint32_t  num_migrated;
    /* followed by: num_migrated × (key + value) pairs              */
};

struct CheckpointBeginPayload {
    uint64_t  checkpoint_lsn;
    uint64_t  epoch;
};

struct CheckpointEndPayload {
    uint64_t  checkpoint_lsn;
    uint64_t  end_lsn;
};

/* ─── serialization / deserialization ─────────────────────────────── */

/*
 * Compute CRC-16/ARC over a byte buffer.
 */
uint16_t wal_crc16(const void* data, size_t len);

/*
 * Serialize a WAL record (header + payload) into `dst`.
 * Returns total bytes written.  `dst` must have at least
 * `sizeof(RecordHeader) + payload_size` bytes available.
 */
size_t wal_record_serialize(RecordType type,
                            uint64_t lsn,
                            const void* payload,
                            size_t payload_size,
                            void* dst);

/*
 * Deserialize a WAL record from `src`.
 * On success: fills `hdr_out` and returns a pointer to the payload.
 * Returns nullptr on CRC mismatch or truncation.
 */
const void* wal_record_deserialize(const void* src,
                                   size_t available,
                                   RecordHeader* hdr_out);

} // namespace WAL
} // namespace BLINK_HASH

#endif // BLINK_HASH_WAL_RECORD_H__
