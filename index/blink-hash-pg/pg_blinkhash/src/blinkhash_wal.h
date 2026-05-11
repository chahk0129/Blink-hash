/*
 * blinkhash_wal.h — PostgreSQL WAL integration for B^link-hash
 *
 * Registers a Custom WAL Resource Manager (PG 15+) and defines
 * XLog record types that map to the standalone WAL record types
 * in wal/wal_record.h.
 *
 * For PG < 15 or standalone operation, the engine uses its own
 * WAL implementation (wal/ directory).  This file bridges the
 * two worlds.
 *
 * Implements: Phase 8 of IMPLEMENTATION_SEQUENCE.md
 */
#ifndef BLINKHASH_WAL_H
#define BLINKHASH_WAL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlog_internal.h"

/* ─── XLog record info codes ─────────────────────────────────────── */

#define XLOG_BLINKHASH_INSERT           0x00
#define XLOG_BLINKHASH_DELETE           0x10
#define XLOG_BLINKHASH_UPDATE           0x20
#define XLOG_BLINKHASH_SPLIT_LEAF       0x30
#define XLOG_BLINKHASH_SPLIT_INTERNAL   0x40
#define XLOG_BLINKHASH_CONVERT          0x50
#define XLOG_BLINKHASH_NEW_ROOT         0x60
#define XLOG_BLINKHASH_STABILIZE        0x70

/* ─── WAL record payload structs (PG-side, mirroring wal_record.h) ── */

typedef struct xl_blinkhash_insert {
    uint64      node_id;
    uint32      bucket_idx;
    uint16      key_len;
    /* followed by: key data + 8-byte value */
} xl_blinkhash_insert;

typedef struct xl_blinkhash_delete {
    uint64      node_id;
    uint16      key_len;
    /* followed by: key data */
} xl_blinkhash_delete;

typedef struct xl_blinkhash_update {
    uint64      node_id;
    uint16      key_len;
    /* followed by: key data + 8-byte new value */
} xl_blinkhash_update;

typedef struct xl_blinkhash_split_leaf {
    uint64      old_leaf_id;
    uint64      new_leaf_id;
    uint16      split_key_len;
    uint32      num_migrated;
    /* followed by: split key, then key/value pairs */
} xl_blinkhash_split_leaf;

typedef struct xl_blinkhash_split_internal {
    uint64      inode_id;
    uint64      new_child_id;
    uint16      split_key_len;
    /* followed by: split key */
} xl_blinkhash_split_internal;

typedef struct xl_blinkhash_convert {
    uint64      old_hash_leaf_id;
    uint32      num_new_leaves;
    uint32      total_entries;
    /* followed by: new_leaf_ids, split keys, then entries */
} xl_blinkhash_convert;

typedef struct xl_blinkhash_new_root {
    uint64      new_root_id;
    uint64      left_child_id;
    uint64      right_child_id;
    uint16      split_key_len;
    uint8       level;
    /* followed by: split key */
} xl_blinkhash_new_root;

typedef struct xl_blinkhash_stabilize {
    uint64      leaf_id;
    uint32      bucket_idx;
    uint32      num_migrated;
    /* followed by: migrated key/value pairs */
} xl_blinkhash_stabilize;

/* ─── Registration ───────────────────────────────────────────────── */

/*
 * Register the B^link-hash custom WAL resource manager.
 * Called from _PG_init().
 *
 * PG 15+ only — uses RegisterCustomWALResourceManager().
 */
void blinkhash_wal_init(void);

/* ─── XLog helpers ───────────────────────────────────────────────── */

/*
 * Emit a WAL record for an INSERT operation.
 * Called from blinkhash_aminsert() after the in-memory insert succeeds.
 *
 * Returns the LSN of the emitted record.
 */
XLogRecPtr blinkhash_xlog_insert(uint64 node_id,
                                 uint32 bucket_idx,
                                 const void *key_data,
                                 uint16 key_len,
                                 uint64 value);

/*
 * Emit a WAL record for a DELETE operation.
 */
XLogRecPtr blinkhash_xlog_delete(uint64 node_id,
                                 const void *key_data,
                                 uint16 key_len);

/*
 * Emit a WAL record for a SPLIT_LEAF operation.
 */
XLogRecPtr blinkhash_xlog_split_leaf(uint64 old_leaf_id,
                                     uint64 new_leaf_id,
                                     const void *split_key,
                                     uint16 split_key_len,
                                     uint32 num_migrated,
                                     const void *entries,
                                     size_t entries_len);

/*
 * Emit a WAL record for an UPDATE operation.
 */
XLogRecPtr blinkhash_xlog_update(uint64 node_id,
                                 const void *key_data,
                                 uint16 key_len,
                                 uint64 new_value);

/*
 * Emit a WAL record for a SPLIT_INTERNAL operation.
 */
XLogRecPtr blinkhash_xlog_split_internal(uint64 inode_id,
                                         uint64 new_child_id,
                                         const void *split_key,
                                         uint16 split_key_len);

/*
 * Emit a WAL record for a CONVERT operation (hash leaf → btree leaves).
 */
XLogRecPtr blinkhash_xlog_convert(uint64 old_hash_leaf_id,
                                  uint32 num_new_leaves,
                                  uint32 total_entries,
                                  const void *payload,
                                  size_t payload_len);

/*
 * Emit a WAL record for a NEW_ROOT operation.
 */
XLogRecPtr blinkhash_xlog_new_root(uint64 new_root_id,
                                   uint64 left_child_id,
                                   uint64 right_child_id,
                                   const void *split_key,
                                   uint16 split_key_len,
                                   uint8 level);

/*
 * Emit a WAL record for a STABILIZE operation.
 */
XLogRecPtr blinkhash_xlog_stabilize(uint64 leaf_id,
                                    uint32 bucket_idx,
                                    uint32 num_migrated,
                                    const void *entries,
                                    size_t entries_len);

/* ─── Redo callbacks (called during crash recovery) ──────────────── */

/*
 * Master redo dispatcher — called by PG recovery for each B^link-hash
 * XLog record.
 */
void blinkhash_redo(XLogReaderState *record);

/*
 * Per-type redo handlers.
 */
void blinkhash_redo_insert(XLogReaderState *record);
void blinkhash_redo_delete(XLogReaderState *record);
void blinkhash_redo_split_leaf(XLogReaderState *record);
void blinkhash_redo_split_internal(XLogReaderState *record);
void blinkhash_redo_convert(XLogReaderState *record);
void blinkhash_redo_new_root(XLogReaderState *record);
void blinkhash_redo_stabilize(XLogReaderState *record);

#ifdef __cplusplus
}
#endif

#endif /* BLINKHASH_WAL_H */
