#ifndef BLINK_HASH_WAL_RECOVERY_H__
#define BLINK_HASH_WAL_RECOVERY_H__

/*
 * wal_recovery.h — Crash recovery: replay WAL records to rebuild the tree
 *
 * On startup, the recovery module:
 *   1. Reads the checkpoint manifest (if any) to find the snapshot
 *      file and the replay-start LSN.
 *   2. Loads the snapshot into memory (bulk insert).
 *   3. Sequentially replays all WAL records with LSN ≥ checkpoint_lsn.
 *
 * Implements: Phase 4 of IMPLEMENTATION_SEQUENCE.md
 */

#include <cstdint>
#include <string>

namespace BLINK_HASH {

/* Forward declarations */
template <typename K, typename V> class btree_t;
struct ThreadInfo;

namespace WAL {

struct RecordHeader;   /* from wal_record.h */

/* ── Recovery result ─────────────────────────────────────────────── */

struct RecoveryStats {
    uint64_t records_replayed;
    uint64_t records_skipped;   /* LSN < from_lsn */
    uint64_t last_lsn;
    double   elapsed_sec;
};

/* ── Main recovery entry point ───────────────────────────────────── */

/*
 * Open the WAL directory, locate the latest checkpoint manifest,
 * load the snapshot (if any), and replay all WAL records from
 * `checkpoint_lsn` forward.
 *
 * On return, `tree` is fully reconstructed and ready for use.
 * Returns the LSN of the last replayed record.
 *
 * Template parameters match btree_t<Key_t, Value_t>.
 */
template <typename Key_t, typename Value_t>
RecoveryStats recover(const std::string& wal_dir,
                      btree_t<Key_t, Value_t>& tree,
                      ThreadInfo& threadInfo);

/* ── Individual redo handlers (one per RecordType) ───────────────── */

template <typename Key_t, typename Value_t>
void redo_insert(const void* payload, size_t len,
                 btree_t<Key_t, Value_t>& tree,
                 ThreadInfo& threadInfo);

template <typename Key_t, typename Value_t>
void redo_delete(const void* payload, size_t len,
                 btree_t<Key_t, Value_t>& tree,
                 ThreadInfo& threadInfo);

template <typename Key_t, typename Value_t>
void redo_update(const void* payload, size_t len,
                 btree_t<Key_t, Value_t>& tree,
                 ThreadInfo& threadInfo);

/* Structural redo — for checkpoint-based recovery where the tree
 * is loaded from a snapshot and only the tail of the WAL is replayed.
 * These handle split/convert records that occurred after the snapshot. */

template <typename Key_t, typename Value_t>
void redo_split_leaf(const void* payload, size_t len,
                     btree_t<Key_t, Value_t>& tree,
                     ThreadInfo& threadInfo);

template <typename Key_t, typename Value_t>
void redo_convert(const void* payload, size_t len,
                  btree_t<Key_t, Value_t>& tree,
                  ThreadInfo& threadInfo);

/* ── WAL segment scanning ────────────────────────────────────────── */

/*
 * Scan WAL segment files in `wal_dir` and return the (min_lsn, max_lsn)
 * across all segments.
 */
void scan_wal_segments(const std::string& wal_dir,
                       uint64_t& min_lsn_out,
                       uint64_t& max_lsn_out);

} // namespace WAL
} // namespace BLINK_HASH

#endif // BLINK_HASH_WAL_RECOVERY_H__
