#ifndef BLINK_HASH_WAL_RECOVERY_H__
#define BLINK_HASH_WAL_RECOVERY_H__

/*

 * On startup, the recovery module:
 *   1. Reads the checkpoint manifest (if any) to find the snapshot
 *      file and the replay-start LSN.
 *   2. Loads the snapshot into memory (bulk insert).
 *   3. Sequentially replays all WAL records with LSN >= checkpoint_lsn.

 */

#include <cstdint>
#include <string>
#include <vector>
#include <functional>

namespace BLINK_HASH {

template <typename K, typename V> class btree_t;
struct ThreadInfo;

namespace WAL {

struct RecordHeader; 



struct RecoveryStats {
    uint64_t records_total;       /* total records scanned          */
    uint64_t records_replayed;   
    uint64_t records_skipped;     /* LSN < from_lsn or structural   */
    uint64_t inserts_replayed;
    uint64_t deletes_replayed;
    uint64_t updates_replayed;
    uint64_t max_lsn;             /* highest LSN seen               */
    uint64_t max_node_id;         /* highest node_id in payloads    */
    double   elapsed_sec;
};



/*
 * Find all WAL segment files in `wal_dir`, sorted by segment_id.
 * Returns paths like: ["/tmp/wal/wal_000000.seg", "/tmp/wal/wal_000001.seg"]
 */
std::vector<std::string> find_wal_segments(const std::string& wal_dir);


std::vector<char> read_all_segments(const std::string& wal_dir);



/*
 * Replay all WAL records from `wal_dir` into `tree`.
 *
 * @param wal_dir      Path to the WAL directory containing .seg files
 * @param tree         Empty tree to populate (or snapshot-loaded tree)
 * @param threadInfo   Thread epoch info for tree operations
 * @param from_lsn     Only replay records with LSN >= from_lsn (0 = all)
 *
 * On return:
 *   - `tree` is fully reconstructed
 *   - `stats_out` contains replay statistics
 *   - Global LSN and node_id counters are re-seeded
 */
template <typename Key_t, typename Value_t>
RecoveryStats recover(const std::string& wal_dir,
                      btree_t<Key_t, Value_t>& tree,
                      ThreadInfo& threadInfo,
                      uint64_t from_lsn = 0);

/* Replay a single record (exposed for testing)*/


template <typename Key_t, typename Value_t>
bool replay_one_record(const RecordHeader& hdr,
                       const void* payload,
                       size_t payload_len,
                       btree_t<Key_t, Value_t>& tree,
                       ThreadInfo& threadInfo,
                       RecoveryStats& stats);



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

}
} 

#endif 