#ifndef BLINK_HASH_WAL_CHECKPOINT_H__
#define BLINK_HASH_WAL_CHECKPOINT_H__

/*
 * The Checkpointer runs in a dedicated background thread.
 * When triggered it:
 *   1. Flips the global checkpoint epoch (atomic bool).
 *   2. Emits WAL_CHECKPOINT_BEGIN with the current LSN.
 *   3. Walks the tree (level-order), reading stable (CoW-protected) nodes.
 *   4. Writes a binary snapshot file (O_DIRECT where possible).
 *   5. Emits WAL_CHECKPOINT_END with the end LSN.
 *   6. Writes a checkpoint manifest JSON.
 *   7. Flips the epoch back; workers free CoW copies.
 *   8. Deletes WAL segments older than the checkpoint LSN.
 *
 * Workers detect the epoch and, when modifying a node during an
 * active checkpoint, perform a copy-on-write: allocate a new node,
 * leave the old node for the snapshot reader.
 */

#include <atomic>
#include <cstdint>
#include <mutex>
#include <condition_variable>
#include <string>
#include <thread>
#include <vector>
#include <functional>

namespace BLINK_HASH {


template <typename K, typename V> class btree_t;
class node_t;
struct ThreadInfo;

namespace WAL {

class Flusher;    



struct CheckpointManifest {
    uint64_t    checkpoint_lsn;     /* WAL replay starts here       */
    uint64_t    end_lsn;            /* LSN when snapshot completed   */
    std::string snapshot_file;      /* "snap_000005.dat"        */
    uint64_t    num_entries;        /* total K/V pairs in snapshot   */
};


bool write_manifest(const std::string& wal_dir,
                    const CheckpointManifest& m);

bool read_manifest(const std::string& wal_dir,
                   CheckpointManifest* m_out);



/*
 * Binary snapshot file format:
 *
 * Header (32 bytes):
 *   uint64_t  magic;           // 0x424C494E4B534E50 ("BLINKSNP")
 *   uint64_t  version;         // 1
 *   uint64_t  num_entries;
 *   uint32_t  key_size;        // sizeof(Key_t) — fixed for this tree
 *   uint32_t  value_size;      // sizeof(Value_t)
 *
 * Entries (repeated num_entries times):
 *   uint8_t   key_bytes[key_size];
 *   uint8_t   value_bytes[value_size];
 *
 * Footer (16 bytes):
 *   uint64_t  checksum;        // CRC-64 of all entry data
 *   uint64_t  num_entries;     // redundant — for validation
 */

constexpr uint64_t SNAP_MAGIC   = 0x424C494E4B534E50ULL; 
constexpr uint64_t SNAP_VERSION = 1;

struct SnapHeader {
    uint64_t magic;
    uint64_t version;
    uint64_t num_entries;
    uint32_t key_size;
    uint32_t value_size;
};
static_assert(sizeof(SnapHeader) == 32, "snapshot header must be 32 bytes");

struct SnapFooter {
    uint64_t checksum;
    uint64_t num_entries;
};
static_assert(sizeof(SnapFooter) == 16, "snapshot footer must be 16 bytes");


/*
 * Global checkpoint epoch flag.  Workers check this with
 * `checkpoint_epoch_active()` before modifying a node.
 *
 * When true: any node about to be MODIFIED should be CoW'd.
 * When false: normal operation, no CoW overhead.
 */
extern std::atomic<bool> g_checkpoint_active;

inline bool checkpoint_epoch_active() {
    return g_checkpoint_active.load(std::memory_order_acquire);
}

inline void checkpoint_epoch_set(bool active) {
    g_checkpoint_active.store(active, std::memory_order_release);
}


extern std::mutex              g_cow_mutex;
extern std::vector<void*>      g_cow_pending;


inline void cow_register_old(void* old_node) {
    std::lock_guard<std::mutex> lk(g_cow_mutex);
    g_cow_pending.push_back(old_node);
}


inline void cow_free_pending() {
    std::lock_guard<std::mutex> lk(g_cow_mutex);
    for (void* p : g_cow_pending)
        operator delete(p);
    g_cow_pending.clear();
}



class Checkpointer {
public:
    Checkpointer(const std::string& wal_dir,
                 Flusher& flusher);
    ~Checkpointer();

    
    Checkpointer(const Checkpointer&) = delete;
    Checkpointer& operator=(const Checkpointer&) = delete;

    template <typename Key_t, typename Value_t>
    CheckpointManifest run_checkpoint(btree_t<Key_t, Value_t>& tree,
                                      ThreadInfo& threadInfo);


    bool active() const { return active_.load(std::memory_order_acquire); }

private:
    /* ── snapshot writer ── */

    /*
     * Walk the tree level-by-level, serialize all leaf entries to
     * the snapshot file.  Returns the number of entries written.
     */
    template <typename Key_t, typename Value_t>
    uint64_t write_snapshot(btree_t<Key_t, Value_t>& tree,
                            const std::string& snap_path);


    template <typename Key_t, typename Value_t>
    uint64_t serialize_leaf(const void* leaf_node, int fd,
                            char* write_buf, size_t& buf_used,
                            size_t buf_cap);


    void delete_old_segments(uint64_t before_lsn);

   
    std::string   wal_dir_;
    Flusher&      flusher_;
    uint64_t      snapshot_counter_{0};

    std::atomic<bool>     active_{false};
};


template <typename Key_t, typename Value_t>
uint64_t load_snapshot(const std::string& snap_path,
                       btree_t<Key_t, Value_t>& tree,
                       ThreadInfo& threadInfo);

} 
} 

#endif