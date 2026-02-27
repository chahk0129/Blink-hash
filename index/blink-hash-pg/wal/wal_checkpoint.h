#ifndef BLINK_HASH_WAL_CHECKPOINT_H__
#define BLINK_HASH_WAL_CHECKPOINT_H__

/*
 * wal_checkpoint.h — Fuzzy checkpointing with copy-on-write
 *
 * The Checkpointer runs in a dedicated background thread.
 * When triggered it:
 *   1. Flips the global checkpoint epoch (atomic bool).
 *   2. Emits WAL_CHECKPOINT_BEGIN with the current LSN.
 *   3. Walks the tree (DFS), reading stable (CoW-protected) nodes.
 *   4. Writes a binary snapshot file (O_DIRECT).
 *   5. Emits WAL_CHECKPOINT_END with the end LSN.
 *   6. Writes a checkpoint manifest JSON.
 *   7. Flips the epoch back; workers free CoW copies.
 *   8. Deletes WAL segments older than the checkpoint LSN.
 *
 * Workers detect the epoch and, when modifying a node during an
 * active checkpoint, perform a copy-on-write: allocate a new node,
 * leave the old node for the snapshot reader.
 *
 * Implements: Phase 5 of IMPLEMENTATION_SEQUENCE.md
 */

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

namespace BLINK_HASH {

/* Forward declarations */
template <typename K, typename V> class btree_t;
struct node_t;
struct ThreadInfo;

namespace WAL {

class Flusher;    /* forward — needed for LSN queries */

/* ─── Checkpoint manifest (serialized to JSON on disk) ────────────── */

struct CheckpointManifest {
    uint64_t    checkpoint_lsn;     /* WAL replay starts here       */
    uint64_t    end_lsn;            /* LSN when snapshot finished    */
    std::string snapshot_file;      /* e.g. "snap_000005.dat"        */
    uint64_t    num_entries;        /* total K/V pairs in snapshot   */
};

/*
 * Write / read the checkpoint manifest to/from disk.
 * The manifest is a small JSON file: `checkpoint_manifest.json`.
 */
bool write_manifest(const std::string& wal_dir,
                    const CheckpointManifest& m);

bool read_manifest(const std::string& wal_dir,
                   CheckpointManifest* m_out);

/* ─── Copy-on-Write epoch ────────────────────────────────────────── */

/*
 * Global checkpoint epoch flag.  Workers check this with
 * `checkpoint_epoch_active()` before modifying a node.
 */
bool    checkpoint_epoch_active();
void    checkpoint_epoch_set(bool active);

/*
 * CoW helper: if a checkpoint is active, allocate a new copy of
 * `node`, install it in the parent, and return the new pointer.
 * If no checkpoint is active, return `node` unchanged.
 *
 * The old node is added to a pending-free list and released when
 * the epoch flips back.
 */
node_t* cow_if_needed(node_t* node);

/* ─── Checkpointer ───────────────────────────────────────────────── */

class Checkpointer {
public:
    Checkpointer(const std::string& wal_dir,
                 Flusher& flusher);
    ~Checkpointer();

    /* Non-copyable */
    Checkpointer(const Checkpointer&) = delete;
    Checkpointer& operator=(const Checkpointer&) = delete;

    /*
     * Trigger a checkpoint for the given tree.
     * This is asynchronous: it signals the background thread and
     * returns immediately.  Use `wait()` to block until done.
     *
     * Template parameters must match the tree's Key/Value types.
     */
    template <typename Key_t, typename Value_t>
    void trigger(btree_t<Key_t, Value_t>& tree,
                 ThreadInfo& threadInfo);

    /*
     * Block until the current checkpoint completes.
     */
    void wait();

    /*
     * True while a checkpoint is in progress.
     */
    bool active() const { return active_.load(std::memory_order_acquire); }

    /*
     * Start / stop the background checkpointer thread.
     */
    void start();
    void stop();

private:
    /* ── snapshot writer ── */
    template <typename Key_t, typename Value_t>
    uint64_t write_snapshot(btree_t<Key_t, Value_t>& tree,
                            ThreadInfo& threadInfo,
                            const std::string& snap_path);

    /* ── WAL segment cleanup ── */
    void delete_old_segments(uint64_t before_lsn);

    /* ── state ── */
    std::string   wal_dir_;
    Flusher&      flusher_;
    uint64_t      snapshot_counter_{0};

    std::thread           thread_;
    std::atomic<bool>     running_{false};
    std::atomic<bool>     active_{false};

    /* CoW pending-free list */
    std::vector<node_t*>  cow_pending_;
};

} // namespace WAL
} // namespace BLINK_HASH

#endif // BLINK_HASH_WAL_CHECKPOINT_H__
