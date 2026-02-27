#ifndef BLINK_HASH_WAL_GROUP_COMMIT_H__
#define BLINK_HASH_WAL_GROUP_COMMIT_H__

/*
 * wal_group_commit.h — Group commit queue for sync-commit workers
 *
 * When a worker needs durability (e.g., PG transaction commit),
 * it registers its LSN + condition variable here.  The flusher
 * wakes all waiters whose LSN ≤ flushed_lsn after each I/O batch.
 *
 * Implements: Phase 2.3 of IMPLEMENTATION_SEQUENCE.md
 */

#include <cstdint>
#include <mutex>
#include <condition_variable>
#include <vector>

namespace BLINK_HASH {
namespace WAL {

class GroupCommit {
public:
    GroupCommit();
    ~GroupCommit();

    /*
     * Block the calling thread until `lsn` is durable on disk.
     * Called by worker threads at transaction commit time.
     */
    void wait_for_lsn(uint64_t lsn);

    /*
     * Called by the flusher thread after each successful disk write.
     * Wakes all waiters whose registered LSN ≤ `durable_lsn`.
     */
    void notify_flushed(uint64_t durable_lsn);

private:
    struct Waiter {
        uint64_t                lsn;
        bool                    done;
        std::condition_variable cv;
    };

    std::mutex           mu_;
    std::vector<Waiter*> waiters_;
};

} // namespace WAL
} // namespace BLINK_HASH

#endif // BLINK_HASH_WAL_GROUP_COMMIT_H__
