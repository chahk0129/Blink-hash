/*
 * the flusher publishes a durable LSN ≥ their record's LSN.
 */

#include "wal_group_commit.h"
#include <algorithm>

namespace BLINK_HASH {
namespace WAL {

GroupCommit::GroupCommit()  = default;
GroupCommit::~GroupCommit() = default;



void GroupCommit::wait_for_lsn(uint64_t lsn) {
    /*
     * Register a Waiter on the stack and block.
     * The flusher thread calls notify_flushed() which scans
     * the vector and wakes all waiters whose LSN ≤ durable_lsn.
     */
    Waiter w;
    w.lsn  = lsn;
    w.done = false;

    {
        std::lock_guard<std::mutex> lk(mu_);
        waiters_.push_back(&w);
    }

    /* Block until the flusher wakes us */
    {
        std::unique_lock<std::mutex> lk(mu_);
        w.cv.wait(lk, [&w]() { return w.done; });
    }


}



void GroupCommit::notify_flushed(uint64_t durable_lsn) {
    std::lock_guard<std::mutex> lk(mu_);

    /*
     * Wake all waiters whose LSN ≤ durable_lsn.
     * We use an erase-remove pattern to clean up the vector.
     */
    auto it = std::remove_if(waiters_.begin(), waiters_.end(),
        [durable_lsn](Waiter* w) {
            if (w->lsn <= durable_lsn) {
                w->done = true;
                w->cv.notify_one();
                return true;  
            }
            return false;
        });
    waiters_.erase(it, waiters_.end());
}

}
} 