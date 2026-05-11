#ifndef BLINK_HASH_WAL_GROUP_COMMIT_H__
#define BLINK_HASH_WAL_GROUP_COMMIT_H__


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


    void wait_for_lsn(uint64_t lsn);
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

} 
} 

#endif 
