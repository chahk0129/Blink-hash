
#include "wal_ring.h"

#include <cstring>
#include <cstdlib>
#include <cassert>
#include <new>

#ifdef __linux__
#include <sys/mman.h>
#elif defined(__APPLE__)
#include <sys/mman.h>
#endif
#include <thread>

namespace BLINK_HASH {
namespace WAL {


static bool is_power_of_two(size_t n) {
    return n != 0 && (n & (n - 1)) == 0;
}

/* Block size for commit tracking. */
static constexpr size_t COMMIT_BLOCK = 4096;

/* constructor / destructor */

RingBuffer::RingBuffer(size_t capacity)
    : buf_(nullptr)
    , capacity_(capacity)
    , mask_(capacity - 1)
    , committed_(nullptr)
{
    assert(is_power_of_two(capacity) && "capacity must be power-of-two");
    assert(capacity >= 2 * COMMIT_BLOCK && "need at least 2 blocks");

    void* p = ::mmap(nullptr, capacity, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (p == MAP_FAILED)
        std::abort();
    buf_ = static_cast<char*>(p);

    num_commit_blocks_ = capacity / COMMIT_BLOCK;
    committed_ = new std::atomic<uint8_t>[num_commit_blocks_];
    for (size_t i = 0; i < num_commit_blocks_; ++i)
        committed_[i].store(0, std::memory_order_relaxed);
}
RingBuffer::~RingBuffer() {
    if (buf_)
        ::munmap(buf_, capacity_);
    delete[] committed_;
}

/* Producer: reserve*/

uint64_t RingBuffer::reserve(size_t size) {
    uint64_t cur;
    for (;;) {
        cur = write_head_.load(std::memory_order_relaxed);
        uint64_t rh = read_head_.load(std::memory_order_acquire);

        /* Check if there's enough space in the ring */
        if (cur + size - rh > capacity_) {
            /* Ring is full — back off and let consumer drain */
            std::this_thread::yield();
            continue;
        }

        /* Try to atomically claim [cur, cur+size) */
        if (write_head_.compare_exchange_weak(
                cur, cur + size,
                std::memory_order_acq_rel,
                std::memory_order_relaxed)) {
            return cur;
        }
        /* CAS failed — another producer won. retry */
    }
}

/* Producer: commit  */

void RingBuffer::commit(uint64_t offset, size_t size) {
    /* Step 1: Mark all blocks in [offset, offset+size) as committed */
    uint64_t first_block = offset / COMMIT_BLOCK;
    uint64_t last_block  = (offset + size - 1) / COMMIT_BLOCK;

    for (uint64_t b = first_block; b <= last_block; ++b) {
        size_t idx = b % num_commit_blocks_;
        committed_[idx].store(1, std::memory_order_release);
    }

    /* Step 2: Try to advance commit_head_ past consecutive committed blocks */
    for (;;) {
        uint64_t ch = commit_head_.load(std::memory_order_acquire);

        /* Don't advance past write_head_ */
        uint64_t wh = write_head_.load(std::memory_order_acquire);
        if (ch >= wh)
            break;

        size_t idx = (ch / COMMIT_BLOCK) % num_commit_blocks_;
        if (committed_[idx].load(std::memory_order_acquire) == 0)
            break;      /* This block isn't committed yet. stop */

        uint64_t next = ch + COMMIT_BLOCK;
        if (commit_head_.compare_exchange_weak(
                ch, next,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            /* We advanced — clear the flag for reuse when ring wraps */
            committed_[idx].store(0, std::memory_order_release);
            
        }
        /* If CAS failed, another thread advanced it loop and retry */
    }
}


/* Consumer: peek */

size_t RingBuffer::peek(const void** out_ptr) const {
    uint64_t rh = read_head_.load(std::memory_order_acquire);
    uint64_t ch = commit_head_.load(std::memory_order_acquire);

    if (rh >= ch) {
        *out_ptr = nullptr;
        return 0;
    }

    size_t phys_off = static_cast<size_t>(rh & mask_);
    size_t avail    = static_cast<size_t>(ch - rh);

    /* Don't return data across the physical wrap boundary */
    size_t contig = capacity_ - phys_off;
    if (avail > contig)
        avail = contig;

    *out_ptr = buf_ + phys_off;
    return avail;
}

/*  Consumer: advance  */

void RingBuffer::advance(size_t size) {
    read_head_.fetch_add(size, std::memory_order_release);
}

/* write_at (for ThreadBuf flush)  */


void RingBuffer::write_at(uint64_t abs_offset, const void* data, size_t len) {
    size_t phys = static_cast<size_t>(abs_offset & mask_);
    size_t first_part = capacity_ - phys;

    if (len <= first_part) {
        std::memcpy(buf_ + phys, data, len);
    } else {
        std::memcpy(buf_ + phys, data, first_part);
        std::memcpy(buf_, static_cast<const char*>(data) + first_part,
                    len - first_part);
    }
}

} 
} 