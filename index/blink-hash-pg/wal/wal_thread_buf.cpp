

#include "wal_thread_buf.h"
#include "wal_ring.h"
#include <cstring>
#include <cstdlib>      /* posix_memalign, free */
#include <cassert>

namespace BLINK_HASH {
namespace WAL {

static constexpr size_t COMMIT_BLOCK = 4096;

static char* alloc_aligned(size_t sz) {
    void* ptr = nullptr;
    int rc = posix_memalign(&ptr, 4096, sz);
    if (rc != 0) {
        /* posix_memalign failed — unrecoverable */
        std::abort();
    }
    return static_cast<char*>(ptr);
}



ThreadBuf::ThreadBuf()
    : buf_(alloc_aligned(THREAD_BUF_SIZE))
    , used_(0)
{}

ThreadBuf::~ThreadBuf() {
    std::free(buf_);
}

ThreadBuf::ThreadBuf(ThreadBuf&& o) noexcept
    : buf_(o.buf_), used_(o.used_)
{
    o.buf_  = nullptr;
    o.used_ = 0;
}

ThreadBuf& ThreadBuf::operator=(ThreadBuf&& o) noexcept {
    if (this != &o) {
        std::free(buf_);
        buf_   = o.buf_;
        used_  = o.used_;
        o.buf_  = nullptr;
        o.used_ = 0;
    }
    return *this;
}

/* append */

bool ThreadBuf::append(const void* data, size_t len) {
    /*
     * If the record is larger than the entire buffer capacity
     * there is no way to buffer it.  The caller should flush
     * the record directly to the ring.
     */
    if (len > THREAD_BUF_SIZE)
        return false;

    /*
     * If appending would exceed the buffer, return false.
     * The caller must flush() first and then retry.
     */
    if (used_ + len > THREAD_BUF_SIZE)
        return false;

    std::memcpy(buf_ + used_, data, len);
    used_ += len;
    return true;
}
// use this API when you want to append and auto-flush if we cross the high-water mark
bool ThreadBuf::append_and_maybe_flush(const void* data, size_t len, RingBuffer& ring) {
    /* If buffer can't hold this record, flush first */
    if (used_ + len > THREAD_BUF_SIZE) {
        flush(ring);
    }

    if (len > THREAD_BUF_SIZE)
        return false;

    std::memcpy(buf_ + used_, data, len);
    used_ += len;

    /* Auto-flush when we've crossed the 93% threshold.  This keeps us from buffering too much data while still allowing
     some headroom for records that are slightly larger than the flush
     threshold. */
    if (used_ >= THREAD_BUF_FLUSH_AT) {
        flush(ring);
    }

    return true;
}

/* flush / drain */

void ThreadBuf::flush(RingBuffer& ring) {
    if (used_ == 0) return;

    /* Round up to COMMIT_BLOCK boundary */
    size_t aligned = (used_ + COMMIT_BLOCK - 1) & ~(COMMIT_BLOCK - 1);

    /* Zero-pad the tail so committed data is clean */
    if (aligned > used_) {
        std::memset(buf_ + used_, 0, aligned - used_);
    }

    uint64_t off = ring.reserve(aligned);
    ring.write_at(off, buf_, aligned);
    ring.commit(off, aligned);

    used_ = 0;
}

void ThreadBuf::drain(RingBuffer& ring) {
    flush(ring);
}

} 
} 