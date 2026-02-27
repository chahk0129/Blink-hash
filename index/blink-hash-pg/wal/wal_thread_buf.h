#ifndef BLINK_HASH_WAL_THREAD_BUF_H__
#define BLINK_HASH_WAL_THREAD_BUF_H__

/*
 * wal_thread_buf.h — Per-thread WAL staging buffer
 *
 * Each worker thread accumulates WAL records locally (zero contention).
 * When the buffer reaches the high-water mark it is drained into the
 * global MPSC ring buffer via a single atomic fetch_add.
 *
 * Implements: Phase 1.2 of IMPLEMENTATION_SEQUENCE.md
 */

#include <cstdint>
#include <cstddef>

namespace BLINK_HASH {
namespace WAL {

/* Forward declaration */
class RingBuffer;

/* ── configuration ────────────────────────────────────────────────── */
constexpr size_t THREAD_BUF_SIZE       = 64 * 1024;  /* 64 KB  */
constexpr size_t THREAD_BUF_FLUSH_AT   = 48 * 1024;  /* 48 KB high-water mark */

class ThreadBuf {
public:
    ThreadBuf();
    ~ThreadBuf();

    /* Non-copyable, movable */
    ThreadBuf(const ThreadBuf&) = delete;
    ThreadBuf& operator=(const ThreadBuf&) = delete;
    ThreadBuf(ThreadBuf&&) noexcept;
    ThreadBuf& operator=(ThreadBuf&&) noexcept;

    /*
     * Append a serialized WAL record to the thread-local buffer.
     * `data` must already contain the RecordHeader + payload.
     * Returns true if the record fit; false if the caller must
     * flush first.
     */
    bool append(const void* data, size_t len);

    /*
     * Drain the thread-local buffer into the global ring buffer.
     * Performs one atomic fetch_add to reserve space, then memcpy.
     * Automatically called by append() when the high-water mark
     * is reached, but may also be called explicitly.
     */
    void flush(RingBuffer& ring);

    /*
     * Number of bytes currently buffered (not yet flushed).
     */
    size_t buffered() const { return used_; }

    /*
     * Force-drain remaining bytes (called at thread exit or commit).
     */
    void drain(RingBuffer& ring);

private:
    char*   buf_;     /* 4-KB aligned buffer, THREAD_BUF_SIZE bytes */
    size_t  used_;    /* bytes used so far                          */
};

} // namespace WAL
} // namespace BLINK_HASH

#endif // BLINK_HASH_WAL_THREAD_BUF_H__
