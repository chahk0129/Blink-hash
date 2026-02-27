#ifndef BLINK_HASH_WAL_RING_H__
#define BLINK_HASH_WAL_RING_H__

/*
 * wal_ring.h — Lock-free MPSC ring buffer for WAL aggregation
 *
 * Multiple worker threads (producers) reserve space with a single
 * atomic fetch_add on `write_head_`.  A single flusher thread
 * (consumer) reads committed data and pushes it to disk.
 *
 * Memory is allocated via mmap (hugepages on Linux where available,
 * regular anonymous pages elsewhere).
 *
 * Implements: Phase 1.3 of IMPLEMENTATION_SEQUENCE.md
 */

#include <atomic>
#include <cstdint>
#include <cstddef>

namespace BLINK_HASH {
namespace WAL {

constexpr size_t RING_DEFAULT_CAPACITY = 64ULL * 1024 * 1024;  /* 64 MB */

class RingBuffer {
public:
    /*
     * Allocate the ring buffer.
     * `capacity` must be a power of two.
     */
    explicit RingBuffer(size_t capacity = RING_DEFAULT_CAPACITY);
    ~RingBuffer();

    /* Non-copyable */
    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;

    /* ── Producer API (called by worker threads) ─────────────────── */

    /*
     * Reserve `size` contiguous bytes in the ring.
     * Returns the byte offset into the ring.
     * The caller must then memcpy data and call commit().
     */
    uint64_t reserve(size_t size);

    /*
     * Mark a previously reserved region as fully written (readable).
     * `offset` and `size` must match a prior reserve() call.
     */
    void commit(uint64_t offset, size_t size);

    /* ── Consumer API (called by the flusher thread) ─────────────── */

    /*
     * Return a pointer and length of the next contiguous committed
     * block that is ready to be flushed.  Returns 0 if nothing is
     * available.  After the flusher has written the data to disk,
     * call advance() to release the space.
     */
    size_t peek(const void** out_ptr) const;

    /*
     * Advance the read head by `size` bytes, freeing the space.
     */
    void advance(size_t size);

    /* ── Diagnostics ─────────────────────────────────────────────── */

    uint64_t write_head()   const { return write_head_.load(std::memory_order_acquire); }
    uint64_t commit_head()  const { return commit_head_.load(std::memory_order_acquire); }
    uint64_t read_head()    const { return read_head_.load(std::memory_order_acquire); }
    size_t   capacity()     const { return capacity_; }
    size_t   used()         const { return write_head() - read_head(); }

private:
    char*    buf_;          /* ring memory, `capacity_` bytes           */
    size_t   capacity_;     /* always power-of-two                     */
    size_t   mask_;         /* capacity_ - 1 for cheap modulo          */

    alignas(64) std::atomic<uint64_t> write_head_{0};
    alignas(64) std::atomic<uint64_t> commit_head_{0};
    alignas(64) std::atomic<uint64_t> read_head_{0};

    /*
     * Per-slot committed flag array, one byte per 4-KB block,
     * used by commit() to signal that a reserved region is readable.
     */
    std::atomic<uint8_t>*  committed_;   /* capacity_ / 4096 entries */
};

} // namespace WAL
} // namespace BLINK_HASH

#endif // BLINK_HASH_WAL_RING_H__
