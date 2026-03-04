#ifndef BLINK_HASH_WAL_RING_H__
#define BLINK_HASH_WAL_RING_H__


#include <atomic>
#include <cstdint>
#include <cstddef>

namespace BLINK_HASH {
namespace WAL {

constexpr size_t RING_DEFAULT_CAPACITY = 64ULL * 1024 * 1024;  /* 64 MB */

class RingBuffer {
public:
  
     /* capacity must be a power of two.*/

    explicit RingBuffer(size_t capacity = RING_DEFAULT_CAPACITY);
    ~RingBuffer();

    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;

    /*  Producer API */

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

    /* Consumer API  */

    /*
     * Return a pointer and length of the next contiguous committed
     * block that is ready to be flushed.  Returns 0 if nothing is
     * available.  After the flusher has written the data to disk,
     * call advance() to release the space.
     */
    size_t peek(const void** out_ptr) const;
    void advance(size_t size);
        /*
     * Called by ThreadBuf::flush() after reserve() and before commit().
     */
    void write_at(uint64_t abs_offset, const void* data, size_t len);
    /*  Diagnostics */

    uint64_t write_head()   const { return write_head_.load(std::memory_order_acquire); }
    uint64_t commit_head()  const { return commit_head_.load(std::memory_order_acquire); }
    uint64_t read_head()    const { return read_head_.load(std::memory_order_acquire); }
    size_t   capacity()     const { return capacity_; }
    size_t   used()         const { return write_head() - read_head(); }

private:
    char*    buf_;         
    size_t   capacity_;     
    size_t   mask_;         

    alignas(64) std::atomic<uint64_t> write_head_{0};
    alignas(64) std::atomic<uint64_t> commit_head_{0};
    alignas(64) std::atomic<uint64_t> read_head_{0};

    size_t   num_commit_blocks_;
    std::atomic<uint8_t>*  committed_;  
};

} 
} 

#endif 
