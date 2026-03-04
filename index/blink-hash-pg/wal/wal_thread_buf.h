#ifndef BLINK_HASH_WAL_THREAD_BUF_H__
#define BLINK_HASH_WAL_THREAD_BUF_H__



#include <cstdint>
#include <cstddef>

namespace BLINK_HASH {
namespace WAL {


class RingBuffer;

/* configuration */
constexpr size_t THREAD_BUF_SIZE       = 64 * 1024;  /* 64 KB  */
constexpr size_t THREAD_BUF_FLUSH_AT   = 60 * 1024;  /* 48 KB */

class ThreadBuf {
public:
    ThreadBuf();
    ~ThreadBuf();

   
    ThreadBuf(const ThreadBuf&) = delete;
    ThreadBuf& operator=(const ThreadBuf&) = delete;
    ThreadBuf(ThreadBuf&&) noexcept;
    ThreadBuf& operator=(ThreadBuf&&) noexcept;

    bool append(const void* data, size_t len);

    bool append_and_maybe_flush(const void *data, size_t len, RingBuffer &ring);

    void flush(RingBuffer& ring);
    size_t buffered() const { return used_; }
    void drain(RingBuffer& ring);

private:
    char*   buf_;    
    size_t  used_;                          
};

} 
}

#endif 
