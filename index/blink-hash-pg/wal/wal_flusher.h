#ifndef BLINK_HASH_WAL_FLUSHER_H__
#define BLINK_HASH_WAL_FLUSHER_H__

/*
 * wal_flusher.h — Dedicated flusher thread: ring buffer → disk
 *
 * Reads committed data from the ring buffer and writes it to
 * WAL segment files using O_DIRECT I/O.  On Linux uses io_uring
 * for async submission; on macOS falls back to pwrite + F_FULLFSYNC.
 *
 * Implements: Phase 2.1 of IMPLEMENTATION_SEQUENCE.md
 */

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <string>
#include <thread>

namespace BLINK_HASH {
namespace WAL {

class RingBuffer;    /* forward */

/* ── configuration ────────────────────────────────────────────────── */
constexpr size_t   FLUSH_BATCH_MAX       = 256 * 1024;  /* 256 KB */
constexpr size_t   WAL_SEGMENT_SIZE      = 64ULL * 1024 * 1024;  /* 64 MB */
constexpr size_t   WRITE_ALIGNMENT       = 4096;  /* NVMe / O_DIRECT */

class Flusher {
public:
    /*
     * `wal_dir` — directory where WAL segment files are stored.
     * `ring`    — reference to the global MPSC ring buffer.
     */
    Flusher(const std::string& wal_dir, RingBuffer& ring);
    ~Flusher();

    /* Non-copyable */
    Flusher(const Flusher&) = delete;
    Flusher& operator=(const Flusher&) = delete;

    /*
     * Start the flusher thread.  It will spin/yield reading from
     * the ring buffer and writing to WAL segment files.
     */
    void start();

    /*
     * Signal the flusher to stop after draining all committed data.
     * Blocks until the thread exits.
     */
    void stop();

    /*
     * The most recent LSN that has been durably written to disk.
     * Workers compare their record's LSN against this to decide
     * whether a commit is durable.
     */
    uint64_t flushed_lsn() const {
        return flushed_lsn_.load(std::memory_order_acquire);
    }

private:
    /* ── flusher main loop ── */
    void run();

    /* ── WAL segment file management ── */
    void open_segment(uint64_t segment_id);
    void rotate_segment();

    /* ── I/O backend ── */
    void write_batch(const void* data, size_t len);
    void sync_current_segment();

    /* io_uring state (Linux only) */
#ifdef __linux__
    void init_io_uring();
    void submit_uring_write(const void* data, size_t len, uint64_t offset);
    void wait_uring_completion();
    void* uring_;            /* struct io_uring* — type-erased      */
#endif

    /* ── state ── */
    std::string   wal_dir_;
    RingBuffer&   ring_;

    int           fd_;                   /* current segment fd       */
    uint64_t      segment_id_;           /* current segment number   */
    uint64_t      segment_offset_;       /* write offset in segment  */
    char*         write_buf_;            /* 4-KB aligned staging buf */

    std::thread           thread_;
    std::atomic<bool>     running_{false};
    alignas(64) std::atomic<uint64_t> flushed_lsn_{0};
};

} // namespace WAL
} // namespace BLINK_HASH

#endif // BLINK_HASH_WAL_FLUSHER_H__
