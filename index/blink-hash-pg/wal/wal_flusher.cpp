#include "wal_flusher.h"
#include "wal_ring.h"
#include "wal_record.h"

#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cassert>
#include <cerrno>
#include <algorithm>

/* platform I/O*/
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#ifdef __linux__
#include <liburing.h>           
#include <linux/fs.h>
#endif

#ifdef __APPLE__
#include <sys/types.h>
#endif

namespace BLINK_HASH {
namespace WAL {



static char* alloc_aligned(size_t sz) {
    void* p = nullptr;
    int rc = posix_memalign(&p, WRITE_ALIGNMENT, sz);
    if (rc != 0)
        std::abort();
    return static_cast<char*>(p);
}

static std::string segment_path(const std::string& dir, uint64_t seg_id) {
    char name[64];
    std::snprintf(name, sizeof(name), "wal_%06lu.seg", (unsigned long)seg_id);
    return dir + "/" + name;
}

/* ── constructor / destructor ──────────────────────────────────── */

Flusher::Flusher(const std::string& wal_dir, RingBuffer& ring)
    : wal_dir_(wal_dir)
    , ring_(ring)
    , fd_(-1)
    , segment_id_(0)
    , segment_offset_(0)
    , write_buf_(alloc_aligned(FLUSH_BATCH_MAX))
#ifdef __linux__
    , uring_(nullptr)
#endif
{
    /* Ensure WAL directory exists */
    ::mkdir(wal_dir.c_str(), 0755);
}

Flusher::~Flusher() {
    stop();
    if (fd_ >= 0)
        ::close(fd_);
    std::free(write_buf_);
#ifdef __linux__
    if (uring_) {
        ::io_uring_queue_exit(static_cast<struct io_uring*>(uring_));
        delete static_cast<struct io_uring*>(uring_);
    }
#endif
}



void Flusher::start() {
    if (running_.exchange(true))
        return;  

    open_segment(0);

#ifdef __linux__
    init_io_uring();
#endif

    thread_ = std::thread([this]() { this->run(); });
}

void Flusher::stop() {
    if (!running_.exchange(false))
        return;  

    if (thread_.joinable())
        thread_.join();

  
    if (fd_ >= 0)
        sync_current_segment();
}



void Flusher::open_segment(uint64_t seg_id) {
    if (fd_ >= 0) {
        sync_current_segment();
        ::close(fd_);
    }

    segment_id_ = seg_id;
    segment_offset_ = 0;

    std::string path = segment_path(wal_dir_, seg_id);

    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef __linux__
    flags |= O_DIRECT;
#endif
    fd_ = ::open(path.c_str(), flags, 0644);
    if (fd_ < 0) {
        std::perror("Flusher::open_segment");
        std::abort();
    }

#ifdef __APPLE__
    /* On macOS, O_DIRECT doesn't exist.  Use F_NOCACHE instead
     * to bypass the buffer cache (similar effect). */
    ::fcntl(fd_, F_NOCACHE, 1);
#endif
}

void Flusher::rotate_segment() {
    open_segment(segment_id_ + 1);
}


void Flusher::sync_current_segment() {
    if (fd_ < 0)
        return;
#ifdef __APPLE__
    /* F_FULLFSYNC flushes the drive's write cache on macOS.
     * fdatasync() only guarantees metadata on HFS+. */
    ::fcntl(fd_, F_FULLFSYNC);
#else
    ::fdatasync(fd_);
#endif
}



void Flusher::write_batch(const void* data, size_t len) {
    /*
     * Pad `len` up to WRITE_ALIGNMENT for O_DIRECT.
     * The extra bytes are zero-filled in write_buf_.
     */
    size_t aligned_len = (len + WRITE_ALIGNMENT - 1) & ~(WRITE_ALIGNMENT - 1);

    /* Copy to aligned staging buffer (may already be there) */
    if (data != write_buf_) {
        assert(aligned_len <= FLUSH_BATCH_MAX);
        std::memcpy(write_buf_, data, len);
    }
    /* Zero-pad the tail */
    if (aligned_len > len)
        std::memset(write_buf_ + len, 0, aligned_len - len);

    /* Would exceed segment?  Rotate first. */
    if (segment_offset_ + aligned_len > WAL_SEGMENT_SIZE)
        rotate_segment();

#ifdef __linux__
    submit_uring_write(write_buf_, aligned_len, segment_offset_);
    wait_uring_completion();
#else
    /* macOS: blocking pwrite */
    ssize_t written = ::pwrite(fd_, write_buf_, aligned_len,
                               static_cast<off_t>(segment_offset_));
    if (written < 0) {
        std::perror("Flusher::write_batch pwrite");
        std::abort();
    }
    assert(static_cast<size_t>(written) == aligned_len);
#endif

    segment_offset_ += aligned_len;
}



#ifdef __linux__

void Flusher::init_io_uring() {
    auto* ring = new struct io_uring;
    int rc = io_uring_queue_init(32, ring, 0);
    if (rc < 0) {
        fprintf(stderr, "io_uring_queue_init failed: %s\n", strerror(-rc));
        /* Fall back to pwrite — set uring_ to nullptr */
        delete ring;
        uring_ = nullptr;
        return;
    }
    uring_ = ring;
}

void Flusher::submit_uring_write(const void* data, size_t len, uint64_t offset) {
    if (!uring_) {
        /* Fallback: blocking pwrite */
        ssize_t w = ::pwrite(fd_, data, len, static_cast<off_t>(offset));
        if (w < 0) {
            std::perror("Flusher::uring_fallback pwrite");
            std::abort();
        }
        return;
    }

    auto* ring = static_cast<struct io_uring*>(uring_);
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    assert(sqe && "SQ full — increase queue depth");

    io_uring_prep_write(sqe, fd_, data, static_cast<unsigned>(len),
                        static_cast<__u64>(offset));
    sqe->flags |= IOSQE_IO_DRAIN;  /* serialize writes within segment */

    int rc = io_uring_submit(ring);
    if (rc < 0) {
        fprintf(stderr, "io_uring_submit: %s\n", strerror(-rc));
        std::abort();
    }
}

void Flusher::wait_uring_completion() {
    if (!uring_)
        return;

    auto* ring = static_cast<struct io_uring*>(uring_);
    struct io_uring_cqe* cqe = nullptr;
    int rc = io_uring_wait_cqe(ring, &cqe);
    if (rc < 0) {
        fprintf(stderr, "io_uring_wait_cqe: %s\n", strerror(-rc));
        std::abort();
    }
    if (cqe->res < 0) {
        fprintf(stderr, "io_uring write failed: %s\n", strerror(-cqe->res));
        std::abort();
    }
    io_uring_cqe_seen(ring, cqe);
}

#endif 



/*
 * scan_max_lsn — walk a buffer of WAL records, skipping zero-padding
 * from 4KB block alignment, and return the highest LSN found.
 */
static uint64_t scan_max_lsn(const char* buf, size_t len) {
    uint64_t max_lsn = 0;
    const char* scan = buf;
    const char* end  = buf + len;

    while (scan + sizeof(RecordHeader) <= end) {
        RecordHeader hdr;
        std::memcpy(&hdr, scan, sizeof(RecordHeader));

        /* Skip zero-padding left by 4KB-aligned ThreadBuf flushes.
         * Check lsn==0 AND total_size==0 (not just the first byte),
         * because a valid record can have lsn % 256 == 0 giving a
         * zero first byte in little-endian. */
        if (hdr.lsn == 0) {
            /* Skip all contiguous zero bytes — padding may not be
             * a multiple of sizeof(RecordHeader). */
            while (scan < end && *scan == 0)
                ++scan;
            continue;
        }

        if (hdr.total_size < sizeof(RecordHeader) ||
            hdr.total_size > FLUSH_BATCH_MAX ||
            scan + hdr.total_size > end) {
            /* Corrupted or partial — skip one byte */
            ++scan;
            continue;
        }

        if (hdr.lsn > max_lsn)
            max_lsn = hdr.lsn;
        scan += hdr.total_size;
    }
    return max_lsn;
}

void Flusher::run() {
    size_t bytes_since_sync = 0;

    while (running_.load(std::memory_order_acquire)) {
        const void* ptr = nullptr;
        size_t avail = ring_.peek(&ptr);

        if (avail == 0) {
         
            if (bytes_since_sync > 0) {
                sync_current_segment();
                bytes_since_sync = 0;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(50));
            continue;
        }

        size_t batch_len = std::min(avail, FLUSH_BATCH_MAX);

        std::memcpy(write_buf_, ptr, batch_len);

        uint64_t max_lsn = scan_max_lsn(write_buf_, batch_len);

        write_batch(write_buf_, batch_len);
        bytes_since_sync += batch_len;

        ring_.advance(batch_len);

        /* Sync periodically — every 256 KB or when idle (handled above) */
        if (bytes_since_sync >= 256 * 1024) {
            sync_current_segment();
            bytes_since_sync = 0;
        }

        /* Publish durable LSN */
        if (max_lsn > flushed_lsn_.load(std::memory_order_relaxed))
            flushed_lsn_.store(max_lsn, std::memory_order_release);
    }

    /* Drain remaining data */
    for (;;) {
        const void* ptr = nullptr;
        size_t avail = ring_.peek(&ptr);
        if (avail == 0)
            break;

        size_t batch_len = std::min(avail, FLUSH_BATCH_MAX);
        std::memcpy(write_buf_, ptr, batch_len);

        uint64_t max_lsn = scan_max_lsn(write_buf_, batch_len);

        write_batch(write_buf_, batch_len);
        ring_.advance(batch_len);

        if (max_lsn > flushed_lsn_.load(std::memory_order_relaxed))
            flushed_lsn_.store(max_lsn, std::memory_order_release);
    }

    sync_current_segment();
    flushed_lsn_.store(flushed_lsn_.load(std::memory_order_relaxed),
                       std::memory_order_release);
}

} 
} 