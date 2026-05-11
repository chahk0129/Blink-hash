

#include "bh_buffer_pool.h"

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace BLINK_HASH {
namespace WAL {



BufferPool::BufferPool(const std::string& data_file_path,
                       size_t pool_size)
    : data_file_path_(data_file_path),
      pool_size_(pool_size),
      pages_(pool_size),
      descs_(pool_size) {

    data_fd_ = ::open(data_file_path.c_str(),
                      O_RDWR | O_CREAT, 0644);
    if (data_fd_ < 0) {
        perror("BufferPool: open data file");
        std::abort();
    }

   
    for (size_t i = 0; i < pool_size_; i++) {
        descs_[i].page_id = 0;  /* unused */
        descs_[i].pin_count.store(0);
        descs_[i].usage_count.store(0);
        descs_[i].dirty = false;
    }

  
    for (size_t i = 0; i < pool_size_; i++) {
        pages_[i].init(0);
    }

 
    struct stat st;
    if (::fstat(data_fd_, &st) == 0 && st.st_size >= BH_PAGE_SIZE) {
        bh_page_t metapage;
        ssize_t n = ::pread(data_fd_, &metapage, BH_PAGE_SIZE, 0);
        if (n == BH_PAGE_SIZE && metapage.header.page_id == METAPAGE_ID) {
            uint64_t stored_next;
            std::memcpy(&stored_next, metapage.payload, sizeof(stored_next));
            if (stored_next > next_page_id_.load())
                next_page_id_.store(stored_next);
        }
    }
}

BufferPool::~BufferPool() {
   
    flush_all_dirty();

  
    bh_page_t metapage;
    metapage.init(METAPAGE_ID);
    uint64_t np = next_page_id_.load();
    std::memcpy(metapage.payload, &np, sizeof(np));
    metapage.header.checksum = metapage.compute_checksum();

    ::pwrite(data_fd_, &metapage, BH_PAGE_SIZE, 0);
#ifdef __APPLE__
    ::fcntl(data_fd_, F_FULLFSYNC);
#else
    ::fdatasync(data_fd_);
#endif

    ::close(data_fd_);
}



void BufferPool::read_page_from_disk(uint64_t page_id, bh_page_t* dest) {
    off_t offset = static_cast<off_t>(page_id) * BH_PAGE_SIZE;
    ssize_t n = ::pread(data_fd_, dest, BH_PAGE_SIZE, offset);

    if (n != BH_PAGE_SIZE) {
       
        dest->init(page_id);
    }
}

void BufferPool::write_page_to_disk(uint64_t page_id,
                                     const bh_page_t* src) {
    off_t offset = static_cast<off_t>(page_id) * BH_PAGE_SIZE;
    ssize_t n = ::pwrite(data_fd_, src, BH_PAGE_SIZE, offset);
    if (n != BH_PAGE_SIZE) {
        perror("BufferPool: pwrite");
    }
    stat_flushes_.fetch_add(1, std::memory_order_relaxed);
}

/*
 *  Clock-sweep eviction
 *  */

size_t BufferPool::find_victim() {
    /*
     * Clock-sweep: scan slots starting from clock_hand_.
     * Skip pinned slots (pin_count > 0).
     * Decrement usage_count until 0, then select as victim.
     */
    size_t max_iters = pool_size_ * 2;
    for (size_t iter = 0; iter < max_iters; iter++) {
        size_t idx = clock_hand_.fetch_add(1, std::memory_order_relaxed)
                     % pool_size_;

        if (descs_[idx].pin_count.load(std::memory_order_relaxed) > 0)
            continue;  

        if (descs_[idx].page_id == 0)
            return idx; 

        int usage = descs_[idx].usage_count.load(std::memory_order_relaxed);
        if (usage > 0) {
            descs_[idx].usage_count.fetch_sub(1, std::memory_order_relaxed);
            continue;
        }

   
        return idx;
    }


    fprintf(stderr, "BufferPool: no victim found after %zu iterations\n",
            max_iters);
    std::abort();
}

void BufferPool::evict_slot(size_t idx) {
    if (descs_[idx].dirty) {
       
        pages_[idx].header.checksum = pages_[idx].compute_checksum();
        write_page_to_disk(descs_[idx].page_id, &pages_[idx]);
        descs_[idx].dirty = false;
    }

    /* Remove from page table */
    {
        std::lock_guard<std::mutex> lk(page_table_mu_);
        page_table_.erase(descs_[idx].page_id);
    }

    descs_[idx].page_id = 0;
    descs_[idx].usage_count.store(0, std::memory_order_relaxed);
    stat_evictions_.fetch_add(1, std::memory_order_relaxed);
}


bh_page_t* BufferPool::pin_page(uint64_t page_id, bool new_page) {
    assert(page_id != 0);

    /* Check if already in pool */
    {
        std::lock_guard<std::mutex> lk(page_table_mu_);
        auto it = page_table_.find(page_id);
        if (it != page_table_.end()) {
            size_t idx = it->second;
            descs_[idx].pin_count.fetch_add(1, std::memory_order_relaxed);
            descs_[idx].usage_count.store(1, std::memory_order_relaxed);
            stat_hits_.fetch_add(1, std::memory_order_relaxed);
            return &pages_[idx];
        }
    }

    /* Cache miss — find a slot */
    stat_misses_.fetch_add(1, std::memory_order_relaxed);
    size_t idx = find_victim();

    
    if (descs_[idx].page_id != 0)
        evict_slot(idx);

 
    if (new_page) {
        pages_[idx].init(page_id);
    } else {
        read_page_from_disk(page_id, &pages_[idx]);
    }

    /* Set up descriptor */
    descs_[idx].page_id = page_id;
    descs_[idx].pin_count.store(1, std::memory_order_relaxed);
    descs_[idx].usage_count.store(1, std::memory_order_relaxed);
    descs_[idx].dirty = false;

    
    {
        std::lock_guard<std::mutex> lk(page_table_mu_);
        page_table_[page_id] = idx;
    }

    return &pages_[idx];
}

void BufferPool::unpin_page(uint64_t page_id, bool dirty) {
    std::lock_guard<std::mutex> lk(page_table_mu_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        fprintf(stderr, "BufferPool: unpin non-resident page %llu\n",
                (unsigned long long)page_id);
        return;
    }

    size_t idx = it->second;
    int old_pin = descs_[idx].pin_count.fetch_sub(1, std::memory_order_relaxed);
    assert(old_pin > 0);

    if (dirty)
        descs_[idx].dirty = true;
}



void BufferPool::flush_page(uint64_t page_id) {
    std::lock_guard<std::mutex> lk(page_table_mu_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) return;

    size_t idx = it->second;
    if (descs_[idx].dirty) {
        pages_[idx].header.checksum = pages_[idx].compute_checksum();
        write_page_to_disk(page_id, &pages_[idx]);
        descs_[idx].dirty = false;
    }
}

void BufferPool::flush_all_dirty() {
    for (size_t i = 0; i < pool_size_; i++) {
        if (descs_[i].page_id != 0 && descs_[i].dirty) {
            pages_[i].header.checksum = pages_[i].compute_checksum();
            write_page_to_disk(descs_[i].page_id, &pages_[i]);
            descs_[i].dirty = false;
        }
    }

#ifdef __APPLE__
    ::fcntl(data_fd_, F_FULLFSYNC);
#else
    ::fdatasync(data_fd_);
#endif
}

uint64_t BufferPool::alloc_page_id() {
    return next_page_id_.fetch_add(1, std::memory_order_relaxed);
}

size_t BufferPool::resident_pages() const {
    std::lock_guard<std::mutex> lk(page_table_mu_);
    return page_table_.size();
}

size_t BufferPool::dirty_pages() const {
    size_t count = 0;
    for (size_t i = 0; i < pool_size_; i++) {
        if (descs_[i].page_id != 0 && descs_[i].dirty)
            count++;
    }
    return count;
}

BufferPool::Stats BufferPool::stats() const {
    return {
        stat_hits_.load(std::memory_order_relaxed),
        stat_misses_.load(std::memory_order_relaxed),
        stat_evictions_.load(std::memory_order_relaxed),
        stat_flushes_.load(std::memory_order_relaxed)
    };
}

}
} 