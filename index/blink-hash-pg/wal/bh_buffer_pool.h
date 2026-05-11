#ifndef BLINK_HASH_BH_BUFFER_POOL_H__
#define BLINK_HASH_BH_BUFFER_POOL_H__

/*
 * The buffer pool manages a fixed number of bh_page_t slots.
 * Pages are read from / written to a data file on demand.
 *
 * Pin/unpin semantics:
 *   - pin_page(page_id): load page into pool, increment pin count,
 *     return pointer.  Pinned pages cannot be evicted.
 *   - unpin_page(page_id, dirty): decrement pin count.  If dirty=true,
 *     mark the page for background flush.
 *
 * Eviction: clock-sweep algorithm (same as PostgreSQL).
 */

#include "bh_page.h"
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace BLINK_HASH {
namespace WAL {


struct BufferDesc {
    uint64_t           page_id;    
    std::atomic<int>   pin_count;  /* >0 = pinned, cannot evict */
    std::atomic<int>   usage_count;/* clock-sweep counter */
    bool               dirty;
};

class BufferPool {
public:
    /*
     * @param data_file_path  Path to the data file for page I/O.
     * @param pool_size       Number of page slots in the pool.
     *                        Default: 16384 slots = 128 MB.
     */
    BufferPool(const std::string& data_file_path,
               size_t pool_size = 16384);
    ~BufferPool();


    BufferPool(const BufferPool&) = delete;
    BufferPool& operator=(const BufferPool&) = delete;

    /*
     * Pin a page: load into pool if not present, increment pin count.
     * Returns pointer to the page.
     *
     * If `new_page` is true, allocates a fresh page without reading
     * from disk (for newly created nodes).
     */
    bh_page_t* pin_page(uint64_t page_id, bool new_page = false);

    /*
     * Unpin a page.  Decrements pin count.
     * If dirty=true, marks the page for eventual flush.
     */
    void unpin_page(uint64_t page_id, bool dirty = false);


    void flush_page(uint64_t page_id);

    void flush_all_dirty();


    uint64_t alloc_page_id();

 
    size_t resident_pages() const;


    size_t dirty_pages() const;

    struct Stats {
        uint64_t hits;
        uint64_t misses;
        uint64_t evictions;
        uint64_t flushes;
    };

    Stats stats() const;

private:
 
    void read_page_from_disk(uint64_t page_id, bh_page_t* dest);
    void write_page_to_disk(uint64_t page_id, const bh_page_t* src);

    /* Clock-sweep: find a victim slot for eviction */
    size_t find_victim();


    void evict_slot(size_t idx);

 
    std::string            data_file_path_;
    int                    data_fd_;
    size_t                 pool_size_;

  
    std::vector<bh_page_t>   pages_;
    std::vector<BufferDesc>  descs_;

   
    std::unordered_map<uint64_t, size_t> page_table_;
    mutable std::mutex page_table_mu_;

   
    std::atomic<size_t> clock_hand_{0};


    std::atomic<uint64_t> next_page_id_{METAPAGE_ID + 1};


    std::atomic<uint64_t> stat_hits_{0};
    std::atomic<uint64_t> stat_misses_{0};
    std::atomic<uint64_t> stat_evictions_{0};
    std::atomic<uint64_t> stat_flushes_{0};
};

} 
} 

#endif 