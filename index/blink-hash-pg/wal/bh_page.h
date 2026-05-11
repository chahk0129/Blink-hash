#ifndef BLINK_HASH_BH_PAGE_H__
#define BLINK_HASH_BH_PAGE_H__

/*
 * bh_page.h — Fixed-size 8 KB page for node persistence
 *
 * Each page wraps a tree node (or part of a multi-page node).
 * The page format is designed to be compatible with PostgreSQL's
 * buffer manager (Phase 7).
 *
 * Page layout:
 *   [PageHeader | Payload ............... ]
 *   [64 bytes   | 8128 bytes             ]
 *   Total: 8192 bytes = 8 KB
 *

 */

#include <atomic>
#include <cstdint>
#include <cstring>

namespace BLINK_HASH {
namespace WAL {

constexpr size_t BH_PAGE_SIZE   = 8192;          /* 8 KB */
constexpr size_t BH_PAGE_HEADER = 64;
constexpr size_t BH_PAGE_PAYLOAD = BH_PAGE_SIZE - BH_PAGE_HEADER;


constexpr uint64_t INVALID_PAGE_ID = 0;
constexpr uint64_t METAPAGE_ID     = 1;  


enum class PageNodeType : uint8_t {
    INVALID      = 0,
    INODE        = 1,  
    LNODE_HASH   = 2,   /* hash leaf (part of a multi-page extent) */
    LNODE_BTREE  = 3,   /* btree leaf (fits in one page) */
};

/*
 * Page flags bitmap.
 */
enum PageFlags : uint16_t {
    PAGE_FLAG_NONE     = 0,
    PAGE_FLAG_DIRTY    = 1 << 0,
    PAGE_FLAG_EXTENT   = 1 << 1,   /* part of a multi-page extent */
    PAGE_FLAG_FREE     = 1 << 2,   /* on free list */
};

/*
 * Page header — 64 bytes, cache-line aligned.
 *
 * For multi-page nodes (hash leaves):
 *   extent_id       = page_id of the first page in the extent
 *   page_in_extent  = 0-based index within the extent
 *   extent_pages    = total pages in the extent
 */
struct PageHeader {
    uint64_t     page_id;           /* unique page identifier          */
    uint64_t     node_id;           /* corresponding node_id           */
    PageNodeType node_type;         /* INODE / LNODE_HASH / LNODE_BTREE */
    uint8_t      level;             /* tree level (0 = leaf)           */
    uint16_t     flags;             /* PageFlags bitmap                */
    uint32_t     checksum;          /* CRC-32C of payload              */
    uint64_t     lsn;              /* page LSN (last WAL record)       */
    uint64_t     extent_id;         /* first page_id if multi-page     */
    uint16_t     page_in_extent;    /* index within extent             */
    uint16_t     extent_pages;      /* total pages in extent           */
    uint16_t     payload_used;      /* bytes of payload actually used  */
    uint8_t      _reserved[18];     /* pad to 64 bytes                 */
};
static_assert(sizeof(PageHeader) == 64, "PageHeader must be 64 bytes");


struct alignas(BH_PAGE_SIZE) bh_page_t {
    PageHeader header;
    char       payload[BH_PAGE_PAYLOAD];

 
    void init(uint64_t page_id) {
        std::memset(this, 0, sizeof(*this));
        header.page_id = page_id;
    }

    
    uint32_t compute_checksum() const;

 
    bool verify_checksum() const {
        return header.checksum == compute_checksum();
    }


    void mark_dirty() {
        header.flags |= PAGE_FLAG_DIRTY;
    }

   
    void clear_dirty() {
        header.flags &= ~PAGE_FLAG_DIRTY;
    }

    bool is_dirty() const {
        return (header.flags & PAGE_FLAG_DIRTY) != 0;
    }
};
static_assert(sizeof(bh_page_t) == BH_PAGE_SIZE,
              "bh_page_t must be exactly 8 KB");

/* 
 *  These functions pack/unpack tree nodes into bh_page_t payloads.
 *  Used by the buffer pool for I/O and by snapshot for persistence.
 */

/*
 * Serialize an internal node into a page.
 * Returns bytes written into payload.
 *
 * Format:
 *   [cnt:4][key_size:4][leftmost_ptr_node_id:8][sibling_ptr_node_id:8]
 *   for each entry: [key:key_size][child_node_id:8]
 */
template <typename Key_t>
uint16_t serialize_inode(const void* inode, bh_page_t* page);


template <typename Key_t>
void* deserialize_inode(const bh_page_t* page, class NodeMap* nmap);

/*
 * Serialize a btree leaf into a page.
 * Returns bytes written.
 *
 * Format:
 *   [cnt:4][high_key:key_size][sibling_node_id:8]
 *   for each entry: [key:key_size][value:8]
 */
template <typename Key_t, typename Value_t>
uint16_t serialize_btree_leaf(const void* leaf, bh_page_t* page);


template <typename Key_t, typename Value_t>
void* deserialize_btree_leaf(const bh_page_t* page);

/*
 * Serialize a hash leaf into an extent (array of pages).
 * A 256 KB hash leaf spans ~32 pages.
 *
 * @param leaf       pointer to lnode_hash_t
 * @param pages      output array of pages (caller allocates)
 * @param max_pages  capacity of pages array
 * @return           number of pages written
 */
template <typename Key_t, typename Value_t>
uint16_t serialize_hash_leaf(const void* leaf, bh_page_t* pages,
                             uint16_t max_pages);


template <typename Key_t, typename Value_t>
void* deserialize_hash_leaf(const bh_page_t* pages, uint16_t num_pages);

}
} 

#endif 