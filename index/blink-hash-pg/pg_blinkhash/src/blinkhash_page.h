/*
 * blinkhash_page.h — Buffer manager and page layout for B^link-hash
 *
 * When operating inside PostgreSQL, tree nodes must live in 8-KB
 * shared buffer pages.  This module defines the page layout and
 * provides read/write helpers.
 *
 * Hash leaf nodes (256 KB) span multiple contiguous pages (an extent
 * of 32 standard pages).  Btree leaf nodes and internal nodes fit
 * in a single page.
 *
 * Implements: Phase 6.2 of IMPLEMENTATION_SEQUENCE.md
 */
#ifndef BLINKHASH_PAGE_H
#define BLINKHASH_PAGE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "storage/bufpage.h"
#include "storage/block.h"

/* ─── Page layout constants ──────────────────────────────────────── */

/* Standard PG page is 8 KB */
#define BH_PAGE_SIZE          BLCKSZ

/* Hash leaf extent: 256 KB / 8 KB = 32 pages */
#define BH_HASH_LEAF_PAGES    32

/* ─── B^link-hash page header (stored in pd_special area) ─────────── */

typedef struct BHPageOpaqueData {
    uint64      node_id;        /* persistent node identity          */
    uint32      node_type;      /* 0=inode, 1=lnode_hash, 2=lnode_bt */
    uint16      level;          /* tree level (0 = leaf)             */
    uint16      flags;          /* BH_PAGE_IS_LEAF, etc.             */
    uint64      page_lsn;       /* LSN of last modification          */
    BlockNumber right_sibling;  /* link-right pointer                */
} BHPageOpaqueData;

typedef BHPageOpaqueData *BHPageOpaque;

#define BH_PAGE_IS_LEAF       0x0001
#define BH_PAGE_IS_HASH       0x0002   /* hash-based leaf            */
#define BH_PAGE_IS_BTREE      0x0004   /* btree-based leaf           */
#define BH_PAGE_IS_EXTENT_HEAD 0x0008  /* first page of multi-page   */
#define BH_PAGE_IS_DELETED    0x0010

/* ─── Page initialization ────────────────────────────────────────── */

/*
 * Initialize a freshly allocated buffer page with the B^link-hash
 * special area.
 */
void bh_page_init(Page page, uint64 node_id, uint32 node_type,
                  uint16 level, uint16 flags);

/* ─── Node ↔ page serialization ──────────────────────────────────── */

/*
 * Serialize a B^link-hash inode/lnode into one or more pages.
 * For hash leaves: writes BH_HASH_LEAF_PAGES consecutive pages.
 * For btree leaves and inodes: writes a single page.
 *
 * Returns the number of pages written.
 */
int bh_node_to_pages(const void *node, uint32 node_type,
                     Page *pages_out, int max_pages);

/*
 * Deserialize one or more pages back into a heap-allocated node.
 * The caller takes ownership of the returned pointer.
 */
void* bh_pages_to_node(const Page *pages, int num_pages,
                       uint32 node_type);

/* ─── Buffer manager helpers ─────────────────────────────────────── */

/*
 * Read the B^link-hash opaque data from a buffer page.
 */
static inline BHPageOpaque
bh_page_get_opaque(Page page)
{
    return (BHPageOpaque) PageGetSpecialPointer(page);
}

/*
 * Allocate a new extent of `num_pages` contiguous pages for a
 * hash leaf.  Returns the BlockNumber of the first page.
 */
BlockNumber bh_alloc_extent(Relation indexRelation, int num_pages);

/*
 * Free an extent starting at `start` for `num_pages` pages.
 * Marks them as available in the free-space map.
 */
void bh_free_extent(Relation indexRelation,
                    BlockNumber start, int num_pages);

#ifdef __cplusplus
}
#endif

#endif /* BLINKHASH_PAGE_H */
