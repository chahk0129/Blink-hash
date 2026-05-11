/*
 * blinkhash_page.c — PG buffer manager integration (Phase 8)
 *
 * When BH_USE_PG_WAL is defined, index pages live in PG shared buffers.
 * All page access goes through ReadBuffer/MarkBufferDirty/etc.
 *
 * For hash leaf extents (32 pages), we allocate contiguous blocks
 * using relation extension.
 */

#ifdef BH_USE_PG_WAL

#include "blinkhash_page.h"
#include "blinkhash_core.h"

#include "postgres.h"
#include "access/generic_xlog.h"
#include "catalog/index.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "utils/rel.h"

void
bh_page_init(Page page, uint64 node_id, uint32 node_type,
             uint16 level, uint16 flags)
{
    BHPageOpaque opaque;

    PageInit(page, BLCKSZ, sizeof(BHPageOpaqueData));

    opaque = bh_page_get_opaque(page);
    opaque->node_id        = node_id;
    opaque->node_type      = node_type;
    opaque->level          = level;
    opaque->flags          = flags;
    opaque->page_lsn       = 0;
    opaque->right_sibling  = InvalidBlockNumber;
}

/*
 * Allocate a contiguous extent of pages for hash leaf storage.
 *
 * Uses relation extension (append to end of data file).
 * Returns the starting BlockNumber.
 */
BlockNumber
bh_alloc_extent(Relation indexRelation, int num_pages)
{
    BlockNumber start;

    /*
     * Lock the relation extension lock to prevent concurrent
     * extends from creating gaps.
     */
    LockRelationForExtension(indexRelation, ExclusiveLock);

    start = RelationGetNumberOfBlocks(indexRelation);

    /* Extend the relation by num_pages blocks */
    for (int i = 0; i < num_pages; i++)
    {
        Buffer buf;
        Page page;

        buf = ReadBufferExtended(indexRelation,
                                        MAIN_FORKNUM,
                                        P_NEW,
                                        RBM_ZERO_AND_LOCK,
                                        NULL);
        page = BufferGetPage(buf);
        bh_page_init(page, 0, 0, 0, 0);
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }

    UnlockRelationForExtension(indexRelation, ExclusiveLock);

    return start;
}

/*
 * Free an extent (mark pages as free in FSM).
 */
void
bh_free_extent(Relation indexRelation, BlockNumber start, int num_pages)
{
    int i;

    for (i = 0; i < num_pages; i++)
    {
        /* Mark page as fully available in free space map */
        RecordPageWithFreeSpace(indexRelation, start + i, BLCKSZ);
    }
}

/*
 * Serialize a C++ tree node into PG buffer pages.
 *
 * Uses the C bridge serialize functions (blinkhash_core.cpp)
 * which call the C++ templates from wal/bh_page.cpp.
 * The standalone bh_page_t (8KB) payload is then copied into
 * the PG Page user-data area.
 *
 * @param node        Pointer to inode_t / lnode_btree_t / lnode_hash_t
 * @param node_type   BHPageOpaqueData.node_type (0=inode, 1=hash, 2=btree)
 * @param pages_out   Array of PG Pages to fill
 * @param max_pages   Capacity of pages_out array
 * @return            Number of pages written, 0 on error
 */
int
bh_node_to_pages(const void *node, uint32 node_type,
                 Page *pages_out, int max_pages)
{
    int bh_type;
    Size buf_size;
    char *tmp_buf;
    int num_pages;
    int i;

    /*
     * Map the PG opaque node_type to the BH_NODE_TYPE_* codes
     * used by the C bridge.
     */
    switch (node_type)
    {
        case 0: bh_type = BH_NODE_TYPE_INODE;       break;
        case 1: bh_type = BH_NODE_TYPE_LNODE_HASH;  break;
        case 2: bh_type = BH_NODE_TYPE_LNODE_BTREE; break;
        default:
            elog(WARNING, "bh_node_to_pages: unknown node_type %u", node_type);
            return 0;
    }

    /*
     * Allocate a temporary buffer of standalone 8KB pages.
     * We serialize the C++ node into this buffer, then copy
     * the payload bytes into each PG Page.
     */
    buf_size = (Size) max_pages * 8192;
    tmp_buf = (char *) palloc0(buf_size);

    num_pages = bh_serialize_node(node, 'i', bh_type,
                                      tmp_buf, max_pages);
    if (num_pages <= 0 || num_pages > max_pages)
    {
        pfree(tmp_buf);
        return 0;
    }

    /*
     * Copy each standalone page's payload into the corresponding
     * PG Page's user-data area.  The standalone page layout:
     *   [header: 64 bytes][payload: 8128 bytes]
     * We skip the 64-byte header and copy the payload into the
     * PG page starting after the PG header.
     */
    for (i = 0; i < num_pages; i++)
    {
        char *src_page = tmp_buf + (Size)i * 8192;
        uint16 payload_used;
        char *payload_src;

        /* Read payload_used from the standalone header (offset 58) */
        memcpy(&payload_used, src_page + 58, sizeof(uint16));

        if (payload_used > 0 && payload_used <= 8128)
        {
            payload_src = src_page + 64; /* skip PageHeader */

            /*
             * Store the payload into the PG page's tuple area.
             * Use PageAddItem or direct memcpy into the page body.
             */
            memcpy((char *)pages_out[i] + MAXALIGN(SizeOfPageHeaderData),
                   payload_src, payload_used);
        }
    }

    pfree(tmp_buf);
    return num_pages;
}

void *
bh_pages_to_node(const Page *pages, int num_pages, uint32 node_type)
{
    int bh_type;
    Size buf_size;
    char *tmp_buf;
    int i;
    void *result;

    /*
     * Map the PG opaque node_type to the C bridge type codes.
     */
    switch (node_type)
    {
        case 0: bh_type = BH_NODE_TYPE_INODE;       break;
        case 1: bh_type = BH_NODE_TYPE_LNODE_HASH;  break;
        case 2: bh_type = BH_NODE_TYPE_LNODE_BTREE; break;
        default:
            elog(WARNING, "bh_pages_to_node: unknown node_type %u", node_type);
            return NULL;
    }

    /*
     * Reconstruct standalone bh_page_t pages from PG Pages.
     * Copy the user-data area from each PG page into the payload
     * of a temporary standalone page.
     */
    buf_size = (Size) num_pages * 8192;
    tmp_buf = (char *) palloc0(buf_size);

    for (i = 0; i < num_pages; i++)
    {
        char *dst_page = tmp_buf + (Size)i * 8192;
        BHPageOpaque opaque = bh_page_get_opaque(pages[i]);
        uint64 page_id;
        uint8 nt;
        uint8 lvl;
        Size pg_header_size;
        Size special_offset;
        Size payload_used;
        uint16 pu16;

        /* Set up a minimal standalone header */
        /* page_id at offset 0 */
        page_id = (uint64)(i + 1);
        memcpy(dst_page + 0, &page_id, 8);
        /* node_id at offset 8 */
        memcpy(dst_page + 8, &opaque->node_id, 8);
        /* node_type at offset 16 */
        nt = (uint8)opaque->node_type;
        memcpy(dst_page + 16, &nt, 1);
        /* level at offset 17 */
        lvl = (uint8)opaque->level;
        memcpy(dst_page + 17, &lvl, 1);

        /*
         * Copy PG page body into the standalone payload area.
         * Estimate payload_used from the PG page free space.
         */
        pg_header_size = MAXALIGN(SizeOfPageHeaderData);
        special_offset = ((PageHeader)pages[i])->pd_special;
        payload_used = special_offset - pg_header_size;
        if (payload_used > 8128)
            payload_used = 8128;

        /* payload_used at offset 58 */
        pu16 = (uint16)payload_used;
        memcpy(dst_page + 58, &pu16, sizeof(uint16));

        /* payload at offset 64 */
        memcpy(dst_page + 64,
               (char *)pages[i] + pg_header_size, payload_used);
    }

    result = bh_deserialize_node(tmp_buf, num_pages, 'i', bh_type);
    pfree(tmp_buf);
    return result;
}

#endif /* BH_USE_PG_WAL */