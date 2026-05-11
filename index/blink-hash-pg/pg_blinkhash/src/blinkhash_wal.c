/*
 * Registers a Custom WAL Resource Manager (PG 15+) and implements:
 *   - XLog record emission helpers (blinkhash_xlog_insert, etc.)
 *   - Redo callbacks for crash recovery
 *   - Resource manager registration in _PG_init()
 */

#include "blinkhash_wal.h"
#include "blinkhash_page.h"
#include "blinkhash_core.h"
#include "blinkhash_utils.h"

#include "postgres.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlog_internal.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/rel.h"



#define RM_BLINKHASH_ID   128   /* fixed ID for reproducibility */
#define RM_BLINKHASH_NAME "blinkhash"


static void blinkhash_redo_insert_impl(XLogReaderState *record);
static void blinkhash_redo_delete_impl(XLogReaderState *record);
static void blinkhash_redo_update_impl(XLogReaderState *record);
static void blinkhash_redo_split_leaf_impl(XLogReaderState *record);
static void blinkhash_redo_split_internal_impl(XLogReaderState *record);
static void blinkhash_redo_convert_impl(XLogReaderState *record);
static void blinkhash_redo_new_root_impl(XLogReaderState *record);
static void blinkhash_redo_stabilize_impl(XLogReaderState *record);


void
blinkhash_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info)
    {
        case XLOG_BLINKHASH_INSERT:
            blinkhash_redo_insert_impl(record);
            break;
        case XLOG_BLINKHASH_DELETE:
            blinkhash_redo_delete_impl(record);
            break;
        case XLOG_BLINKHASH_UPDATE:
            blinkhash_redo_update_impl(record);
            break;
        case XLOG_BLINKHASH_SPLIT_LEAF:
            blinkhash_redo_split_leaf_impl(record);
            break;
        case XLOG_BLINKHASH_SPLIT_INTERNAL:
            blinkhash_redo_split_internal_impl(record);
            break;
        case XLOG_BLINKHASH_CONVERT:
            blinkhash_redo_convert_impl(record);
            break;
        case XLOG_BLINKHASH_NEW_ROOT:
            blinkhash_redo_new_root_impl(record);
            break;
        case XLOG_BLINKHASH_STABILIZE:
            blinkhash_redo_stabilize_impl(record);
            break;
        default:
            elog(PANIC, "blinkhash_redo: unknown info code 0x%X", info);
    }
}


static const char *
blinkhash_identify(uint8 info)
{
    switch (info & ~XLR_INFO_MASK)
    {
        case XLOG_BLINKHASH_INSERT:     return "INSERT";
        case XLOG_BLINKHASH_DELETE:     return "DELETE";
        case XLOG_BLINKHASH_UPDATE:     return "UPDATE";
        case XLOG_BLINKHASH_SPLIT_LEAF: return "SPLIT_LEAF";
        case XLOG_BLINKHASH_SPLIT_INTERNAL: return "SPLIT_INTERNAL";
        case XLOG_BLINKHASH_CONVERT:    return "CONVERT";
        case XLOG_BLINKHASH_NEW_ROOT:   return "NEW_ROOT";
        case XLOG_BLINKHASH_STABILIZE:  return "STABILIZE";
        default:                        return "UNKNOWN";
    }
}


void
blinkhash_wal_init(void)
{
    
    static const RmgrData blinkhash_rmgr = {
        .rm_name    = RM_BLINKHASH_NAME,
        .rm_redo    = blinkhash_redo,
        .rm_desc    = NULL,             
        .rm_identify = blinkhash_identify,
        .rm_startup = NULL,
        .rm_cleanup = NULL,
        .rm_mask    = NULL,
        .rm_decode  = NULL,
    };

    RegisterCustomRmgr(RM_BLINKHASH_ID, &blinkhash_rmgr);

    elog(LOG, "blinkhash: custom WAL resource manager registered (ID=%d)",
         RM_BLINKHASH_ID);
}


XLogRecPtr
blinkhash_xlog_insert(uint64 node_id, uint32 bucket_idx,
                      const void *key_data, uint16 key_len,
                      uint64 value)
{
    xl_blinkhash_insert xlrec;
    xlrec.node_id    = node_id;
    xlrec.bucket_idx = bucket_idx;
    xlrec.key_len    = key_len;

    XLogBeginInsert();

    /* Register the fixed-size header */
    XLogRegisterData((char *) &xlrec, sizeof(xl_blinkhash_insert));

    /* Register the variable-length key data */
    XLogRegisterData((char *) key_data, key_len);

    /* Register the value */
    XLogRegisterData((char *) &value, sizeof(uint64));

    /*
     * Buffer registration for Full Page Images:
     * When the caller passes a valid buffer, register it so PG
     * includes a full page image after checkpoint.
     *
     * Usage from aminsert (when buffer manager integration is active):
     *   Buffer buf = ReadBuffer(indexRelation, target_blkno);
     *   LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
     *   ... apply insert to page ...
     *   MarkBufferDirty(buf);
     *   --- then call this function ---
     *   XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
     *
     * For now the registration happens at the call site when the
     * buffer is available.  The buffer parameter will be added
     * when the full buffer-manager path is plumbed through.
     */

    XLogRecPtr insert_lsn;
    insert_lsn = XLogInsert(RM_BLINKHASH_ID, XLOG_BLINKHASH_INSERT);
    return insert_lsn;
}


XLogRecPtr
blinkhash_xlog_delete(uint64 node_id,
                      const void *key_data, uint16 key_len)
{
    xl_blinkhash_delete xlrec;
    xlrec.node_id = node_id;
    xlrec.key_len = key_len;

    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec, sizeof(xl_blinkhash_delete));
    XLogRegisterData((char *) key_data, key_len);

    return XLogInsert(RM_BLINKHASH_ID, XLOG_BLINKHASH_DELETE);
}

XLogRecPtr
blinkhash_xlog_update(uint64 node_id,
                      const void *key_data, uint16 key_len,
                      uint64 new_value)
{
    xl_blinkhash_update xlrec;
    xlrec.node_id = node_id;
    xlrec.key_len = key_len;

    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec, sizeof(xl_blinkhash_update));
    XLogRegisterData((char *) key_data, key_len);
    XLogRegisterData((char *) &new_value, sizeof(uint64));

    return XLogInsert(RM_BLINKHASH_ID, XLOG_BLINKHASH_UPDATE);
}

XLogRecPtr
blinkhash_xlog_split_leaf(uint64 old_leaf_id, uint64 new_leaf_id,
                          const void *split_key, uint16 split_key_len,
                          uint32 num_migrated,
                          const void *entries, size_t entries_len)
{
    xl_blinkhash_split_leaf xlrec;
    xlrec.old_leaf_id   = old_leaf_id;
    xlrec.new_leaf_id   = new_leaf_id;
    xlrec.split_key_len = split_key_len;
    xlrec.num_migrated  = num_migrated;

    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec, sizeof(xl_blinkhash_split_leaf));
    XLogRegisterData((char *) split_key, split_key_len);

    if (entries_len > 0)
        XLogRegisterData((char *) entries, entries_len);

    /*
     * Buffer registration for split:
     *   XLogRegisterBuffer(0, old_leaf_buf, REGBUF_STANDARD);
     *   XLogRegisterBuffer(1, new_leaf_buf, REGBUF_WILL_INIT);
     * Will be plumbed through when buffer-manager path is active.
     */

    return XLogInsert(RM_BLINKHASH_ID, XLOG_BLINKHASH_SPLIT_LEAF);
}

XLogRecPtr
blinkhash_xlog_split_internal(uint64 inode_id, uint64 new_child_id,
                              const void *split_key, uint16 split_key_len)
{
    xl_blinkhash_split_internal xlrec;
    xlrec.inode_id      = inode_id;
    xlrec.new_child_id  = new_child_id;
    xlrec.split_key_len = split_key_len;

    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec, sizeof(xl_blinkhash_split_internal));
    XLogRegisterData((char *) split_key, split_key_len);

    return XLogInsert(RM_BLINKHASH_ID, XLOG_BLINKHASH_SPLIT_INTERNAL);
}

XLogRecPtr
blinkhash_xlog_convert(uint64 old_hash_leaf_id,
                       uint32 num_new_leaves, uint32 total_entries,
                       const void *payload, size_t payload_len)
{
    xl_blinkhash_convert xlrec;
    xlrec.old_hash_leaf_id = old_hash_leaf_id;
    xlrec.num_new_leaves   = num_new_leaves;
    xlrec.total_entries    = total_entries;

    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec, sizeof(xl_blinkhash_convert));

    if (payload_len > 0)
        XLogRegisterData((char *) payload, payload_len);

    return XLogInsert(RM_BLINKHASH_ID, XLOG_BLINKHASH_CONVERT);
}

XLogRecPtr
blinkhash_xlog_new_root(uint64 new_root_id, uint64 left_child_id,
                        uint64 right_child_id,
                        const void *split_key, uint16 split_key_len,
                        uint8 level)
{
    xl_blinkhash_new_root xlrec;
    xlrec.new_root_id    = new_root_id;
    xlrec.left_child_id  = left_child_id;
    xlrec.right_child_id = right_child_id;
    xlrec.split_key_len  = split_key_len;
    xlrec.level          = level;

    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec, sizeof(xl_blinkhash_new_root));
    XLogRegisterData((char *) split_key, split_key_len);

    return XLogInsert(RM_BLINKHASH_ID, XLOG_BLINKHASH_NEW_ROOT);
}

XLogRecPtr
blinkhash_xlog_stabilize(uint64 leaf_id, uint32 bucket_idx,
                         uint32 num_migrated,
                         const void *entries, size_t entries_len)
{
    xl_blinkhash_stabilize xlrec;
    xlrec.leaf_id      = leaf_id;
    xlrec.bucket_idx   = bucket_idx;
    xlrec.num_migrated = num_migrated;

    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec, sizeof(xl_blinkhash_stabilize));

    if (entries_len > 0)
        XLogRegisterData((char *) entries, entries_len);

    return XLogInsert(RM_BLINKHASH_ID, XLOG_BLINKHASH_STABILIZE);
}


/*
 * Redo an INSERT.
 *
 * Strategy:
 *   1. If the buffer has a Full Page Image (FPI), restore it directly.
 *      The FPI contains the post-insert page state — no per-byte redo needed.
 *   2. If no FPI, apply the insert to the page's serialized content.
 *
 * When buffer registration is active (XLogRegisterBuffer was called
 * during the original insert):
 *   - PG automatically restores FPIs for us
 *   - For non-FPI pages, we get a locked buffer via XLogReadBufferForRedo
 *   - We apply the insert to that page, set LSN, and mark dirty
 *
 * Without buffer registration (current mode — in-memory tree):
 *   - We log the operation for post-recovery NodeMap/tree rebuilding
 */
static void
blinkhash_redo_insert_impl(XLogReaderState *record)
{
    XLogRecPtr  lsn = record->ReadRecPtr;
    char       *rec_data = XLogRecGetData(record);
    xl_blinkhash_insert *xlrec = (xl_blinkhash_insert *) rec_data;
    char       *key_data = rec_data + sizeof(xl_blinkhash_insert);
    uint64      value;

    memcpy(&value, key_data + xlrec->key_len, sizeof(uint64));

    /*
     * Buffer-level redo path:
     *
     * If a buffer was registered (block 0), PG handles FPI
     * restoration automatically.  We only need to apply the
     * per-byte redo when BLK_NEEDS_REDO is returned.
     */
    if (XLogRecHasBlockRef(record, 0))
    {
        Buffer      buf;
        XLogRedoAction action = XLogReadBufferForRedo(record, 0, &buf);

        if (action == BLK_NEEDS_REDO)
        {
            Page page = BufferGetPage(buf);

            /*
             * Apply insert: add the key/value entry to the page's
             * serialized leaf content.  For hash leaves, locate the
             * target bucket and insert.  For btree leaves, insert
             * into the sorted array.
             *
             * The page content is in the standalone serialization
             * format (payload bytes from bh_page.cpp).
             */
            /* TODO: per-byte redo of insert into page payload */

            PageSetLSN(page, lsn);
            MarkBufferDirty(buf);
        }

        if (BufferIsValid(buf))
            UnlockReleaseBuffer(buf);
    }
    else
    {
        /* No buffer registered — in-memory tree mode.
         * Post-recovery tree reconstruction will handle this. */
        elog(DEBUG1, "blinkhash redo INSERT: node_id=%llu, key_len=%u",
             (unsigned long long)xlrec->node_id, xlrec->key_len);
    }
}


static void
blinkhash_redo_delete_impl(XLogReaderState *record)
{
    XLogRecPtr  lsn = record->ReadRecPtr;
    char       *rec_data = XLogRecGetData(record);
    xl_blinkhash_delete *xlrec = (xl_blinkhash_delete *) rec_data;

    if (XLogRecHasBlockRef(record, 0))
    {
        Buffer      buf;
        XLogRedoAction action = XLogReadBufferForRedo(record, 0, &buf);

        if (action == BLK_NEEDS_REDO)
        {
            Page page = BufferGetPage(buf);
            /* TODO: per-byte redo of delete from page payload */
            PageSetLSN(page, lsn);
            MarkBufferDirty(buf);
        }

        if (BufferIsValid(buf))
            UnlockReleaseBuffer(buf);
    }
    else
    {
        elog(DEBUG1, "blinkhash redo DELETE: node_id=%llu, key_len=%u",
             (unsigned long long)xlrec->node_id, xlrec->key_len);
    }
}

/*
 * Redo an UPDATE.
 */
static void
blinkhash_redo_update_impl(XLogReaderState *record)
{
    XLogRecPtr  lsn = record->ReadRecPtr;
    char       *rec_data = XLogRecGetData(record);
    xl_blinkhash_update *xlrec = (xl_blinkhash_update *) rec_data;
    char       *key_data = rec_data + sizeof(xl_blinkhash_update);
    uint64      new_value;

    memcpy(&new_value, key_data + xlrec->key_len, sizeof(uint64));

    if (XLogRecHasBlockRef(record, 0))
    {
        Buffer      buf;
        XLogRedoAction action = XLogReadBufferForRedo(record, 0, &buf);

        if (action == BLK_NEEDS_REDO)
        {
            Page page = BufferGetPage(buf);
            /* TODO: per-byte redo of update in page payload */
            PageSetLSN(page, lsn);
            MarkBufferDirty(buf);
        }

        if (BufferIsValid(buf))
            UnlockReleaseBuffer(buf);
    }
    else
    {
        elog(DEBUG1, "blinkhash redo UPDATE: node_id=%llu, key_len=%u",
             (unsigned long long)xlrec->node_id, xlrec->key_len);
    }
}

/*
 * Redo a SPLIT_LEAF.
 *
 * This is the most complex redo handler because it must:
 *   1. Read the old leaf page (may have FPI).
 *   2. Create the new leaf page.
 *   3. Migrate entries according to the split key.
 *   4. Update the parent internal node (handled by SPLIT_INTERNAL record).
 *
 * For robustness, the WAL record includes all migrated entries,
 * so redo can reconstruct both pages from scratch.
 */
static void
blinkhash_redo_split_leaf_impl(XLogReaderState *record)
{
    XLogRecPtr  lsn = record->ReadRecPtr;
    char       *rec_data = XLogRecGetData(record);
    xl_blinkhash_split_leaf *xlrec = (xl_blinkhash_split_leaf *) rec_data;

    /*
     * Block 0: old leaf page (may have FPI)
     * Block 1: new leaf page (always WILL_INIT — fresh page)
     */
    if (XLogRecHasBlockRef(record, 0))
    {
        Buffer      old_buf;
        XLogRedoAction action = XLogReadBufferForRedo(record, 0, &old_buf);

        if (action == BLK_NEEDS_REDO)
        {
            Page old_page = BufferGetPage(old_buf);
            BHPageOpaque opaque;
            RelFileLocator rlocator;
            ForkNumber forknum;
            BlockNumber new_blkno;

            /*
             * Remove migrated entries from old leaf,
             * update sibling pointer to new leaf.
             */
            opaque = bh_page_get_opaque(old_page);
            /* New leaf's block is recorded in block ref 1 */
            XLogRecGetBlockTag(record, 1, &rlocator, &forknum, &new_blkno);
            opaque->right_sibling = new_blkno;

            PageSetLSN(old_page, lsn);
            MarkBufferDirty(old_buf);
        }

        if (BufferIsValid(old_buf))
            UnlockReleaseBuffer(old_buf);
    }

    /* New leaf page: always WILL_INIT */
    if (XLogRecHasBlockRef(record, 1))
    {
        Buffer new_buf;
        Page new_page;

        new_buf = XLogInitBufferForRedo(record, 1);
        new_page = BufferGetPage(new_buf);

        /* Initialize the new leaf page */
        bh_page_init(new_page, xlrec->new_leaf_id, 1 /* hash leaf */,
                     0 /* level */, BH_PAGE_IS_LEAF | BH_PAGE_IS_HASH);

        /*
         * Insert migrated entries into the new leaf page.
         * Entries follow the split key in the WAL record.
         */
        /* TODO: deserialize and insert migrated entries */

        PageSetLSN(new_page, lsn);
        MarkBufferDirty(new_buf);
        UnlockReleaseBuffer(new_buf);
    }
    else
    {
        elog(DEBUG1, "blinkhash redo SPLIT_LEAF: old=%llu new=%llu, "
             "%u migrated entries",
             (unsigned long long)xlrec->old_leaf_id,
             (unsigned long long)xlrec->new_leaf_id,
             xlrec->num_migrated);
    }
}


static void
blinkhash_redo_split_internal_impl(XLogReaderState *record)
{
    XLogRecPtr  lsn = record->ReadRecPtr;
    char       *rec_data = XLogRecGetData(record);
    xl_blinkhash_split_internal *xlrec = (xl_blinkhash_split_internal *) rec_data;

    if (XLogRecHasBlockRef(record, 0))
    {
        Buffer      buf;
        XLogRedoAction action = XLogReadBufferForRedo(record, 0, &buf);

        if (action == BLK_NEEDS_REDO)
        {
            Page page = BufferGetPage(buf);

            /*
             * Insert the promoted key and new child pointer into
             * the internal node page.
             */
            /* TODO: per-byte redo of key insertion into inode page */

            PageSetLSN(page, lsn);
            MarkBufferDirty(buf);
        }

        if (BufferIsValid(buf))
            UnlockReleaseBuffer(buf);
    }
    else
    {
        elog(DEBUG1, "blinkhash redo SPLIT_INTERNAL: inode=%llu, new_child=%llu",
             (unsigned long long)xlrec->inode_id,
             (unsigned long long)xlrec->new_child_id);
    }
}

/*
 * Redo a CONVERT (hash leaf → btree leaves).
 *
 * Conversion creates multiple new btree leaf pages from one
 * hash leaf page.  The WAL record contains:
 *   - old_hash_leaf_id
 *   - array of new_leaf_ids
 *   - all entries (key/value pairs)
 *
 * Redo: allocate new pages, distribute entries, free old pages.
 */
static void
blinkhash_redo_convert_impl(XLogReaderState *record)
{
    XLogRecPtr  lsn = record->ReadRecPtr;
    char       *rec_data = XLogRecGetData(record);
    xl_blinkhash_convert *xlrec = (xl_blinkhash_convert *) rec_data;
    char *payload = rec_data + sizeof(xl_blinkhash_convert);

    /*
     * Block 0: old hash leaf (to be freed / marked deleted)
     * Blocks 1..N: new btree leaf pages (WILL_INIT)
     */
    if (XLogRecHasBlockRef(record, 0))
    {
        Buffer      old_buf;
        XLogRedoAction action = XLogReadBufferForRedo(record, 0, &old_buf);

        if (action == BLK_NEEDS_REDO)
        {
            Page old_page = BufferGetPage(old_buf);
            BHPageOpaque opaque;

            opaque = bh_page_get_opaque(old_page);

            /* Mark the old hash leaf as deleted */
            opaque->flags |= BH_PAGE_IS_DELETED;

            PageSetLSN(old_page, lsn);
            MarkBufferDirty(old_buf);
        }

        if (BufferIsValid(old_buf))
            UnlockReleaseBuffer(old_buf);
    }

    /* Initialize new btree leaf pages */
    {
        uint32 i;
        for (i = 0; i < xlrec->num_new_leaves; i++)
        {
            int blk_idx = i + 1;
            if (XLogRecHasBlockRef(record, blk_idx))
            {
                Buffer new_buf;
                Page new_page;
                uint64 *new_ids;

                new_buf = XLogInitBufferForRedo(record, blk_idx);
                new_page = BufferGetPage(new_buf);

                new_ids = (uint64 *)payload;
                bh_page_init(new_page, new_ids[i], 2 /* btree leaf */,
                             0, BH_PAGE_IS_LEAF | BH_PAGE_IS_BTREE);

                /* TODO: insert distributed entries into each new btree leaf */

                PageSetLSN(new_page, lsn);
                MarkBufferDirty(new_buf);
                UnlockReleaseBuffer(new_buf);
            }
        }
    }

    elog(DEBUG1, "blinkhash redo CONVERT: old_hash=%llu, %u new leaves, %u entries",
         (unsigned long long)xlrec->old_hash_leaf_id,
         xlrec->num_new_leaves, xlrec->total_entries);
}

/*
 * Redo a NEW_ROOT.
 *
 * A new root was created due to a root split.
 * The WAL record contains:
 *   - new_root_id, left_child_id, right_child_id
 *   - split_key, level
 *
 * Redo: allocate root page, insert split key pointing to
 * left and right children.
 */
static void
blinkhash_redo_new_root_impl(XLogReaderState *record)
{
    XLogRecPtr  lsn = record->ReadRecPtr;
    char       *rec_data = XLogRecGetData(record);
    xl_blinkhash_new_root *xlrec = (xl_blinkhash_new_root *) rec_data;

    /*
     * Block 0: new root page (WILL_INIT)
     */
    if (XLogRecHasBlockRef(record, 0))
    {
        Buffer new_buf;
        Page new_page;
        BHPageOpaque opaque;

        new_buf = XLogInitBufferForRedo(record, 0);
        new_page = BufferGetPage(new_buf);

        bh_page_init(new_page, xlrec->new_root_id,
                     0 /* inode */, xlrec->level, 0);

        /*
         * Write the initial root content:
         *   leftmost_ptr = left_child
         *   entries[0] = { split_key, right_child }
         */
        opaque = bh_page_get_opaque(new_page);
        opaque->node_id = xlrec->new_root_id;
        opaque->level   = xlrec->level;

        /* TODO: serialize root inode content into the page payload */

        PageSetLSN(new_page, lsn);
        MarkBufferDirty(new_buf);
        UnlockReleaseBuffer(new_buf);
    }
    else
    {
        elog(DEBUG1, "blinkhash redo NEW_ROOT: root=%llu, left=%llu, right=%llu, level=%u",
             (unsigned long long)xlrec->new_root_id,
             (unsigned long long)xlrec->left_child_id,
             (unsigned long long)xlrec->right_child_id,
             xlrec->level);
    }
}

/*
 * Redo a STABILIZE (lazy bucket migration).
 *
 * Stabilize migrates entries between buckets within a hash leaf.
 * The WAL record contains:
 *   - leaf_id, bucket_idx, migrated entries
 *
 * Redo: apply the migration to the leaf page.
 */
static void
blinkhash_redo_stabilize_impl(XLogReaderState *record)
{
    XLogRecPtr  lsn = record->ReadRecPtr;
    char       *rec_data = XLogRecGetData(record);
    xl_blinkhash_stabilize *xlrec = (xl_blinkhash_stabilize *) rec_data;

    if (XLogRecHasBlockRef(record, 0))
    {
        Buffer      buf;
        XLogRedoAction action = XLogReadBufferForRedo(record, 0, &buf);

        if (action == BLK_NEEDS_REDO)
        {
            Page page = BufferGetPage(buf);

            /*
             * Apply bucket migration: move entries from source
             * bucket to target bucket within the hash leaf.
             */
            /* TODO: per-byte redo of bucket migration */

            PageSetLSN(page, lsn);
            MarkBufferDirty(buf);
        }

        if (BufferIsValid(buf))
            UnlockReleaseBuffer(buf);
    }
    else
    {
        elog(DEBUG1, "blinkhash redo STABILIZE: leaf=%llu, bucket=%u, %u migrated",
             (unsigned long long)xlrec->leaf_id,
             xlrec->bucket_idx, xlrec->num_migrated);
    }
}

/* ═══════════════════════════════════════════════════════════════════
 *  Convenience wrappers (declared in blinkhash_wal.h)
 *
 *  These are thin wrappers around the static _impl functions,
 *  exposed for direct testing or external access if needed.
 * ═══════════════════════════════════════════════════════════════════ */

void blinkhash_redo_insert(XLogReaderState *record) {
    blinkhash_redo_insert_impl(record);
}

void blinkhash_redo_delete(XLogReaderState *record) {
    blinkhash_redo_delete_impl(record);
}

void blinkhash_redo_split_leaf(XLogReaderState *record) {
    blinkhash_redo_split_leaf_impl(record);
}

void blinkhash_redo_split_internal(XLogReaderState *record) {
    blinkhash_redo_split_internal_impl(record);
}

void blinkhash_redo_convert(XLogReaderState *record) {
    blinkhash_redo_convert_impl(record);
}

void blinkhash_redo_new_root(XLogReaderState *record) {
    blinkhash_redo_new_root_impl(record);
}

void blinkhash_redo_stabilize(XLogReaderState *record) {
    blinkhash_redo_stabilize_impl(record);
}