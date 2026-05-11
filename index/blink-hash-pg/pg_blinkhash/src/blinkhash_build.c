#include "shared_mmap.h"


#include "blinkhash_am.h"
#include "blinkhash_build.h"
#include "blinkhash_core.h"
#include "blinkhash_utils.h"

#include "postgres.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"


void
blinkhash_ambuildempty(Relation indexRelation)
{
    /* Nothing to do — the tree is created on first insert. */
}

/*
 * Callback for table_index_build_scan: called once per heap tuple.
 */
void
blinkhash_build_callback(Relation indexRelation,
                         ItemPointer tid,
                         Datum *values,
                         bool *isnull,
                         bool tupleIsAlive,
                         void *state)
{
    BHBuildState *bs = (BHBuildState *) state;

    /* Skip NULL keys */
    if (isnull[0])
        return;

    uint64 packed_tid = bh_tid_to_value(tid);

    if (bs->key_class == 'i')
    {
        bool ok;
        uint64 key = bh_datum_to_key64(values[0], bs->key_typid, &ok);
        if (ok)
        {
            bh_insert(bs->tree, 'i', &key, sizeof(key),
                      packed_tid, bs->thread_info);
            bs->tuples_inserted++;
        }
    }
    else
    {
        char key_buf[32];
        bh_datum_to_string_key(values[0], bs->key_typid, key_buf, 32);
        bh_insert(bs->tree, 's', key_buf, 32,
                  packed_tid, bs->thread_info);
        bs->tuples_inserted++;
    }
}

IndexBuildResult *
blinkhash_ambuild(Relation heapRelation,
                  Relation indexRelation,
                  IndexInfo *indexInfo)
{
    IndexBuildResult *result;

    /* Determine key type from the first indexed column */
    Oid key_typid = TupleDescAttr(
        RelationGetDescr(indexRelation), 0)->atttypid;
    char key_class = bh_classify_type(key_typid);

    /* Create the tree */
    bh_shared_mmap_init(1024ULL * 1024 * 1024); /* 1 GB Arena */ 
    
    bh_global_lock();
    void *tree = bh_get_global_tree();
    if (tree == NULL) {
        tree = bh_tree_create(key_class);
        bh_set_global_tree(tree);
    }
    void *ti = bh_get_thread_info(tree, key_class);
    bh_global_unlock();

    BHBuildState bs;
    bs.tree           = tree;
    bs.thread_info    = ti;
    bs.tuples_inserted = 0;
    bs.key_class      = key_class;
    bs.key_typid      = key_typid;

    /* Scan the heap and build the index */
    double reltuples = table_index_build_scan(heapRelation,
                                              indexRelation,
                                              indexInfo,
                                              true,     /* allow_sync */
                                              false,    /* progress */
                                              blinkhash_build_callback,
                                              &bs,
                                              NULL);    /* scan */
    #ifdef BH_USE_PG_WAL

    if (RelationNeedsWAL(indexRelation))
    {
        /* Mark all index pages as needing full page write on next modify */
        // This is automatic with REGBUF_STANDARD
    }
    #endif

    /*
     * Store the tree handle in rd_amcache for later use by
     * insert/scan/vacuum.
     */
    BHIndexState *state = (BHIndexState *)
        MemoryContextAllocZero(indexRelation->rd_indexcxt,
                               sizeof(BHIndexState));
    state->tree         = tree;
    state->thread_info  = ti;
    state->key_class    = key_class;
    state->key_typid    = key_typid;
    indexRelation->rd_amcache = state;

    /* Return statistics */
    result = (IndexBuildResult *) palloc0(sizeof(IndexBuildResult));
    result->heap_tuples  = reltuples;
    result->index_tuples = bs.tuples_inserted;

    return result;
}

/*
 * bh_lazy_rebuild --- Rebuild the in-memory tree from the heap.
 *
 * Called when a backend opens the index for the first time and finds
 * rd_amcache == NULL (i.e. the tree was built by a different backend).
 * Scans the heap with the active snapshot, inserts every live tuple
 * into a fresh tree, then stores it in rd_amcache for subsequent use.
 *
 * This is O(N) but amortised: it happens at most once per backend
 * per index per relcache lifetime.
 */
BHIndexState *
bh_lazy_rebuild(Relation indexRelation)
{
    Oid        heapOid;
    Relation   heapRel;
    TupleTableSlot *slot;
    TableScanDesc   hscan;
    BHIndexState   *state;
    Oid        key_typid;
    char       key_class;
    void      *tree;
    void      *ti;
    int        attrno;

    /* Already populated? */
    if (indexRelation->rd_amcache != NULL)
        return (BHIndexState *) indexRelation->rd_amcache;

    /* Determine key type from the first indexed column */
    key_typid = TupleDescAttr(
        RelationGetDescr(indexRelation), 0)->atttypid;
    key_class = bh_classify_type(key_typid);

    /* Heap attribute number for the indexed column (1-based) */
    attrno = indexRelation->rd_index->indkey.values[0];

    bh_shared_mmap_init(1024ULL * 1024 * 1024); // 1 GB setup
    bh_global_lock();
    tree = bh_get_global_tree();

    if (tree != NULL) {
        ti = bh_get_thread_info(tree, key_class);
        bh_global_unlock();

        state = (BHIndexState *)
            MemoryContextAllocZero(indexRelation->rd_indexcxt,
                                   sizeof(BHIndexState));
        state->tree        = tree;
        state->thread_info = ti;
        state->key_class   = key_class;
        state->key_typid   = key_typid;
        indexRelation->rd_amcache = state;
        return state;
    }

    /* Create an empty tree */
    tree = bh_tree_create(key_class);
    ti   = bh_get_thread_info(tree, key_class);
    
    bh_set_global_tree(tree);
    bh_global_unlock();

    /* Open the parent heap */
    heapOid = IndexGetRelation(RelationGetRelid(indexRelation), false);
    heapRel = table_open(heapOid, AccessShareLock);

    /* Scan heap with the active snapshot */
    slot  = table_slot_create(heapRel, NULL);
    hscan = table_beginscan(heapRel, GetActiveSnapshot(), 0, NULL);

    while (table_scan_getnextslot(hscan, ForwardScanDirection, slot))
    {
        Datum  val;
        bool   isnull;
        uint64 packed_tid;

        val = slot_getattr(slot, attrno, &isnull);
        if (isnull)
            continue;

        packed_tid = bh_tid_to_value(&slot->tts_tid);

        if (key_class == 'i')
        {
            bool ok;
            uint64 key = bh_datum_to_key64(val, key_typid, &ok);
            if (ok)
                bh_insert(tree, 'i', &key, sizeof(key),
                          packed_tid, ti);
        }
        else
        {
            char key_buf[32];
            bh_datum_to_string_key(val, key_typid, key_buf, 32);
            bh_insert(tree, 's', key_buf, 32,
                      packed_tid, ti);
        }
    }

    table_endscan(hscan);
    ExecDropSingleTupleTableSlot(slot);
    table_close(heapRel, AccessShareLock);

    /* Store in rd_amcache */
    state = (BHIndexState *)
        MemoryContextAllocZero(indexRelation->rd_indexcxt,
                               sizeof(BHIndexState));
    state->tree        = tree;
    state->thread_info = ti;
    state->key_class   = key_class;
    state->key_typid   = key_typid;
    indexRelation->rd_amcache = state;

    return state;
}