

#include "blinkhash_am.h"
#include "blinkhash_core.h"
#include "blinkhash_utils.h"

#include "postgres.h"
#include "access/genam.h"
#include "commands/vacuum.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * Wrapper that bridges the PG IndexBulkDeleteCallback to
 * the bh_delete_callback_fn expected by the C++ bridge.
 *
 * PG callback signature:  bool (*)(ItemPointer, void *)
 * Bridge signature:       int  (*)(uint64_t value, void *)
 *
 * We unpack the uint64_t value into an ItemPointerData and
 * forward to PG's callback.
 */
typedef struct BHBulkDeleteState {
    IndexBulkDeleteCallback callback;
    void                   *callback_state;
} BHBulkDeleteState;

static int
bh_vacuum_callback(uint64_t value, void *state)
{
    BHBulkDeleteState *bds = (BHBulkDeleteState *) state;
    ItemPointerData   tid;

    /* Unpack TID: block = value >> 16, offset = value & 0xFFFF */
    ItemPointerSetBlockNumber(&tid, (BlockNumber)(value >> 16));
    ItemPointerSetOffsetNumber(&tid, (OffsetNumber)(value & 0xFFFF));

    return bds->callback(&tid, bds->callback_state) ? 1 : 0;
}

/*
 * ambulkdelete — Walk all entries and remove dead tuples.
 *
 * Strategy:
 *   Descend to the leftmost leaf via leftmost_ptr, then walk the
 *   sibling chain.  For each entry, unpack the value as a TID and
 *   ask PG's callback whether it's dead.  Dead entries are collected
 *   and removed in a second pass to avoid mutating the structure
 *   while iterating.
 */
IndexBulkDeleteResult *
blinkhash_ambulkdelete(IndexVacuumInfo *info,
                       IndexBulkDeleteResult *stats,
                       IndexBulkDeleteCallback callback,
                       void *callback_state)
{
    Relation    indexRelation = info->index;
    BHIndexState *state;
    BHBulkDeleteState bds;

    if (stats == NULL)
        stats = (IndexBulkDeleteResult *)
            palloc0(sizeof(IndexBulkDeleteResult));

    state = bh_get_index_state(indexRelation);
    if (state == NULL || state->tree == NULL)
    {
        /* No tree materialised yet — nothing to delete */
        return stats;
    }

    bds.callback       = callback;
    bds.callback_state = callback_state;

    int64_t deleted = bh_bulk_delete(state->tree,
                                     state->key_class,
                                     bh_vacuum_callback,
                                     &bds,
                                     state->thread_info);

    stats->tuples_removed  += (double) deleted;
    stats->num_pages        = 0;     /* in-memory; no pages on disk */
    stats->pages_deleted    = 0;
    stats->pages_free       = 0;

    elog(DEBUG1, "blinkhash ambulkdelete: removed %lld tuples",
         (long long) deleted);

    return stats;
}

/*
 * amvacuumcleanup — Post-VACUUM cleanup.
 */
IndexBulkDeleteResult *
blinkhash_amvacuumcleanup(IndexVacuumInfo *info,
                          IndexBulkDeleteResult *stats)
{
    if (stats == NULL)
        stats = (IndexBulkDeleteResult *)
            palloc0(sizeof(IndexBulkDeleteResult));

    /* Estimate number of index entries */
    BHIndexState *state = bh_get_index_state(info->index);
    if (state && state->tree)
    {
        /* Use tree utilization as a rough estimate */
        stats->num_index_tuples = info->num_heap_tuples;
    }

    return stats;
}