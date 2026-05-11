/*
 * blinkhash_am.h — All PostgreSQL Index AM callback declarations
 *
 * Each callback corresponds to a slot in IndexAmRoutine.
 * The implementations live in their own .c files (blinkhash_build.c,
 * blinkhash_insert.c, blinkhash_scan.c, etc.).
 *
 * Implements: Phase 7.2.2–7.2.13 of IMPLEMENTATION_SEQUENCE.md
 */
#ifndef BLINKHASH_AM_H
#define BLINKHASH_AM_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "access/amapi.h"
#include "access/genam.h"
#include "nodes/execnodes.h"
#include "nodes/pathnodes.h"
#include "utils/selfuncs.h"

/* ─── Build ──────────────────────────────────────────────────────── */

/*
 * Build a new B^link-hash index from a populated heap relation.
 * Scans the heap, converts each tuple to (key, TID), and inserts
 * into the in-memory B^link-hash tree.
 */
IndexBuildResult* blinkhash_ambuild(Relation heapRelation,
                                    Relation indexRelation,
                                    IndexInfo *indexInfo);

/*
 * Build an empty index (no heap data).
 * Called for UNLOGGED tables and during REINDEX with no data.
 */
void blinkhash_ambuildempty(Relation indexRelation);

/* ─── Insert ─────────────────────────────────────────────────────── */

/*
 * Insert a single entry into the index.
 * Converts the Datum array + ItemPointer into the tree's native
 * key/value types and calls btree_t::insert().
 */
bool blinkhash_aminsert(Relation indexRelation,
                        Datum *values,
                        bool *isnull,
                        ItemPointer heap_tid,
                        Relation heapRelation,
                        IndexUniqueCheck checkUnique,
                        bool indexUnchanged,
                        IndexInfo *indexInfo);

/* ─── Scan ───────────────────────────────────────────────────────── */

/*
 * Begin an index scan.
 * Allocates and returns an IndexScanDesc with B^link-hash opaque state.
 */
IndexScanDesc blinkhash_ambeginscan(Relation indexRelation,
                                    int nkeys, int norderbys);

/*
 * (Re)start a scan with new scan keys.
 */
void blinkhash_amrescan(IndexScanDesc scan,
                        ScanKey scankey, int nscankeys,
                        ScanKey orderbys, int norderbys);

/*
 * Return the next matching tuple from the scan.
 * For point lookups: uses btree_t::lookup().
 * For range scans:   uses btree_t::range_lookup() with a cursor.
 */
bool blinkhash_amgettuple(IndexScanDesc scan, ScanDirection direction);

/*
 * Return a bitmap of all matching TIDs.
 */
int64 blinkhash_amgetbitmap(IndexScanDesc scan, TIDBitmap *tbm);

/*
 * End the index scan and free opaque state.
 */
void blinkhash_amendscan(IndexScanDesc scan);

/* ─── Scan position save/restore (for merge join) ────────────────── */

void blinkhash_ammarkpos(IndexScanDesc scan);
void blinkhash_amrestrpos(IndexScanDesc scan);

/* ─── Cost estimation ────────────────────────────────────────────── */

/*
 * Supply planner with cost estimates for this index.
 * For hash leaves: effectively O(1) = 1 page.
 * For btree leaves: log(N) height traversal.
 */
void blinkhash_amcostestimate(PlannerInfo *root,
                              IndexPath *path,
                              double loop_count,
                              Cost *indexStartupCost,
                              Cost *indexTotalCost,
                              Selectivity *indexSelectivity,
                              double *indexCorrelation,
                              double *indexPages);

/* ─── Validate ───────────────────────────────────────────────────── */

/*
 * Validate that the given operator class is supported.
 * B^link-hash supports: int4_ops, int8_ops, float8_ops, text_ops.
 */
bool blinkhash_amvalidate(Oid opclassoid);

/* ─── VACUUM ─────────────────────────────────────────────────────── */

/*
 * Bulk-delete dead tuples from the index.
 * Walks all leaf entries, calls the callback for each TID,
 * and removes those marked dead.
 */
IndexBulkDeleteResult* blinkhash_ambulkdelete(IndexVacuumInfo *info,
                                              IndexBulkDeleteResult *stats,
                                              IndexBulkDeleteCallback callback,
                                              void *callback_state);

/*
 * Post-VACUUM cleanup.
 * Updates statistics (number of entries, number of pages, height).
 */
IndexBulkDeleteResult* blinkhash_amvacuumcleanup(IndexVacuumInfo *info,
                                                 IndexBulkDeleteResult *stats);

/* ─── Index-only scans ───────────────────────────────────────────── */

/*
 * Can the index return the indexed column directly (without heap fetch)?
 * Returns true for fixed-size types stored in the key.
 */
bool blinkhash_amcanreturn(Relation indexRelation, int attno);

#ifdef __cplusplus
}
#endif

#endif /* BLINKHASH_AM_H */
