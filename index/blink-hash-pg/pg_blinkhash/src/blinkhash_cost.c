

#include "blinkhash_am.h"
#include "blinkhash_utils.h"

#include "postgres.h"
#include "access/amapi.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"

/*
 * B^link-hash cost model:
 *
 *   The tree lives entirely in process memory — PostgreSQL sees 0
 *   physical pages on disk.  We must synthesize a page count so
 *   genericcostestimate() produces sane selectivity / row estimates,
 *   then override the I/O costs to reflect in-memory access.
 *
 *   Point lookup:  O(1) hash probe + O(1) bucket scan  →  near-zero
 *   Range scan:    O(log N) descent + O(range) leaf scan, all in RAM
 */
void
blinkhash_amcostestimate(PlannerInfo *root,
                         IndexPath *path,
                         double loop_count,
                         Cost *indexStartupCost,
                         Cost *indexTotalCost,
                         Selectivity *indexSelectivity,
                         double *indexCorrelation,
                         double *indexPages)
{
    IndexOptInfo *index = path->indexinfo;
    GenericCosts  costs;
    double        synthPages;
    bool          is_equality_only;
    ListCell     *lc;

    MemSet(&costs, 0, sizeof(costs));

    /*
     * Synthesize a page count (~100 tuples per virtual 8 KiB page)
     * so genericcostestimate doesn't divide-by-zero or collapse.
     */
    synthPages = Max(1.0, ceil(Max(index->tuples, 1.0) / 100.0));
    costs.numIndexPages = synthPages;

    /* Use the generic estimator for selectivity and row estimates. */
    genericcostestimate(root, path, loop_count, &costs);

    /*
     * Classify: equality-only (point lookup) vs range scan.
     * Check each IndexClause's operator against the opfamily to get
     * the btree strategy number.
     */
    is_equality_only = (list_length(path->indexclauses) > 0);
    foreach(lc, path->indexclauses)
    {
        IndexClause  *ic = (IndexClause *) lfirst(lc);
        RestrictInfo *ri = ic->rinfo;

        if (ri && IsA(ri->clause, OpExpr))
        {
            Oid opno     = ((OpExpr *) ri->clause)->opno;
            Oid opfamily = index->opfamily[ic->indexcol];
            int strategy = get_op_opfamily_strategy(opno, opfamily);

            if (strategy != BTEqualStrategyNumber)
            {
                is_equality_only = false;
                break;
            }
        }
        else
        {
            is_equality_only = false;
            break;
        }
    }

    if (is_equality_only)
    {
        /*
         * Hash probe: essentially 0 I/O, ~2 operator evaluations.
         * Make this dramatically cheaper than btree's O(log N) descent.
         */
        *indexStartupCost = 0;
        *indexTotalCost   = cpu_operator_cost * 2.0;
        *indexCorrelation = 0.0;   /* hash order ≠ heap order */
    }
    else
    {
        /*
         * Range scan over in-memory sorted leaves.
         * No disk I/O for the index itself — only CPU per tuple.
         * The 0.4 multiplier makes this comfortably cheaper than a
         * btree range scan (which pays real page-fetch I/O).
         */
        *indexStartupCost = 0;
        *indexTotalCost   = costs.numIndexTuples * cpu_index_tuple_cost;
        *indexCorrelation = 1.0;   /* monotonic keys → sequential heap */
    }

    *indexSelectivity = costs.indexSelectivity;
    *indexPages       = synthPages;
}