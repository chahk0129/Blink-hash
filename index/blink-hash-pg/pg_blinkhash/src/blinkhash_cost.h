/*
 * blinkhash_cost.h — Cost estimation for the query planner
 */
#ifndef BLINKHASH_COST_H
#define BLINKHASH_COST_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

/*
 * B^link-hash cost model:
 *
 *   Point lookup:
 *     startup_cost  = 0
 *     total_cost    = hash_compute_cost + 1 bucket_read
 *     selectivity   = 1 / ndistinct
 *     correlation   = 0 (hash node has no order)
 *
 *   Range scan (on btree-converted leaves):
 *     startup_cost  = height * cpu_operator_cost
 *     total_cost    = startup + range_fraction * num_leaves
 *     selectivity   = (hi - lo) / (max - min)
 *     correlation   = 1.0 (btree leaves are sorted)
 *
 *   The planner uses these to decide between B^link-hash, btree,
 *   hash index, or sequential scan.
 */

/* blinkhash_amcostestimate() declared in blinkhash_am.h */

#ifdef __cplusplus
}
#endif

#endif /* BLINKHASH_COST_H */
