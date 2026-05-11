/*
 * blinkhash_am.c — Miscellaneous AM callbacks
 *
 * Phase 7.2.12
 */

#include "blinkhash_am.h"

#include "postgres.h"
#include "utils/rel.h"

/*
 * amcanreturn — Can we do index-only scans?
 *
 * For now, return false.  Index-only scans require the AM to
 * reconstruct the indexed key from the index alone, which we
 * support (values are stored in leaves), but enabling this
 * requires amreturntid + visibility map integration.
 */
bool
blinkhash_amcanreturn(Relation indexRelation, int attno)
{
    return false;
}