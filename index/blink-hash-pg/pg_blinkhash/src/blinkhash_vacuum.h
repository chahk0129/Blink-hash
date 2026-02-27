/*
 * blinkhash_vacuum.h — VACUUM support for B^link-hash
 */
#ifndef BLINKHASH_VACUUM_H
#define BLINKHASH_VACUUM_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "access/amapi.h"
#include "commands/vacuum.h"

/*
 * Walk all tree leaves and call the VACUUM callback on each TID.
 * Remove entries where the callback returns true (dead tuple).
 *
 * Implementation strategy:
 *   1. Get a ThreadInfo for epoch-based reclamation.
 *   2. Do a full range_lookup from min_key to max_key.
 *   3. For each returned value, unpack TID and check via callback.
 *   4. For dead TIDs, call btree_t::remove().
 *   5. Tally stats.
 */

/* Callbacks declared in blinkhash_am.h */

#ifdef __cplusplus
}
#endif

#endif /* BLINKHASH_VACUUM_H */
