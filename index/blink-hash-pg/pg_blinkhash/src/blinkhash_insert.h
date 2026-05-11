/*
 * blinkhash_insert.h — Single-row insert callback
 */
#ifndef BLINKHASH_INSERT_H
#define BLINKHASH_INSERT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

/* blinkhash_aminsert() is declared in blinkhash_am.h */

/*
 * Insert a (key, TID) pair into the B^link-hash tree.
 *
 * This is a thin C wrapper that:
 *   1. Converts the Datum to the native key type.
 *   2. Packs the ItemPointer into a uint64 value.
 *   3. Calls btree_t::insert().
 *   4. Emits a WAL record.
 */

#ifdef __cplusplus
}
#endif

#endif /* BLINKHASH_INSERT_H */
