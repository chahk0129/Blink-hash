/*
 * blinkhash_build.h — Index build callbacks
 *
 * ambuild:      full build from a populated heap
 * ambuildempty: create an empty index structure
 */
#ifndef BLINKHASH_BUILD_H
#define BLINKHASH_BUILD_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "access/amapi.h"

/*
 * Callback passed to table_index_build_scan().
 * Converts each heap tuple into (key, TID) and inserts into the tree.
 */
void blinkhash_build_callback(Relation indexRelation,
                              ItemPointer tid,
                              Datum *values,
                              bool *isnull,
                              bool tupleIsAlive,
                              void *state);

/*
 * State carried through the build.
 */
typedef struct BHBuildState {
    void   *tree;           /* btree_t<K,V>*  (type-erased)        */
    void   *thread_info;    /* ThreadInfo*     (type-erased)        */
    int64   tuples_inserted;
    char    key_class;      /* 'i' = int, 's' = string             */
    Oid     key_typid;
} BHBuildState;

/*
 * Lazy rebuild: populate the in-memory tree from the heap on first
 * access in a new backend.  Returns the BHIndexState stored in
 * rd_amcache, or NULL if the heap is not accessible.
 */
struct BHIndexState;  /* forward declaration (defined in blinkhash_core.h) */
struct BHIndexState *bh_lazy_rebuild(Relation indexRelation);

/* Declared in blinkhash_am.h — implementations in blinkhash_build.c */

#ifdef __cplusplus
}
#endif

#endif /* BLINKHASH_BUILD_H */
