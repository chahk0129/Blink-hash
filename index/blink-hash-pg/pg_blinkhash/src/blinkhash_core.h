/*
 * PostgreSQL AM callbacks are implemented in C.  They call through
 * this thin C API which internally casts to the C++ btree_t and
 * ThreadInfo types.
 *
 * All tree handles are type-erased as void*.  The key_class parameter
 * ('i' for integer, 's' for string) selects the correct template
 * specialization.
 */
#ifndef BLINKHASH_CORE_H
#define BLINKHASH_CORE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>


void* bh_tree_create(char key_class);


void bh_tree_destroy(void* tree, char key_class);

void* bh_get_thread_info(void* tree, char key_class);


void bh_insert(void* tree, char key_class,
               const void* key_data, size_t key_len,
               uint64_t value,
               void* thread_info);

void bh_insert_with_meta(void* tree, char key_class,
                         const void* key_data, size_t key_len,
                         uint64_t value,
                         void* thread_info,
                         uint64_t* node_id_out,
                         uint32_t* bucket_idx_out);


uint64_t bh_lookup(void* tree, char key_class,
                   const void* key_data, size_t key_len,
                   void* thread_info,
                   int* found);


int bh_update(void* tree, char key_class,
              const void* key_data, size_t key_len,
              uint64_t new_value,
              void* thread_info);


int bh_remove(void* tree, char key_class,
              const void* key_data, size_t key_len,
              void* thread_info);


int bh_range_lookup(void* tree, char key_class,
                    const void* min_key_data, size_t min_key_len,
                    int range,
                    uint64_t* buf,
                    void* thread_info);


int      bh_height(void* tree, char key_class);
double   bh_utilization(void* tree, char key_class);
void     bh_print(void* tree, char key_class);

void     bh_free_thread_info(void* thread_info);

/* ─── Node ↔ page serialization (C bridge to C++ templates) ───── */

/*
 * Node type codes matching PageNodeType in bh_page.h
 */
#define BH_NODE_TYPE_INODE       1
#define BH_NODE_TYPE_LNODE_HASH  2
#define BH_NODE_TYPE_LNODE_BTREE 3

/*
 * Serialize a tree node into a buffer of 8KB standalone pages.
 * Returns the number of pages written (1 for inode/btree leaf,
 * up to 32 for hash leaf).
 *
 * @param node        Pointer to inode_t / lnode_btree_t / lnode_hash_t
 * @param key_class   'i' or 's'
 * @param node_type   BH_NODE_TYPE_INODE / _LNODE_HASH / _LNODE_BTREE
 * @param page_buf    Output buffer (must be at least max_pages * 8192 bytes)
 * @param max_pages   Capacity of page_buf
 */
int bh_serialize_node(const void *node, char key_class,
                      int node_type, void *page_buf, int max_pages);

/*
 * Deserialize a standalone page buffer back into a tree node.
 * Returns a heap-allocated node (caller takes ownership).
 */
void *bh_deserialize_node(const void *page_buf, int num_pages,
                          char key_class, int node_type);

/*
 * Callback for bulk-delete: receives the packed TID value for each entry.
 * Return non-zero (true) if the entry should be deleted.
 */
typedef int (*bh_delete_callback_fn)(uint64_t value, void *cb_state);

/*
 * Walk every leaf entry.  For each entry, call `callback(value, cb_state)`.
 * If the callback returns true, remove that key from the tree.
 * Returns the number of entries deleted.
 */
int64_t  bh_bulk_delete(void* tree, char key_class,
                        bh_delete_callback_fn callback,
                        void *cb_state,
                        void* thread_info);

#ifdef __cplusplus
}
#endif

/*
 * Per-index state, stored in indexRelation->rd_amcache.
 * Created on first access, freed by PG relcache invalidation.
 * Only available when building inside PostgreSQL.
 */
#ifdef PG_MODULE_MAGIC
#include "utils/rel.h"
typedef struct BHIndexState {
    void   *tree;
    void   *thread_info;
    char    key_class;      /* 'i' or 's'                        */
    Oid     key_typid;      /* OID of the indexed column type    */
} BHIndexState;

static inline BHIndexState*
bh_get_index_state(Relation indexRelation)
{
    if (indexRelation->rd_amcache == NULL)
        return NULL;
    return (BHIndexState *) indexRelation->rd_amcache;
}
#endif /* PG_MODULE_MAGIC */

#endif /* BLINKHASH_CORE_H */
