/*
 * blinkhash_core.h — extern "C" bridge between the C++ B^link-hash
 *                    engine and the PostgreSQL C callback layer
 *
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

/* ─── Tree lifecycle ─────────────────────────────────────────────── */

/*
 * Create a new B^link-hash tree.
 * key_class: 'i' → btree_t<uint64_t, uint64_t>
 *            's' → btree_t<GenericKey<32>, uint64_t>
 * Returns an opaque handle (void*).
 */
void* bh_tree_create(char key_class);

/*
 * Destroy a tree and free all memory.
 */
void bh_tree_destroy(void* tree, char key_class);

/* ─── Thread info (epoch-based reclamation) ──────────────────────── */

/*
 * Obtain a ThreadInfo handle for the current thread.
 * Must be called once per backend / per thread.
 */
void* bh_get_thread_info(void* tree, char key_class);

/* ─── Point operations ───────────────────────────────────────────── */

/*
 * Insert a key/value pair.
 * For key_class='i': key_data points to a uint64_t.
 * For key_class='s': key_data points to key_len bytes of string data.
 */
void bh_insert(void* tree, char key_class,
               const void* key_data, size_t key_len,
               uint64_t value,
               void* thread_info);

/*
 * Look up a single key.
 * Returns the associated value, or 0 if not found.
 * Sets *found = true/false.
 */
uint64_t bh_lookup(void* tree, char key_class,
                   const void* key_data, size_t key_len,
                   void* thread_info,
                   int* found);

/*
 * Update the value for an existing key.
 * Returns true if the key existed and was updated.
 */
int bh_update(void* tree, char key_class,
              const void* key_data, size_t key_len,
              uint64_t new_value,
              void* thread_info);

/*
 * Remove a key.
 * Returns true if the key existed and was removed.
 */
int bh_remove(void* tree, char key_class,
              const void* key_data, size_t key_len,
              void* thread_info);

/* ─── Range scan ─────────────────────────────────────────────────── */

/*
 * Perform a forward range scan starting at min_key.
 * Returns up to `range` values in `buf`.
 * Returns the number of values actually returned.
 */
int bh_range_lookup(void* tree, char key_class,
                    const void* min_key_data, size_t min_key_len,
                    int range,
                    uint64_t* buf,
                    void* thread_info);

/* ─── Diagnostics ────────────────────────────────────────────────── */

int      bh_height(void* tree, char key_class);
double   bh_utilization(void* tree, char key_class);
void     bh_print(void* tree, char key_class);

#ifdef __cplusplus
}
#endif

#endif /* BLINKHASH_CORE_H */
