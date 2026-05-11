#ifndef SHARED_MMAP_H
#define SHARED_MMAP_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

void* bh_shared_mmap_init(size_t size);
void* bh_shared_malloc(size_t size);
void bh_shared_free(void* ptr);

void* bh_get_global_tree(void);
void bh_set_global_tree(void* tree);
void bh_global_lock(void);
void bh_global_unlock(void);

#ifdef __cplusplus
}
#endif

#endif
