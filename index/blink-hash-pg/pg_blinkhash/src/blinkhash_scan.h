/*
 * blinkhash_scan.h — Index scan callbacks and opaque scan state
 *
 * Supports:
 *   - Point lookups     (single ScanKey, EQ strategy)
 *   - Forward range scans (ScanKey with LT/LE/GE/GT strategy)
 *   - Bitmap scans      (amgetbitmap)
 */
#ifndef BLINKHASH_SCAN_H
#define BLINKHASH_SCAN_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "access/genam.h"
#include "access/relscan.h"
#include "nodes/execnodes.h"

/* ─── Opaque scan state (stored in scan->opaque) ─────────────────── */

typedef struct BHScanOpaqueData {
    void   *tree;            /* btree_t<K,V>*  (type-erased)        */
    void   *thread_info;     /* ThreadInfo*     (type-erased)        */
    char    key_class;       /* 'i' or 's'                          */
    Oid     key_typid;

    /* Range scan state */
    bool    scan_started;
    bool    scan_finished;
    int     range_buf_capacity;
    int     range_buf_count;
    int     range_buf_pos;
    uint64 *range_buf;       /* buffer of value64_t results          */

    /* Saved key for rescan */
    uint64  saved_key64;
    char    saved_key_str[32]; /* GenericKey<32> */
} BHScanOpaqueData;

typedef BHScanOpaqueData *BHScanOpaque;

/* All scan callbacks declared in blinkhash_am.h */

#ifdef __cplusplus
}
#endif

#endif /* BLINKHASH_SCAN_H */
