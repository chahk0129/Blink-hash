/*
 * blinkhash_utils.h — Datum ↔ B^link-hash key/value type conversions
 *
 * Translates PostgreSQL Datum values and ItemPointers into the
 * native key64_t / GenericKey<N> / value64_t types used by the
 * B^link-hash engine, and vice versa.
 *
 * Implements: Phase 7.3 of IMPLEMENTATION_SEQUENCE.md
 */
#ifndef BLINKHASH_UTILS_H
#define BLINKHASH_UTILS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "access/htup_details.h"
#include "storage/itemptr.h"
#include "utils/builtins.h"

/*
 * Convert a PG Datum of the indexed column into a uint64 key.
 * Handles: int4, int8, float8, Oid.
 *
 * Returns 0 and sets *ok = false for unsupported types.
 */
uint64 bh_datum_to_key64(Datum value, Oid typid, bool *ok);

/*
 * Convert a uint64 key back to a PG Datum.
 */
Datum bh_key64_to_datum(uint64 key, Oid typid);

/*
 * Convert a PG text/varchar Datum into a fixed-size byte buffer.
 * Copies up to `buf_capacity - 1` bytes from the varlena and
 * null-terminates.
 *
 * Returns the number of bytes copied (excluding the NUL).
 */
int bh_datum_to_string_key(Datum value, Oid typid,
                           char *buf, int buf_capacity);

/*
 * Convert a fixed-size byte buffer back to a PG text Datum.
 */
Datum bh_string_key_to_datum(const char *buf, int len);

/*
 * Pack an ItemPointerData (block number + offset number) into a
 * 64-bit value suitable for storage as `value64_t` in the tree.
 *
 * Layout: bits 63..16 = blockNumber, bits 15..0 = offsetNumber
 */
uint64 bh_tid_to_value(ItemPointer tid);

/*
 * Unpack a 64-bit value back into an ItemPointerData.
 */
void bh_value_to_tid(uint64 value, ItemPointerData *tid_out);

/*
 * Determine whether a PG type OID is an integer/numeric type
 * that maps to key64_t, or a string type that maps to GenericKey.
 *
 * Returns 'i' for integer, 's' for string, '?' for unsupported.
 */
char bh_classify_type(Oid typid);

#ifdef __cplusplus
}
#endif

#endif /* BLINKHASH_UTILS_H */
