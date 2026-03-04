/*
 * blinkhash_validate.h — Operator class validation
 */
#ifndef BLINKHASH_VALIDATE_H
#define BLINKHASH_VALIDATE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

/*
 * Validate that the operator class is one we support.
 *
 * Supported opclasses:
 *   - int4_ops     (integer)
 *   - int8_ops     (bigint)
 *   - float8_ops   (double precision)
 *   - text_ops     (text / varchar)
 *
 * For each opclass, we require the standard 5 comparison operators
 * (<, <=, =, >=, >) and a hash function.
 *
 * Returns true if valid; logs a WARNING and returns false otherwise.
 */

/* blinkhash_amvalidate() declared in blinkhash_am.h */

#ifdef __cplusplus
}
#endif

#endif /* BLINKHASH_VALIDATE_H */
