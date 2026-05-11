/*
 * blinkhash_handler.h — B^link-hash Index AM handler entry point
 *
 * Registers the access method with PostgreSQL by returning
 * a populated IndexAmRoutine.
 *
 * Implements: Phase 7.2.1 of IMPLEMENTATION_SEQUENCE.md
 */
#ifndef BLINKHASH_HANDLER_H
#define BLINKHASH_HANDLER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "fmgr.h"
#include "access/amapi.h"

/*
 * PG_FUNCTION — called by CREATE ACCESS METHOD ... HANDLER blinkhash_handler
 * Returns a fully populated IndexAmRoutine with capability flags and
 * callback function pointers.
 */
PG_FUNCTION_INFO_V1(blinkhash_handler);
Datum blinkhash_handler(PG_FUNCTION_ARGS);

#ifdef __cplusplus
}
#endif

#endif /* BLINKHASH_HANDLER_H */
