

#include "blinkhash_handler.h"
#include "blinkhash_am.h"
#ifdef BH_USE_PG_WAL
#include "blinkhash_wal.h"
#endif

#include "postgres.h"
#include "access/amapi.h"
#include "utils/elog.h"

PG_MODULE_MAGIC;
void _PG_init(void)
{
#ifdef BH_USE_PG_WAL
    blinkhash_wal_init();
#endif
}
Datum
blinkhash_handler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

  
    amroutine->amstrategies     = 5;    /* 5 comparison strategies */
    amroutine->amsupport        = 1;    /* 1 support function (cmp) */
    amroutine->amoptsprocnum    = 0;
    amroutine->amcanorder       = true; /* sorted output (btree leaves) */
    amroutine->amcanorderbyop   = false;
    amroutine->amcanbackward    = false; /* forward scans only */
    amroutine->amcanunique      = false; /* no unique constraint */
    amroutine->amcanmulticol    = false; /* single-column only */
    amroutine->amoptionalkey    = false;
    amroutine->amsearcharray    = false;
    amroutine->amsearchnulls    = false;
    amroutine->amstorage        = false;
    amroutine->amclusterable    = false;
    amroutine->ampredlocks      = false;
    amroutine->amcanparallel    = false;
    amroutine->amcaninclude     = false;
    amroutine->amusemaintenanceworkmem = false;
    //amroutine->amsummarizing    = false;
    amroutine->amparallelvacuumoptions = 0;
    amroutine->amkeytype        = InvalidOid;

    /* ── Callback pointers ────────────────────────────────── */
    amroutine->ambuild          = blinkhash_ambuild;
    amroutine->ambuildempty     = blinkhash_ambuildempty;
    amroutine->aminsert         = blinkhash_aminsert;
    //amroutine->aminsertcleanup  = NULL;
    amroutine->ambulkdelete     = blinkhash_ambulkdelete;
    amroutine->amvacuumcleanup  = blinkhash_amvacuumcleanup;
    amroutine->amcanreturn      = blinkhash_amcanreturn;
    amroutine->amcostestimate   = blinkhash_amcostestimate;
    amroutine->amoptions        = NULL;  /* no reloptions */
    amroutine->amproperty       = NULL;
    amroutine->ambuildphasename = NULL;
    amroutine->amvalidate       = blinkhash_amvalidate;
    amroutine->ambeginscan      = blinkhash_ambeginscan;
    amroutine->amrescan         = blinkhash_amrescan;
    amroutine->amgettuple       = blinkhash_amgettuple;
    amroutine->amgetbitmap      = blinkhash_amgetbitmap;
    amroutine->amendscan        = blinkhash_amendscan;
    amroutine->ammarkpos        = blinkhash_ammarkpos;
    amroutine->amrestrpos       = blinkhash_amrestrpos;
    amroutine->amestimateparallelscan  = NULL;
    amroutine->aminitparallelscan      = NULL;
    amroutine->amparallelrescan        = NULL;

    PG_RETURN_POINTER(amroutine);
}