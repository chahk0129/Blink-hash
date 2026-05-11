
#include "blinkhash_am.h"
#include "blinkhash_utils.h"

#include "postgres.h"
#include "access/amvalidate.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/regproc.h"

bool
blinkhash_amvalidate(Oid opclassoid)
{
    HeapTuple   classtup;
    Form_pg_opclass classform;
    Oid         opfamilyoid;
    Oid         opcintype;
    bool        result = true;

    classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
    if (!HeapTupleIsValid(classtup))
        elog(ERROR, "cache lookup failed for opclass %u", opclassoid);
    classform = (Form_pg_opclass) GETSTRUCT(classtup);
    opfamilyoid = classform->opcfamily;
    opcintype   = classform->opcintype;
    ReleaseSysCache(classtup);

   
    char key_class = bh_classify_type(opcintype);
    if (key_class != 'i' && key_class != 's')
    {
        ereport(INFO,
                (errmsg("blinkhash does not support type OID %u", opcintype)));
        result = false;
    }

    /*
     * We require 5 operators (< <= = >= >) and
     * 1 support function (comparison).
     *
     * A full implementation would iterate pg_amop and pg_amproc
     * to verify each strategy/support is present.
     * For now, just verify the type is supported.
     */

    return result;
}