

#include "blinkhash_utils.h"

#include "postgres.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/datum.h"

/*
 * Classify a PG type OID into 'i' (integer/numeric) or 's' (string).
 * This determines which template specialization the engine uses.
 */
char
bh_classify_type(Oid typid)
{
    switch (typid)
    {
        case INT4OID:
        case INT8OID:
        case FLOAT8OID:
        case INT2OID:
        case FLOAT4OID:
        case OIDOID:
            return 'i';
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
        case NAMEOID:
        case BYTEAOID:
            return 's';
        default:
            /* Default to integer for fixed-width types ≤ 8 bytes */
            return 'i';
    }
}

/*
 * Convert a PG Datum to a uint64 key.
 *
 * For signed types we apply an offset so that the unsigned comparison
 * order matches the signed SQL order:
 *   int4:  key = (uint64)(value + INT32_MAX + 1)
 *   int8:  key = (uint64)(value + INT64_MAX + 1)  — i.e. XOR sign bit
 *   float8: IEEE 754 trick — flip sign bit, flip rest if negative
 */
uint64
bh_datum_to_key64(Datum value, Oid typid, bool *ok)
{
    *ok = true;

    switch (typid)
    {
        case INT4OID:
        {
            int32 v = DatumGetInt32(value);
            return (uint64)((int64)v + (int64)INT32_MAX + 1);
        }
        case INT8OID:
        {
            int64 v = DatumGetInt64(value);
            /* XOR sign bit: makes unsigned order match signed order */
            return (uint64)(v ^ ((int64)1 << 63));
        }
        case FLOAT8OID:
        {
            double d = DatumGetFloat8(value);
            uint64 bits;
            memcpy(&bits, &d, sizeof(bits));
            /* IEEE 754 order-preserving transform */
            if (bits & ((uint64)1 << 63))
                bits = ~bits;           /* negative: flip all bits */
            else
                bits ^= ((uint64)1 << 63);  /* positive: flip sign bit */
            return bits;
        }
        case INT2OID:
        {
            int16 v = DatumGetInt16(value);
            return (uint64)((int64)v + (int64)INT16_MAX + 1);
        }
        case FLOAT4OID:
        {
            float f = DatumGetFloat4(value);
            uint32 bits;
            memcpy(&bits, &f, sizeof(bits));
            if (bits & ((uint32)1 << 31))
                bits = ~bits;
            else
                bits ^= ((uint32)1 << 31);
            return (uint64)bits;
        }
        case OIDOID:
            return (uint64)DatumGetObjectId(value);
        default:
            *ok = false;
            return 0;
    }
}

/*
 * Convert a uint64 key back to a PG Datum.
 * Reverse of bh_datum_to_key64().
 */
Datum
bh_key64_to_datum(uint64 key, Oid typid)
{
    switch (typid)
    {
        case INT4OID:
        {
            int32 v = (int32)((int64)key - (int64)INT32_MAX - 1);
            return Int32GetDatum(v);
        }
        case INT8OID:
        {
            int64 v = (int64)(key ^ ((uint64)1 << 63));
            return Int64GetDatum(v);
        }
        case FLOAT8OID:
        {
            uint64 bits = key;
            if (bits & ((uint64)1 << 63))
                bits ^= ((uint64)1 << 63);
            else
                bits = ~bits;
            double d;
            memcpy(&d, &bits, sizeof(d));
            return Float8GetDatum(d);
        }
        case INT2OID:
        {
            int16 v = (int16)((int64)key - (int64)INT16_MAX - 1);
            return Int16GetDatum(v);
        }
        case FLOAT4OID:
        {
            uint32 bits = (uint32)key;
            if (bits & ((uint32)1 << 31))
                bits ^= ((uint32)1 << 31);
            else
                bits = ~bits;
            float f;
            memcpy(&f, &bits, sizeof(f));
            return Float4GetDatum(f);
        }
        case OIDOID:
            return ObjectIdGetDatum((Oid)key);
        default:
            elog(ERROR, "blinkhash: unsupported type OID %u", typid);
            return (Datum)0;
    }
}

/*
 * Convert a Datum to a string key (GenericKey<32>).
 * Writes up to buf_capacity bytes into buf.
 * Returns the number of bytes written.
 */
int
bh_datum_to_string_key(Datum value, Oid typid, char *buf, int buf_capacity)
{
    const char *src;
    int         src_len;

    switch (typid)
    {
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
        {
            text *t = DatumGetTextPP(value);
            src     = VARDATA_ANY(t);
            src_len = VARSIZE_ANY_EXHDR(t);
            break;
        }
        case NAMEOID:
        {
            Name n  = DatumGetName(value);
            src     = NameStr(*n);
            src_len = strlen(src);
            break;
        }
        case BYTEAOID:
        {
            bytea *b = DatumGetByteaPP(value);
            src      = VARDATA_ANY(b);
            src_len  = VARSIZE_ANY_EXHDR(b);
            break;
        }
        default:
            elog(ERROR, "blinkhash: unsupported string type OID %u", typid);
            return 0;
    }

    memset(buf, 0, buf_capacity);
    int copy_len = (src_len >= buf_capacity) ? buf_capacity - 1 : src_len;
    memcpy(buf, src, copy_len);
    return copy_len;
}

/*
 * Convert a string key buffer back to a PG Datum (text).
 */
Datum
bh_string_key_to_datum(const char *buf, int len)
{
  
    int actual_len = strnlen(buf, len);
    text *t = (text *) palloc(VARHDRSZ + actual_len);
    SET_VARSIZE(t, VARHDRSZ + actual_len);
    memcpy(VARDATA(t), buf, actual_len);
    return PointerGetDatum(t);
}

/*
 * Pack a PG ItemPointer (TID) into a uint64 value.
 *
 * Layout: [ BlockNumber (48 bits) | OffsetNumber (16 bits) ]
 *
 * PG BlockNumber is uint32, OffsetNumber is uint16.
 * This packing allows up to 2^48 blocks (256 TB at 8KB block size).
 */
uint64
bh_tid_to_value(ItemPointer tid)
{
    uint64 block  = (uint64)ItemPointerGetBlockNumber(tid);
    uint64 offset = (uint64)ItemPointerGetOffsetNumber(tid);
    return (block << 16) | (offset & 0xFFFF);
}


void
bh_value_to_tid(uint64 value, ItemPointerData *tid_out)
{
    BlockNumber  block  = (BlockNumber)(value >> 16);
    OffsetNumber offset = (OffsetNumber)(value & 0xFFFF);
    ItemPointerSet(tid_out, block, offset);
}