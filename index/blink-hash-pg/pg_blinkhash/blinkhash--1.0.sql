-- blinkhash--1.0.sql
-- B^link-hash Index Access Method for PostgreSQL

-- Step 1: Register the handler function (loads blinkhash.dylib/.so)
CREATE FUNCTION blinkhash_handler(internal)
RETURNS index_am_handler
AS 'blinkhash', 'blinkhash_handler'
LANGUAGE C STRICT;

-- Step 2: Register the access method
CREATE ACCESS METHOD blinkhash TYPE INDEX HANDLER blinkhash_handler;
COMMENT ON ACCESS METHOD blinkhash IS
  'B^link-hash: adaptive hybrid index that starts as hash and converts to btree on overflow';

-- Step 2: Operator classes for integer types
--
-- We group int2, int4, int8 into ONE operator FAMILY so the planner
-- can match cross-type comparisons (e.g. int8 column = int4 literal).

CREATE OPERATOR FAMILY integer_ops USING blinkhash;

-- int4 opclass
CREATE OPERATOR CLASS int4_ops
  DEFAULT FOR TYPE integer USING blinkhash FAMILY integer_ops AS
    OPERATOR 1 <  (integer, integer),
    OPERATOR 2 <= (integer, integer),
    OPERATOR 3 =  (integer, integer),
    OPERATOR 4 >= (integer, integer),
    OPERATOR 5 >  (integer, integer),
    FUNCTION 1 btint4cmp(integer, integer);

-- int8 opclass
CREATE OPERATOR CLASS int8_ops
  DEFAULT FOR TYPE bigint USING blinkhash FAMILY integer_ops AS
    OPERATOR 1 <  (bigint, bigint),
    OPERATOR 2 <= (bigint, bigint),
    OPERATOR 3 =  (bigint, bigint),
    OPERATOR 4 >= (bigint, bigint),
    OPERATOR 5 >  (bigint, bigint),
    FUNCTION 1 btint8cmp(bigint, bigint);

-- Cross-type operators:  int8 vs int4  (most common in practice)
ALTER OPERATOR FAMILY integer_ops USING blinkhash ADD
    OPERATOR 1 <  (bigint, integer),
    OPERATOR 2 <= (bigint, integer),
    OPERATOR 3 =  (bigint, integer),
    OPERATOR 4 >= (bigint, integer),
    OPERATOR 5 >  (bigint, integer),
    FUNCTION 1 (bigint, integer) btint84cmp(bigint, integer);

ALTER OPERATOR FAMILY integer_ops USING blinkhash ADD
    OPERATOR 1 <  (integer, bigint),
    OPERATOR 2 <= (integer, bigint),
    OPERATOR 3 =  (integer, bigint),
    OPERATOR 4 >= (integer, bigint),
    OPERATOR 5 >  (integer, bigint),
    FUNCTION 1 (integer, bigint) btint48cmp(integer, bigint);

-- Cross-type operators:  int8 vs int2
ALTER OPERATOR FAMILY integer_ops USING blinkhash ADD
    OPERATOR 1 <  (bigint, smallint),
    OPERATOR 2 <= (bigint, smallint),
    OPERATOR 3 =  (bigint, smallint),
    OPERATOR 4 >= (bigint, smallint),
    OPERATOR 5 >  (bigint, smallint),
    FUNCTION 1 (bigint, smallint) btint82cmp(bigint, smallint);

ALTER OPERATOR FAMILY integer_ops USING blinkhash ADD
    OPERATOR 1 <  (smallint, bigint),
    OPERATOR 2 <= (smallint, bigint),
    OPERATOR 3 =  (smallint, bigint),
    OPERATOR 4 >= (smallint, bigint),
    OPERATOR 5 >  (smallint, bigint),
    FUNCTION 1 (smallint, bigint) btint28cmp(smallint, bigint);

-- Cross-type operators:  int4 vs int2
ALTER OPERATOR FAMILY integer_ops USING blinkhash ADD
    OPERATOR 1 <  (integer, smallint),
    OPERATOR 2 <= (integer, smallint),
    OPERATOR 3 =  (integer, smallint),
    OPERATOR 4 >= (integer, smallint),
    OPERATOR 5 >  (integer, smallint),
    FUNCTION 1 (integer, smallint) btint42cmp(integer, smallint);

ALTER OPERATOR FAMILY integer_ops USING blinkhash ADD
    OPERATOR 1 <  (smallint, integer),
    OPERATOR 2 <= (smallint, integer),
    OPERATOR 3 =  (smallint, integer),
    OPERATOR 4 >= (smallint, integer),
    OPERATOR 5 >  (smallint, integer),
    FUNCTION 1 (smallint, integer) btint24cmp(smallint, integer);

CREATE OPERATOR CLASS float8_ops
  DEFAULT FOR TYPE double precision USING blinkhash AS
    OPERATOR 1 <,
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 btfloat8cmp(double precision, double precision);

-- Step 3: Operator class for text types (uses GenericKey<32>)
CREATE OPERATOR CLASS text_ops
  DEFAULT FOR TYPE text USING blinkhash AS
    OPERATOR 1 <,
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 bttextcmp(text, text);
