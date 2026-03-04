-- blinkhash--1.0.sql
-- B^link-hash Index Access Method for PostgreSQL

-- Step 1: Register the access method
CREATE ACCESS METHOD blinkhash TYPE INDEX HANDLER blinkhash_handler;
COMMENT ON ACCESS METHOD blinkhash IS
  'B^link-hash: adaptive hybrid index that starts as hash and converts to btree on overflow';

-- Step 2: Operator class for integer types
CREATE OPERATOR CLASS int4_ops
  DEFAULT FOR TYPE integer USING blinkhash AS
    OPERATOR 1 <,
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 btint4cmp(integer, integer);

CREATE OPERATOR CLASS int8_ops
  DEFAULT FOR TYPE bigint USING blinkhash AS
    OPERATOR 1 <,
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 btint8cmp(bigint, bigint);

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
