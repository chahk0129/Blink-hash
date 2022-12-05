#ifndef BLINK_BUFFER_BATCH_UTIL_HASH_H_
#define BLINK_BUFFER_BATCH_UTIL_HASH_H_

#include <functional>
#include <stddef.h>

namespace BLINK_BUFFER_BATCH{

inline size_t standard(const void* _ptr, size_t _len, size_t _seed=static_cast<size_t>(0xc70f6907UL));

// JENKINS HASH FUNCTION
inline size_t jenkins(const void* _ptr, size_t _len, size_t _seed=0xc70f6907UL);


//-----------------------------------------------------------------------------
// MurmurHash2, by Austin Appleby

// Note - This code makes a few assumptions about how your machine behaves -

// 1. We can read a 4-byte value from any address without crashing
// 2. sizeof(int) == 4

// And it has a few limitations -

// 1. It will not work incrementally.
// 2. It will not produce the same results on little-endian and big-endian
//    machines.
inline size_t murmur2 ( const void * key, size_t len, size_t seed=0xc70f6907UL);

#define NUMBER64_1 11400714785074694791ULL
#define NUMBER64_2 14029467366897019727ULL
#define NUMBER64_3 1609587929392839161ULL
#define NUMBER64_4 9650029242287828579ULL
#define NUMBER64_5 2870177450012600261ULL

#define hash_get64bits(x) hash_read64_align(x, align)
#define hash_get32bits(x) hash_read32_align(x, align)
#define shifting_hash(x, r) ((x << r) | (x >> (64 - r)))
#define TO64(x) (((U64_INT *)(x))->v)
#define TO32(x) (((U32_INT *)(x))->v)

#define SEED (0xc70697UL)

typedef struct U64_INT
{
    uint64_t v;
} U64_INT;

typedef struct U32_INT
{
    uint32_t v;
} U32_INT;

uint64_t hash_read64_align(const void *ptr, uint32_t align);

uint32_t hash_read32_align(const void *ptr, uint32_t align);

uint64_t hash_compute(const void *input, uint64_t length, uint64_t seed, uint32_t align);

uint64_t xxhash(const void *data, size_t length, size_t seed);

size_t h(const void* key, size_t len, int func_num, size_t seed);

size_t h(const void* key, size_t len, int func_num);

}
#endif  // UTIL_HASH_H_
