#ifndef BLINK_HASH_BH_KEY_H__
#define BLINK_HASH_BH_KEY_H__

/*
 * bh_key.h — Unified key type definitions for B^link-hash PG integration
 *
 * This header consolidates both integer (uint64) and variable-length string
 * key support.  GenericKey<N> is adapted from the blink-hash-str variant's
 * include/indexkey.h, made self-contained so the library does not depend on
 * external header paths.
 */

#include <cstdint>
#include <cstring>
#include <string>
#include <functional>
#include <ostream>

namespace BLINK_HASH {

/* ─── Fixed-size string key ─────────────────────────────────────────── */

template <std::size_t keySize>
class GenericKey {
public:
    char data[keySize];

    /* ── constructors ── */
    GenericKey()                       { memset(data, 0x00, keySize); }
    GenericKey(int)                    { memset(data, 0x00, keySize); }
    GenericKey(const GenericKey& other){ memcpy(data, other.data, keySize); }

    /* ── assignment ── */
    inline GenericKey& operator=(const GenericKey& other) {
        memcpy(data, other.data, keySize);
        return *this;
    }

    /* ── string import ── */
    inline void setFromString(const std::string& key) {
        memset(data, 0, keySize);
        if (key.size() >= keySize) {
            memcpy(data, key.c_str(), keySize - 1);
            data[keySize - 1] = '\0';
        } else {
            memcpy(data, key.c_str(), key.size());
            data[key.size()] = '\0';
        }
    }

    /* ── raw-bytes import (for PG Datum conversion) ── */
    inline void setFromBytes(const char* src, size_t len) {
        memset(data, 0, keySize);
        size_t copy_len = (len >= keySize) ? keySize - 1 : len;
        memcpy(data, src, copy_len);
        data[copy_len] = '\0';
    }

    /* ── comparisons (strcmp-based) ── */
    inline bool operator< (const GenericKey& o) const { return strcmp(data, o.data) <  0; }
    inline bool operator> (const GenericKey& o) const { return strcmp(data, o.data) >  0; }
    inline bool operator==(const GenericKey& o) const { return strcmp(data, o.data) == 0; }
    inline bool operator!=(const GenericKey& o) const { return !(*this == o); }
    inline bool operator<=(const GenericKey& o) const { return !(*this > o); }
    inline bool operator>=(const GenericKey& o) const { return !(*this < o); }
};

/* ── Functor helpers ── */

template <std::size_t keySize>
struct GenericComparator {
    inline bool operator()(const GenericKey<keySize>& a,
                           const GenericKey<keySize>& b) const {
        return strcmp(a.data, b.data) < 0;
    }
};

template <std::size_t keySize>
struct GenericEqualityChecker {
    inline bool operator()(const GenericKey<keySize>& a,
                           const GenericKey<keySize>& b) const {
        return strcmp(a.data, b.data) == 0;
    }
};

/* ── Standard key sizes ── */
constexpr std::size_t BH_DEFAULT_KEY_LENGTH = 32;
constexpr std::size_t BH_MEDIUM_KEY_LENGTH  = 64;
constexpr std::size_t BH_LONG_KEY_LENGTH    = 128;

using StringKey     = GenericKey<BH_DEFAULT_KEY_LENGTH>;   /* 32 bytes  */
using MediumKey     = GenericKey<BH_MEDIUM_KEY_LENGTH>;    /* 64 bytes  */
using LongStringKey = GenericKey<BH_LONG_KEY_LENGTH>;      /* 128 bytes */

/* ── Compile-time sanity checks ── */
static_assert(sizeof(GenericKey<32>)  == 32,  "GenericKey<32> must be 32 bytes");
static_assert(sizeof(GenericKey<64>)  == 64,  "GenericKey<64> must be 64 bytes");
static_assert(sizeof(GenericKey<128>) == 128, "GenericKey<128> must be 128 bytes");

/* ── Stream output (for debug prints) ── */
template <std::size_t keySize>
inline std::ostream& operator<<(std::ostream& os, const GenericKey<keySize>& k) {
    os << k.data;
    return os;
}

} // namespace BLINK_HASH

#endif // BLINK_HASH_BH_KEY_H__
