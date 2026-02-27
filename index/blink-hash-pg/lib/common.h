#ifndef BLINK_HASH_COMMON_H__
#define BLINK_HASH_COMMON_H__

#include <cstdint>
#include <cstring>
#include <string>
#include <cassert>

#include "bh_key.h"

namespace BLINK_HASH{

typedef uint64_t key64_t;
typedef uint64_t value64_t;

/* String key support — GenericKey variants from bh_key.h */
constexpr std::size_t KEY_LENGTH = BH_DEFAULT_KEY_LENGTH;  /* 32 */
using StringKey     = GenericKey<KEY_LENGTH>;
using MediumKey     = GenericKey<BH_MEDIUM_KEY_LENGTH>;     /* 64 */
using LongStringKey = GenericKey<BH_LONG_KEY_LENGTH>;       /* 128 */

}
#endif
