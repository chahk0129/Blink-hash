#ifndef BLINK_HASH_COOMON_H__
#define BLINK_HASH_COMMON_H__

#include <cstdint>
#include <cstring>
#include <string>
#include <cassert>
#include "include/indexkey.h"
#define KEY_LENGTH 32

namespace BLINK_HASH{

typedef GenericKey<KEY_LENGTH> StringKey;
typedef uint64_t value64_t;

}
#endif
