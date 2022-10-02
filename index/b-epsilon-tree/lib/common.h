#ifndef BETREE_COMMON_H__
#define BETREE_COMMON_H__

#include <cstdlib>
#include <iostream>
#include <unistd.h>
#include <immintrin.h>
#include <cstdint>
#include <atomic>
#include <cstring>

namespace B_EPSILON_TREE{

template <typename Key_t, typename Value_t>
struct entry_t{
    Key_t key;
    Value_t value;
};



enum opcode_t{
    OP_INSERT = 0,
    OP_UPDATE,
    OP_DELETE
};

#define BUFFER_SIZE (1024)
}
#define PTR (1000)
#endif
