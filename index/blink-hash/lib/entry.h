#ifndef BLINK_HASH_ENTRY_H__
#define BLINK_HASH_ENTRY_H__

namespace BLINK_HASH{

static constexpr int entry_num = 32;

template <typename Key_t, typename Value_t>
struct entry_t{
    Key_t key;
    Value_t value;
};

}
#endif
