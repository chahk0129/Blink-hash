#ifndef BLINK_HASH_WRAPPER_H__
#define BLINK_HASH_WRAPPER_H__

#include "include/indexkey.h"
//typedef GenericKey<32> Key;
typedef uint64_t Value;

class Blinkhash_wrapper{
    public:
	Blinkhash_wrapper();
	void insert(GenericKey<32> key, Value value);
	Value find(GenericKey<32> key);
	bool update(GenericKey<32> key, Value value);
	void scan(GenericKey<32> key, int range, Value* buf);
	void find_depth();
};
#endif

