#ifndef KEYVAL_H__
#define KEYVAL_H__

#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <unistd.h>
#include <cassert>

struct IntKeyVal{
    uint64_t key;
    uint64_t value;

    void setKey(uint64_t _key, uint64_t _value){
	key = _key;
	value = _value;
    }
};

template <typename ValueType = IntKeyVal*>
class IntKeyExtractor{
    public:
	typedef uint64_t KeyType;
	
	inline KeyType operator()(ValueType const& value) const{
	    return value->key;
	}
};

struct StrKeyVal{
    uint32_t len;
    uint64_t value;
    uint8_t* data;

    StrKeyVal(int length): len(length), data(new uint8_t[length]){ }
    void setKey(const char bytes[], const int length, uint64_t _value){
	value = _value;
	memcpy(data, bytes, length);
    }
};

template <typename ValueType = StrKeyVal*>
class StrKeyExtractor{
    public:
	typedef char const* KeyType;

	inline KeyType operator()(ValueType const& value) const{
	    return (char const*)value->data;
	}
};


#endif
