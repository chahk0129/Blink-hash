#include "wrapper.h"
#include <tree.h>

class Blinkhash_str{
    BLINK_HASH::btree_t<GenericKey<32>, Value>* idx;
    public:
        Blinkhash_str(){
	    idx = new BLINK_HASH::btree_t<GenericKey<32>, Value>();
	}

	void insert(GenericKey<32> key, Value value){
	    idx->insert(key, value);
	}

	Value find(GenericKey<32> key){
	    return idx->lookup(key);
	}

	bool update(GenericKey<32> key, Value value){
	    return idx->update(key, value);
	}

	void scan(GenericKey<32> key, int range, Value* buf){
	    idx->range_lookup(key, range, buf);
	}
};

Blinkhash_str* _idx;

Blinkhash_wrapper::Blinkhash_wrapper(){
    _idx = new Blinkhash_str();
}

void Blinkhash_wrapper::insert(GenericKey<32> key, Value value){
    _idx->insert(key, value);
}

Value Blinkhash_wrapper::find(GenericKey<32> key){
    return _idx->find(key);
}

bool Blinkhash_wrapper::update(GenericKey<32> key, Value value){
    return _idx->update(key, value);
}

void scan(GenericKey<32> key, int range, Value* buf){
    _idx->scan(key, range, buf);
}
