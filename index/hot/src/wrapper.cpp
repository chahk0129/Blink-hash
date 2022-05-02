#include "wrapper.h"

#include <hot/rowex/HOTRowex.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

class HOT_idx{
    #ifdef STRING_KEY
    using TrieType = hot::rowex::HOTRowex<StrKeyVal*, StrKeyExtractor>;
    #else
    using TrieType = hot::rowex::HOTRowex<IntKeyVal*, IntKeyExtractor>;
    #endif
    TrieType* idx;

    public:
        HOT_idx(){ 
	    idx = new TrieType;
        }
	#ifdef STRING_KEY
	void insert(char* key, int keylen, uint64_t value){
	    StrKeyVal* kv = new StrKeyVal(keylen); kv->setKey(key, keylen, value);
	    idx->insert(kv);
	}

	bool upsert(char* key, int keylen, uint64_t value){
	    StrKeyVal* kv = new StrKeyVal(keylen); kv->setKey(key, keylen, value);
	    idx::contenthelpers::OptionalValue<StrKeyVal*> ret = idx->upsert(kv);

	    /*
	    idx::contenthelpers::OptionalValue<StrKeyVal*> check = idx->lookup(key);
	    if(ret.mIsValid){
		if(check.mIsValid){
		    if(*(uint64_t*)check.mValue->value != *(uint64_t*)value) std::cout << "diff value" << std::endl;
		}
		else std::cout << "update lookup failed!" << std::endl;
	    }
	    else std::cout << "update failed! " << std::endl;
	    */
	    return ret.mIsValid;
	}

	uint64_t find(char* key){
	    idx::contenthelpers::OptionalValue<StrKeyVal*> ret = idx->lookup(key);
	    if(ret.mIsValid){
		if(strcmp(key, (char*)ret.mValue->data) != 0){
		    std::cout << "find incorrect key" << std::endl;
		}
	    }
	    else{
		std::cout << "find invalid record" << std::endl;
	    }
	    return ret.mValue->value;
	}

	void scan(char* key, int range){
	    idx::contenthelpers::OptionalValue<StrKeyVal*> ret = idx->scan(key ,range);
	}
	#else
	void insert(uint64_t key, uint64_t value){
	    IntKeyVal* kv = new IntKeyVal; kv->setKey(key, value);
	    idx->insert(kv);
	}

	bool upsert(uint64_t key, uint64_t value){
	    IntKeyVal* kv = new IntKeyVal; kv->setKey(key, value);
	    idx::contenthelpers::OptionalValue<IntKeyVal*> ret = idx->upsert(kv);

	    /*
	    idx::contenthelpers::OptionalValue<IntKeyVal*> check = idx->lookup(key);
	    if(ret.mIsValid){
		if(check.mIsValid){
		    if(*(uint64_t*)check.mValue->value != *(uint64_t*)value) std::cout << "diff value" << std::endl;
		}
		else std::cout << "update lookup failed!" << std::endl;
	    }
	    else std::cout << "update failed! " << std::endl;
	    */
	    return ret.mIsValid;
	}

	uint64_t find(uint64_t key){
	    idx::contenthelpers::OptionalValue<IntKeyVal*> ret = idx->lookup(key);
	    return ret.mValue->value;
	}

	void scan(uint64_t key, int range){
	    idx::contenthelpers::OptionalValue<IntKeyVal*> ret = idx->scan(key, range);
	}
	#endif

	void find_depth(){
	    uint64_t leaf_cnt, leaf_depth, max_depth;
	    leaf_cnt = leaf_depth = max_depth = 0;
	    idx->findDepth(leaf_cnt, leaf_depth, max_depth);
	    std::cout << "Total number of leaf nodes: \t" << leaf_cnt << std::endl;
	    std::cout << "Total depths of leaf nodes: \t" << leaf_depth << std::endl;
	    std::cout << "Max depths: \t" << max_depth << std::endl;
	    std::cout << "Average depths: \t" << (double)leaf_depth / leaf_cnt << std::endl;
	}

	void get_memory(uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied){
	    idx->getMemory(meta, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
	}

	#ifdef BREAKDOWN
	void get_breakdown(uint64_t& time_traversal, uint64_t& time_abort, uint64_t& time_latch, uint64_t& time_node, uint64_t& time_split){
	    idx->get_breakdown(time_traversal, time_abort, time_latch, time_node, time_split);
	}
	#endif
};

    
HOT_idx* hot_idx;

HOT_wrapper::HOT_wrapper(){
    hot_idx = new HOT_idx();
}

#ifdef STRING_KEY
void HOT_wrapper::insert(char* key, int keylen, uint64_t value){
    return hot_idx->insert(key, keylen, value);
}

uint64_t HOT_wrapper::find(char* key){
    return hot_idx->find(key);
}

bool HOT_wrapper::upsert(char* key, int keylen, uint64_t value){
    return hot_idx->upsert(key, keylen, value);
}

void HOT_wrapper::scan(char* key, int range){
    hot_idx->scan(key, range);
}

#else
void HOT_wrapper::insert(uint64_t key, uint64_t value){
    return hot_idx->insert(key, value);
}

uint64_t HOT_wrapper::find(uint64_t key){
    return hot_idx->find(key);
}

bool HOT_wrapper::upsert(uint64_t key, uint64_t value){
    return hot_idx->upsert(key, value);
}

void HOT_wrapper::scan(uint64_t key, int range){
    hot_idx->scan(key, range);
}

#endif

void HOT_wrapper::find_depth(void){
    hot_idx->find_depth();
}

void HOT_wrapper::get_memory(uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied){
    hot_idx->get_memory(meta, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
}

#ifdef BREAKDOWN
void HOT_wrapper::get_breakdown(uint64_t& time_traversal, uint64_t& time_abort, uint64_t& time_latch, uint64_t& time_node, uint64_t& time_split){
    hot_idx->get_breakdown(time_traversal, time_abort, time_latch, time_node, time_split);
}
#endif
