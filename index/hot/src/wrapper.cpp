#include "wrapper.h"

#include <hot/rowex/HOTRowex.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

class HOT_idx_int{
    using TrieType = hot::rowex::HOTRowex<IntKeyVal*, IntKeyExtractor>;
    TrieType* idx;
    public:
        HOT_idx_int(){
	    idx = new TrieType;
	}
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

class HOT_idx_str{
    using TrieType = hot::rowex::HOTRowex<StrKeyVal*, StrKeyExtractor>;
    TrieType* idx;
    public:
        HOT_idx_str(){
	    idx = new TrieType;
	}

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
	    
HOT_idx_str* hot_str;
HOT_idx_int* hot_int;

HOT_int_wrapper::HOT_int_wrapper(){
    hot_int = new HOT_idx_int();
}

void HOT_int_wrapper::insert(uint64_t key, uint64_t value){
    return hot_int->insert(key, value);
}

uint64_t HOT_int_wrapper::find(uint64_t key){
    return hot_int->find(key);
}

bool HOT_int_wrapper::upsert(uint64_t key, uint64_t value){
    return hot_int->upsert(key, value);
}

void HOT_int_wrapper::scan(uint64_t key, int range){
    hot_int->scan(key, range);
}

void HOT_int_wrapper::find_depth(void){
    hot_int->find_depth();
}

void HOT_int_wrapper::get_memory(uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied){
    hot_int->get_memory(meta, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
}

#ifdef BREAKDOWN
void HOT_int_wrapper::get_breakdown(uint64_t& time_traversal, uint64_t& time_abort, uint64_t& time_latch, uint64_t& time_node, uint64_t& time_split){
    hot_int->get_breakdown(time_traversal, time_abort, time_latch, time_node, time_split);
}
#endif



HOT_string_wrapper::HOT_string_wrapper(){
    hot_str = new HOT_idx_str();
}

void HOT_string_wrapper::insert(char* key, int keylen, uint64_t value){
    return hot_str->insert(key, keylen, value);
}

uint64_t HOT_string_wrapper::find(char* key){
    return hot_str->find(key);
}

bool HOT_string_wrapper::upsert(char* key, int keylen, uint64_t value){
    return hot_str->upsert(key, keylen, value);
}

void HOT_string_wrapper::scan(char* key, int range){
    hot_str->scan(key, range);
}

void HOT_string_wrapper::find_depth(void){
    hot_str->find_depth();
}

void HOT_string_wrapper::get_memory(uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied){
    hot_str->get_memory(meta, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
}

#ifdef BREAKDOWN
void HOT_string_wrapper::get_breakdown(uint64_t& time_traversal, uint64_t& time_abort, uint64_t& time_latch, uint64_t& time_node, uint64_t& time_split){
    hot_str->get_breakdown(time_traversal, time_abort, time_latch, time_node, time_split);
}
#endif
