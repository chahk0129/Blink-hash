#ifndef HOT_WRAPPER_H__
#define HOT_WRAPPER_H__

#include <unistd.h>
#include <cstdlib>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <map>

#include "key.h"
#include "include/indexkey.h"
//#include "../../../indexkey.h"

/*
template <typename KeyType>
struct kvpair_t{
    KeyType key;
    uint64_t value;
};*/

//using int_keytype = uint64_t;
class HOT_int_wrapper{
    public:
	HOT_int_wrapper();
	void insert(uint64_t key, uint64_t value);
	uint64_t find(uint64_t key);
	bool upsert(uint64_t key, uint64_t value);
	void scan(uint64_t key, int range);
	void find_depth();
	void get_memory(uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied);
	#ifdef BREAKDOWN
	void get_breakdown(uint64_t& time_traversal, uint64_t& time_abort, uint64_t& time_latch, uint64_t& time_node, uint64_t& time_split);
	#endif
};

//using string_keytype = GenericKey<31>;
class HOT_string_wrapper{
    public:
	HOT_string_wrapper();
	void insert(char* key, int keylen, uint64_t value);
	uint64_t find(char* key);
	bool upsert(char* key, int keylen, uint64_t value);
	void scan(char* key, int range);
	void find_depth();
	void get_memory(uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied);
	#ifdef BREAKDOWN
	void get_breakdown(uint64_t& time_traversal, uint64_t& time_abort, uint64_t& time_latch, uint64_t& time_node, uint64_t& time_split);
	#endif
};

#endif
