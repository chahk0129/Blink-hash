#ifndef BLINK_HASH_TREE_H__
#define BLINK_HASH_TREE_H__


//#include "common.h"
//#include "node.h"
#include "inode.h"
#include "lnode.h"

namespace BLINK_HASH{

template <typename Key_t, typename Value_t>
class btree_t{
    public:
	inline uint64_t _rdtsc(){
	    #if (__x86__ || __x86_64__)
            uint32_t lo, hi;
            asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
    	    return (((uint64_t) hi << 32) | lo);
            #else
    	    uint64_t val;
    	    asm volatile("mrs %0, cntvct_el0" : "=r" (val));
            return val;
	    #endif
	}

	btree_t(){ 
	    root = static_cast<node_t*>(new lnode_hash_t<Key_t, Value_t>());
	}
	~btree_t(){ }

	inline void yield(int count);

	int check_height();

	void insert(Key_t key, Value_t value);
	/* this function is called when root has been split by another threads */
	void insert_key(Key_t key, node_t* value, node_t* prev);

	bool update(Key_t key, Value_t value);

	Value_t lookup(Key_t key);

	inode_t<Key_t>** new_root_for_adjustment(Key_t* key, node_t** value, int num, int& new_num);

	void batch_insert(Key_t* key, node_t** value, int num, node_t* prev);

	int range_lookup(Key_t min_key, int range, Value_t* buf);

	bool convert(lnode_t<Key_t, Value_t>* leaf, uint64_t version);
	
	void convert_all();

	void print_leaf();

	void print_internal();

	void print();

	void sanity_check();

	Value_t find_anyway(Key_t key);

	double utilization();

	int height();

    private:
	node_t* root;
};
}
#endif
