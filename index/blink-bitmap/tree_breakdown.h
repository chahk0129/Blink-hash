#ifndef TREE_H__
#define TREE_H__
#include "node_breakdown.h"
#include <stack>

template <typename Key_t>
class btree_t{
    public:
	//std::atomic<uint64_t> global_sync_time;
	/* sync times : try_readlock, try_readunlock, try_writelock, write_unlock */
	std::atomic<uint64_t> global_try_readlock_time_success;
	std::atomic<uint64_t> global_try_readlock_time_fail;
	std::atomic<uint64_t> global_try_readunlock_time_success;
	std::atomic<uint64_t> global_try_readunlock_time_fail;
	std::atomic<uint64_t> global_try_writelock_time_success;
	std::atomic<uint64_t> global_try_writelock_time_fail;
	std::atomic<uint64_t> global_writeunlock_time;

	//std::atomic<uint64_t> global_writing_time;
	/* writing overhead: sorting, split, alloc*/
	std::atomic<uint64_t> global_sorting_time;
	std::atomic<uint64_t> global_split_time;
	std::atomic<uint64_t> global_writing_time;
	std::atomic<uint64_t> global_alloc_time;

	//std::atomic<uint64_t> global_traversing_time;
	/* traversing time: key_comparison, ptr_dereference */
	std::atomic<uint64_t> global_key_comparison_time;
	std::atomic<uint64_t> global_ptr_dereference_time;


	inline uint64_t _rdtsc(){
	    uint32_t lo, hi;
	    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
	    return (((uint64_t)hi << 32) | lo);
	}

	btree_t(){ 
	    root = static_cast<node_t*>(new lnode_t<Key_t>()); 

	    global_try_readlock_time_success.store(0, std::memory_order_relaxed);
	    global_try_readunlock_time_success.store(0, std::memory_order_relaxed);
	    global_try_writelock_time_success.store(0, std::memory_order_relaxed);
	    global_try_readlock_time_fail.store(0, std::memory_order_relaxed);
	    global_try_readunlock_time_fail.store(0, std::memory_order_relaxed);
	    global_try_writelock_time_fail.store(0, std::memory_order_relaxed);
	    global_writeunlock_time.store(0, std::memory_order_relaxed);

	    global_sorting_time.store(0, std::memory_order_relaxed);
	    global_alloc_time.store(0, std::memory_order_relaxed);
	    global_writing_time.store(0, std::memory_order_relaxed);
	    global_split_time.store(0, std::memory_order_relaxed);

	    global_key_comparison_time.store(0, std::memory_order_relaxed);
	    global_ptr_dereference_time.store(0, std::memory_order_relaxed);

	    /*
	    global_sync_time.store(0, std::memory_order_relaxed);
	    global_writing_time.store(0, std::memory_order_relaxed);
	    global_traversing_time.store(0, std::memory_order_relaxed);
	    */
	}
	~btree_t(){ }

	int check_height(){
	    return root->level;
	}
	//void update_global_time(uint64_t alloc_time, uint64_t sync_time, uint64_t writing_time, uint64_t traversing_time){
	void update_global_time(uint64_t try_readlock_time_s, uint64_t try_readlock_time_f, uint64_t try_readunlock_time_s, uint64_t try_readunlock_time_f, uint64_t try_writelock_time_s, uint64_t try_writelock_time_f, uint64_t writeunlock_time, uint64_t sorting_time, uint64_t alloc_time, uint64_t writing_time, uint64_t split_time, uint64_t key_comparison_time, uint64_t ptr_dereference_time){

	    global_try_readlock_time_success.fetch_add(try_readlock_time_s, std::memory_order_relaxed);
	    global_try_readlock_time_fail.fetch_add(try_readlock_time_f, std::memory_order_relaxed);
	    global_try_readunlock_time_success.fetch_add(try_readunlock_time_s, std::memory_order_relaxed);
	    global_try_readunlock_time_fail.fetch_add(try_readunlock_time_f, std::memory_order_relaxed);
	    global_try_writelock_time_success.fetch_add(try_writelock_time_s, std::memory_order_relaxed);
	    global_try_writelock_time_fail.fetch_add(try_writelock_time_f, std::memory_order_relaxed);
	    global_writeunlock_time.fetch_add(writeunlock_time, std::memory_order_relaxed);

	    global_sorting_time.fetch_add(sorting_time, std::memory_order_relaxed);
	    global_alloc_time.fetch_add(alloc_time, std::memory_order_relaxed);
	    global_writing_time.fetch_add(writing_time, std::memory_order_relaxed);
	    global_split_time.fetch_add(split_time, std::memory_order_relaxed);

	    global_key_comparison_time.fetch_add(key_comparison_time, std::memory_order_relaxed);
	    global_ptr_dereference_time.fetch_add(ptr_dereference_time, std::memory_order_relaxed);

	    /*
	    global_sync_time.fetch_add(sync_time, std::memory_order_relaxed);
	    global_alloc_time.fetch_add(alloc_time, std::memory_order_relaxed);
	    global_writing_time.fetch_add(writing_time, std::memory_order_relaxed);

	    traversing_time = traversing_time - alloc_time - sync_time - writing_time;
	    global_traversing_time.fetch_add(traversing_time, std::memory_order_relaxed);
	    */
	}

	void reset_global_time(){

	    global_try_readlock_time_success.store(0, std::memory_order_relaxed);
	    global_try_readunlock_time_success.store(0, std::memory_order_relaxed);
	    global_try_writelock_time_success.store(0, std::memory_order_relaxed);
	    global_try_readlock_time_fail.store(0, std::memory_order_relaxed);
	    global_try_readunlock_time_fail.store(0, std::memory_order_relaxed);
	    global_try_writelock_time_fail.store(0, std::memory_order_relaxed);
	    global_writeunlock_time.store(0, std::memory_order_relaxed);

	    global_sorting_time.store(0, std::memory_order_relaxed);
	    global_alloc_time.store(0, std::memory_order_relaxed);
	    global_writing_time.store(0, std::memory_order_relaxed);
	    global_split_time.store(0, std::memory_order_relaxed);

	    global_key_comparison_time.store(0, std::memory_order_relaxed);
	    global_ptr_dereference_time.store(0, std::memory_order_relaxed);

/*
	    global_alloc_time.store(0, std::memory_order_relaxed);
	    global_sync_time.store(0, std::memory_order_relaxed);
	    global_writing_time.store(0, std::memory_order_relaxed);
	    global_traversing_time.store(0, std::memory_order_relaxed);
	    */
	}

	void insert(Key_t key, uint64_t value){
	    uint64_t try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, sorting_time, split_time, alloc_time, key_comparison_time, ptr_dereference_time, writing_time;
	    try_readlock_time_s = try_readlock_time_f = try_readunlock_time_s = try_readunlock_time_f = try_writelock_time_s = try_writelock_time_f = writeunlock_time = sorting_time = split_time = alloc_time = key_comparison_time = ptr_dereference_time = writing_time = 0;

	    uint64_t start, end;

	    auto cur = root;

	    int stack_cnt = 0;
	    inode_t<Key_t>* stack[root->level];

	restart:
	    bool need_restart = false;
	    start = _rdtsc();
	    auto cur_vstart = cur->try_readlock(need_restart);
	    end = _rdtsc();
	    if(need_restart){
		try_readlock_time_f += (end - start);
		goto restart;
	    }
	    try_readlock_time_s += (end - start);

	    // tree traversal
	    while(cur->leftmost_ptr != nullptr){
		uint64_t key_comp;
		start = _rdtsc();
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key, key_comp);
		end = _rdtsc();
		key_comparison_time += key_comp;
		ptr_dereference_time += (end - start - key_comp);

		start = _rdtsc();
		auto child_vstart = child->try_readlock(need_restart);
		end = _rdtsc();
		if(need_restart){
		    try_readlock_time_f += (end - start);
		    goto restart;
		}
		try_readlock_time_s = (end - start);

		start = _rdtsc();
		auto cur_vend = cur->get_version(need_restart);
		end = _rdtsc();
		if(need_restart || (cur_vstart != cur_vend)){
		    try_readunlock_time_f += (end - start);
		    goto restart;
		}
		try_readunlock_time_s += (end - start);

		if(child != cur->sibling_ptr)
		    stack[stack_cnt++] = static_cast<inode_t<Key_t>*>(cur);

		start = _rdtsc();
		cur = child;
		end = _rdtsc();
		ptr_dereference_time += (end - start);
		cur_vstart = child_vstart;
	    }

	    // found leaf
	    start = _rdtsc();
	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);
	    end = _rdtsc();
	    ptr_dereference_time += (end - start);

	leaf_restart:
	    need_restart = false;
	    start = _rdtsc();
	    auto leaf_vstart = leaf->try_readlock(need_restart);
	    end = _rdtsc();
	    if(need_restart){
		try_readlock_time_f += (end - start);
		goto leaf_restart;
	    }
	    try_readlock_time_s += (end - start);

	    start = _rdtsc();
	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		end = _rdtsc();
		key_comparison_time += (end - start);

		start = _rdtsc();
		auto sibling = static_cast<lnode_t<Key_t>*>(leaf->sibling_ptr);
		end = _rdtsc();
		ptr_dereference_time += (end - start);

		auto sibling_v = sibling->try_readlock(need_restart);
		end = _rdtsc();
		if(need_restart){
		    try_readlock_time_f += (end - start);
		    goto leaf_restart;
		}
		try_readlock_time_s += (end - start);
		
		start = _rdtsc();
		auto leaf_vend = leaf->get_version(need_restart);
		end = _rdtsc();
		if(need_restart || (leaf_vstart != leaf_vend)){
		    try_readunlock_time_f += (end - start);
		    goto leaf_restart;
		}
		try_readunlock_time_s += (end - start);
		
		start = _rdtsc();
		leaf = sibling;
		end = _rdtsc();
		ptr_dereference_time += (end - start);
		leaf_vstart = sibling_v;
		start = _rdtsc();
	    }

	    start = _rdtsc();
	    leaf->try_upgrade_writelock(leaf_vstart, need_restart);
	    end = _rdtsc();
	    if(need_restart){
		try_writelock_time_f += (end - start);
		goto leaf_restart;
	    }
	    try_writelock_time_s += (end - start);

	    if(!leaf->is_full()){ // normal insert
		leaf->insert(key, value, key_comparison_time, sorting_time, writing_time);
		start = _rdtsc();
		leaf->write_unlock();
		end = _rdtsc();
		writeunlock_time += (end - start);
		update_global_time(try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, sorting_time, alloc_time, writing_time, split_time, key_comparison_time, ptr_dereference_time); 
		return;
	    }
	    else{ // leaf node split
		Key_t split_key; 
		start = _rdtsc();
		uint64_t _alloc_time;
		auto new_leaf = leaf->split(split_key, _alloc_time);
		end = _rdtsc();
		alloc_time += _alloc_time;
		split_time += (end - start) - _alloc_time;

		start = _rdtsc();
		if(key <= split_key)
		    leaf->insert(key, value, key_comparison_time, sorting_time, writing_time);
		else
		    new_leaf->insert(key, value, key_comparison_time, sorting_time, writing_time);

		if(stack_cnt){
		    int stack_idx = stack_cnt-1;
		    auto old_parent = stack[stack_idx];

		    auto original_node = static_cast<node_t*>(leaf);
		    auto new_node = static_cast<node_t*>(new_leaf);
		    //auto high_key = new_leaf->high_key;

		    while(stack_idx > -1){ // backtrack
			old_parent = stack[stack_idx];

		    parent_restart:
			need_restart = false;
			start = _rdtsc();
			auto parent_vstart = old_parent->try_readlock(need_restart);
			end = _rdtsc();
			if(need_restart){
			    try_readlock_time_f += (end - start);
			    goto parent_restart;
			}
			try_readlock_time_s += (end - start);
			
			start = _rdtsc();
			while(old_parent->sibling_ptr && (old_parent->high_key < split_key)){
			    end = _rdtsc();
			    key_comparison_time += (end - start);
			    start = _rdtsc();
			    auto p_sibling = old_parent->sibling_ptr;
			    end = _rdtsc();
			    ptr_dereference_time += (end - start);

			    auto p_sibling_v = p_sibling->try_readlock(need_restart);
			    end = _rdtsc();
			    if(need_restart){
				try_readlock_time_f += (end - start);
				goto parent_restart;
			    }
			    try_readlock_time_s += (end - start);

			    start = _rdtsc();
			    auto parent_vend = old_parent->get_version(need_restart);
			    end = _rdtsc();
			    if(need_restart || (parent_vstart != parent_vend)){
				try_readunlock_time_f += (end - start);
				goto parent_restart;
			    }
			    try_readunlock_time_s += (end - start);

			    start = _rdtsc();
			    old_parent = static_cast<inode_t<Key_t>*>(p_sibling);
			    end = _rdtsc();
			    ptr_dereference_time += (end - start);
			    parent_vstart = p_sibling_v;
			    start = _rdtsc();
			}

			start = _rdtsc();
			old_parent->try_upgrade_writelock(parent_vstart, need_restart);
			end = _rdtsc();
			if(need_restart){
			    try_writelock_time_f += (end - start);
			    goto parent_restart;
			}
			try_writelock_time_s += (end - start);

			start = _rdtsc();
			original_node->write_unlock();
			end = _rdtsc();
			writeunlock_time += (end - start);

			if(!old_parent->is_full()){ // normal insert
			    old_parent->insert(split_key, new_node, key_comparison_time, sorting_time, writing_time);
			    start = _rdtsc();
			    old_parent->write_unlock();
			    end = _rdtsc();
			    writeunlock_time += (end - start);
			    update_global_time(try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, sorting_time, alloc_time, writing_time, split_time, key_comparison_time, ptr_dereference_time); 
			    return;
			}

			// internal node split
			Key_t _split_key;
			start = _rdtsc();
			auto new_parent = old_parent->split(_split_key, _alloc_time);
			end = _rdtsc();
			alloc_time += _alloc_time;
			split_time += (end - start - _alloc_time);
			if(split_key <= _split_key)
			    old_parent->insert(split_key, new_node, key_comparison_time, sorting_time, writing_time);
			else
			    new_parent->insert(split_key, new_node, key_comparison_time, sorting_time, writing_time);

			if(stack_idx){
			    original_node = static_cast<node_t*>(old_parent);
			    new_node = static_cast<node_t*>(new_parent);
			    split_key = _split_key;
			    old_parent = stack[--stack_idx];
			}
			else{ // set new root
			    if(old_parent == root){ // current node is root
				start = _rdtsc();
				auto new_root = new inode_t<Key_t>(_split_key, old_parent, new_parent, nullptr, old_parent->level+1, new_parent->high_key);
				end = _rdtsc();
				alloc_time += (end - start);
				start = _rdtsc();
				root = static_cast<node_t*>(new_root);
				end = _rdtsc();
				writing_time += (end - start);
				start = _rdtsc();
				old_parent->write_unlock();
				end = _rdtsc();
				writeunlock_time += (end - start);
				update_global_time(try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, sorting_time, alloc_time, writing_time, split_time, key_comparison_time, ptr_dereference_time); 
				return;
			    }
			    else{ // other thread has already created a new root
				insert_key(_split_key, new_parent, old_parent, try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, key_comparison_time, split_time, sorting_time, writing_time, ptr_dereference_time, alloc_time);
				update_global_time(try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, sorting_time, alloc_time, writing_time, split_time, key_comparison_time, ptr_dereference_time); 
				return;
			    }
			}
		    }
		}
		else{ // set new root
		    if(root == leaf){ // current node is root
			start = _rdtsc();
			auto new_root = new inode_t<Key_t>(split_key, leaf, new_leaf, nullptr, root->level+1, new_leaf->high_key);
			end = _rdtsc();
			alloc_time += (end - start);
			start = _rdtsc();
			root = static_cast<node_t*>(new_root);
			end = _rdtsc();
			writing_time += (end - start);
			start = _rdtsc();
			leaf->write_unlock();
			end = _rdtsc();
			writeunlock_time += (end - start);
			update_global_time(try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, sorting_time, alloc_time, writing_time, split_time, key_comparison_time, ptr_dereference_time); 
			return;
		    }
		    else{ // other thread has already created a new root
			insert_key(split_key, new_leaf, leaf, try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, key_comparison_time, split_time, sorting_time, writing_time, ptr_dereference_time, alloc_time);
			update_global_time(try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, sorting_time, alloc_time, writing_time, split_time, key_comparison_time, ptr_dereference_time); 
			return;
		    }
		}
	    }
	}

	/* this function is called when root has been split by another threads */
	void insert_key(Key_t key, node_t* value, node_t* prev, uint64_t& try_readlock_time_s, uint64_t& try_readlock_time_f, uint64_t& try_readunlock_time_s, uint64_t& try_readunlock_time_f, uint64_t& try_writelock_time_s, uint64_t& try_writelock_time_f, uint64_t& writeunlock_time, uint64_t& key_comparison_time, uint64_t& split_time, uint64_t& sorting_time, uint64_t& writing_time, uint64_t& ptr_dereference_time, uint64_t& alloc_time){
	    uint64_t start, end;

	restart:
	    bool need_restart = false;
	    auto cur = root;

	    start = _rdtsc();
	    auto cur_vstart = cur->try_readlock(need_restart);
	    end = _rdtsc();
	    if(need_restart){
		try_readlock_time_f += (end - start);
		goto restart;
	    }
	    try_readlock_time_s += (end - start);

	    // since we need to find exact internal node which has been previously the root, we use readlock for traversal
	    while(cur != prev){
		uint64_t key_comp;
		start = _rdtsc();
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key, key_comp);
		end = _rdtsc();
		key_comparison_time += key_comp;
		ptr_dereference_time += (end - start - key_comp);

		start = _rdtsc();
		auto child_vstart = child->try_readlock(need_restart);
		end = _rdtsc();
		if(need_restart){
		    try_readlock_time_f += (end - start);
		    goto restart;
		}
		try_readlock_time_s += (end - start);

		start = _rdtsc();
		auto cur_vend = cur->get_version(need_restart);
		end = _rdtsc();
		if(need_restart || (cur_vstart != cur_vend)){
		    try_readunlock_time_f += (end - start);
		    goto restart;
		}
		try_readunlock_time_s += (end - start);

		start = _rdtsc();
		cur = child;
		end = _rdtsc();
		ptr_dereference_time += (end - start);
		cur_vstart = child_vstart;
	    }

	    start = _rdtsc();
	    cur->try_upgrade_writelock(cur_vstart, need_restart);
	    end = _rdtsc();
	    if(need_restart){
		try_writelock_time_f += (end - start);
		goto restart;
	    }
	    try_writelock_time_s += (end - start);
	    prev->write_unlock();
	    start = _rdtsc();
	    writeunlock_time += (start - end);

	    auto node = static_cast<inode_t<Key_t>*>(cur);
	    
	    if(!node->is_full()){
		node->insert(key, value, key_comparison_time, sorting_time, writing_time);
		start = _rdtsc();
		node->write_unlock();
		end = _rdtsc();
		writeunlock_time += (end - start);
		return;
	    }
	    else{
		Key_t split_key;
		uint64_t _alloc_time = 0;
		start = _rdtsc();
		auto new_node = node->split(split_key, _alloc_time);
		end = _rdtsc();
		alloc_time += _alloc_time;
		split_time += (end - start - _alloc_time);
		if(key <= split_key)
		    node->insert(key, value, key_comparison_time, sorting_time, writing_time);
		else
		    new_node->insert(key, value, key_comparison_time, sorting_time, writing_time);

		if(node == root){ // if current nodes is root
		    start = _rdtsc();
		    auto new_root = new inode_t<Key_t>(split_key, node, new_node, nullptr, node->level+1, new_node->high_key);
		    end = _rdtsc();
		    alloc_time += (end - start);
		    start = _rdtsc();
		    root = static_cast<node_t*>(new_root);
		    end = _rdtsc();
		    writing_time += (end - start);
		    start = _rdtsc();
		    node->write_unlock();
		    end = _rdtsc();
		    writeunlock_time += (end - start);
		    return;
		}
		else{ // other thread has already created a new root
		    insert_key(split_key, new_node, node, try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, key_comparison_time, split_time, sorting_time, writing_time, ptr_dereference_time, alloc_time);
		    return;
		}
	    }
	}



	bool update(Key_t key, uint64_t value){
	    uint64_t try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, sorting_time, split_time, alloc_time, key_comparison_time, ptr_dereference_time, writing_time;
	    try_readlock_time_s = try_readlock_time_f = try_readunlock_time_s = try_readunlock_time_f = try_writelock_time_s = try_writelock_time_f = writeunlock_time = sorting_time = split_time = alloc_time = key_comparison_time = ptr_dereference_time = writing_time = 0;
	    uint64_t start, end;

	    auto cur = root;

	restart:
	    bool need_restart = false;
	    start = _rdtsc();
	    auto cur_vstart = cur->try_readlock(need_restart);
	    end = _rdtsc();
	    if(need_restart){
		try_readlock_time_f += (end - start);
		goto restart;
	    }
	    try_readlock_time_s += (end - start);

	    // traversal
	    while(cur->leftmost_ptr != nullptr){
		uint64_t key_comp;
		start = _rdtsc();
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key, key_comp);
		end = _rdtsc();
		key_comparison_time += key_comp;
		ptr_dereference_time += (end - start - key_comp);

		start = _rdtsc();
		auto child_vstart = child->try_readlock(need_restart);
		end = _rdtsc();
		if(need_restart){
		    try_readlock_time_f += (end - start);
		    goto restart;
		}
		try_readlock_time_s += (end - start);

		start = _rdtsc();
		auto cur_vend = cur->get_version(need_restart);
		end = _rdtsc();
		if(need_restart || (cur_vstart != cur_vend)){
		    try_readunlock_time_f += (end - start);
		    goto restart;
		}
		try_readunlock_time_s += (end - start);

		start = _rdtsc();
		cur = child;
		end = _rdtsc();
		ptr_dereference_time += (end - start);
		cur_vstart = child_vstart;
	    }

	    // found leaf
	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);

	leaf_restart:
	    need_restart = false;
	    start = _rdtsc();
	    auto leaf_vstart = leaf->try_readlock(need_restart);
	    end = _rdtsc();
	    if(need_restart){
		try_readlock_time_f += (end - start);
		goto leaf_restart;
	    }
	    try_readlock_time_s += (end - start);

	    start = _rdtsc();
	    // move right if necessary
	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		end = _rdtsc();
		key_comparison_time += (end - start);

		start = _rdtsc();
		auto sibling = leaf->sibling_ptr;
		end = _rdtsc();
		ptr_dereference_time += (end - start);
		
		start = _rdtsc();
		auto sibling_v = sibling->try_readlock(need_restart);
		end = _rdtsc();
		if(need_restart){
		    try_readlock_time_f += (end - start);
		    goto leaf_restart;
		}
		try_readlock_time_s += (end - start);

		start = _rdtsc();
		auto leaf_vend = leaf->get_version(need_restart);
		end = _rdtsc();
		if(need_restart || (leaf_vstart != leaf_vend)){
		    try_readunlock_time_f += (end - start);
		    goto leaf_restart;
		}
		try_readunlock_time_s += (end - start);

		start = _rdtsc();
		leaf = static_cast<lnode_t<Key_t>*>(sibling);
		end = _rdtsc();
		ptr_dereference_time += (end - start);
		leaf_vstart = sibling_v;
		start = _rdtsc();
	    }

	    start = _rdtsc();
	    leaf->try_upgrade_writelock(leaf_vstart, need_restart);
	    end = _rdtsc();
	    if(need_restart){
		try_writelock_time_f += (end - start);
		goto leaf_restart;
	    }
	    try_writelock_time_s += (end - start);


	    bool ret = leaf->update(key, value, key_comparison_time, writing_time);
	    start = _rdtsc();
	    leaf->write_unlock();
	    end = _rdtsc();
	    writeunlock_time += (end - start);
	    update_global_time(try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, sorting_time, alloc_time, writing_time, split_time, key_comparison_time, ptr_dereference_time); 
	    return ret;
	}



	uint64_t lookup(Key_t key){
	    uint64_t try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, sorting_time, split_time, alloc_time, key_comparison_time, ptr_dereference_time, writing_time;
	    try_readlock_time_s = try_readlock_time_f = try_readunlock_time_s = try_readunlock_time_f = try_writelock_time_s = try_writelock_time_f = writeunlock_time = sorting_time = split_time = alloc_time = key_comparison_time = ptr_dereference_time = writing_time = 0;
	    uint64_t start, end;

	    auto cur = root;

	restart:
	    bool need_restart = false;

	    start = _rdtsc();
	    auto cur_vstart = cur->try_readlock(need_restart);
	    end = _rdtsc();
	    if(need_restart){
		try_readlock_time_f += (end - start);
		goto restart;
	    }
	    try_readlock_time_s += (end - start);

	    // traversal
	    while(cur->leftmost_ptr != nullptr){
		uint64_t key_comp;
		start = _rdtsc();
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key, key_comp);
		end = _rdtsc();
		key_comparison_time += key_comp;
		ptr_dereference_time += (end - start - key_comp);
		start = _rdtsc();
		auto child_vstart = child->try_readlock(need_restart);
		end = _rdtsc();
		if(need_restart){
		    try_readlock_time_f += (end - start);
		    goto restart;
		}
		try_readlock_time_s += (end - start);

		start = _rdtsc();
		auto cur_vend = cur->get_version(need_restart);
		end = _rdtsc();
		if(need_restart || (cur_vstart != cur_vend)){
		    try_readunlock_time_f += (end - start);
		    goto restart;
		}
		try_readunlock_time_s += (end - start);

		start = _rdtsc();
		cur = child;
		end = _rdtsc();
		ptr_dereference_time += (end - start);
		cur_vstart = child_vstart;
	    }

	    // found leaf
	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);

	leaf_restart:
	    need_restart = false;
	    start = _rdtsc();
	    auto leaf_vstart = leaf->try_readlock(need_restart);
	    end = _rdtsc();
	    if(need_restart){ 
		try_readlock_time_f += (end - start);
		goto leaf_restart;
	    }
	    try_readlock_time_s += (end - start);

	    start = _rdtsc();
	    // move right if necessary
	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		end = _rdtsc();
		key_comparison_time += (end - start);
		
		start = _rdtsc();
		auto sibling = leaf->sibling_ptr;
		end = _rdtsc();
		ptr_dereference_time += (end - start);

		start = _rdtsc();
		auto sibling_v = sibling->try_readlock(need_restart);
		end = _rdtsc();
		if(need_restart){
		    try_readlock_time_f += (end - start);
		    goto leaf_restart;
		}
		try_readlock_time_s += (end - start);

		start = _rdtsc();
		auto leaf_vend = leaf->get_version(need_restart);
		end = _rdtsc();
		if(need_restart || (leaf_vstart != leaf_vend)){
		    try_readunlock_time_f += (end - start);
		    goto leaf_restart;
		}
		try_readunlock_time_s += (end - start);

		start = _rdtsc();
		leaf = static_cast<lnode_t<Key_t>*>(sibling);
		end = _rdtsc();
		ptr_dereference_time += (end - start);
		leaf_vstart = sibling_v;
		start = _rdtsc();
	    }

	    start = _rdtsc();
	    auto ret = leaf->find(key);
	    end = _rdtsc();
	    key_comparison_time += (end - start);

	    start = _rdtsc();
	    auto leaf_vend = leaf->get_version(need_restart);
	    end = _rdtsc();
	    if(need_restart || (leaf_vstart != leaf_vend)){
		try_readunlock_time_f += (end - start);
		goto leaf_restart;
	    }
	    try_readunlock_time_s += (end - start);

	    update_global_time(try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, sorting_time, alloc_time, writing_time, split_time, key_comparison_time, ptr_dereference_time); 
	    return ret;
	}


	int range_lookup(Key_t min_key, int range, uint64_t* buf){
	    uint64_t try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, sorting_time, split_time, alloc_time, key_comparison_time, ptr_dereference_time, writing_time;
	    try_readlock_time_s = try_readlock_time_f = try_readunlock_time_s = try_readunlock_time_f = try_writelock_time_s = try_writelock_time_f = writeunlock_time = sorting_time = split_time = alloc_time = key_comparison_time = ptr_dereference_time = writing_time = 0;
	    uint64_t start, end;

	restart:
	    auto cur = root;
	    bool need_restart = false;
	    start = _rdtsc();
	    auto cur_vstart = cur->try_readlock(need_restart);
	    end = _rdtsc();
	    if(need_restart){
		try_readlock_time_f += (end - start);
		goto restart;
	    }
	    try_readlock_time_s += (end - start);

	    // traversal
	    while(cur->leftmost_ptr != nullptr){
		uint64_t key_comp;
		start = _rdtsc();
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(min_key, key_comp);
		end = _rdtsc();
		key_comparison_time += (key_comp);
		ptr_dereference_time += (end - start - key_comp);

		start = _rdtsc();
		auto child_vstart = child->try_readlock(need_restart);
		end = _rdtsc();
		if(need_restart){
		    try_readlock_time_f += (end - start);
		    goto restart;
		}
		try_readlock_time_s += (end - start);

		start = _rdtsc();
		auto cur_vend = cur->get_version(need_restart);
		end = _rdtsc();
		if(need_restart || (cur_vstart != cur_vend)){
		    try_readunlock_time_f += (end - start);
		    goto restart;
		}
		try_readunlock_time_s += (end - start);

		start = _rdtsc();
		cur = child;
		end = _rdtsc();
		ptr_dereference_time += (end - start);
		cur_vstart = child_vstart;
	    }

	    // found leaf
	    int count = 0;
	    start = _rdtsc();
	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);
	    end = _rdtsc();
	    ptr_dereference_time += (end - start);

	    start = _rdtsc();
	    auto leaf_vstart = leaf->try_readlock(need_restart);
	    end = _rdtsc();
	    if(need_restart){
		try_readlock_time_f += (end - start);
		goto restart;
	    }
	    try_readlock_time_s += (end - start);

	    while(count < range){

		// move right if necessary
		start = _rdtsc();
		while(leaf->sibling_ptr && (leaf->high_key < min_key)){
		    end = _rdtsc();
		    key_comparison_time += (end - start);
		    start = _rdtsc();
		    auto sibling = leaf->sibling_ptr;
		    end = _rdtsc();
		    ptr_dereference_time += (end - start);

		    start = _rdtsc();
		    auto sibling_v = sibling->try_readlock(need_restart);
		    end = _rdtsc();
		    if(need_restart){
			try_readlock_time_f += (end - start);
			goto restart;
		    }
		    try_readlock_time_s += (end - start);

		    start = _rdtsc();
		    auto leaf_vend = leaf->get_version(need_restart);
		    end = _rdtsc();
		    if(need_restart || (leaf_vstart != leaf_vend)){
			try_readunlock_time_f += (end - start);
			goto restart;
		    }
		    try_readunlock_time_s += (end - start);

		    start = _rdtsc();
		    leaf = static_cast<lnode_t<Key_t>*>(sibling);
		    end = _rdtsc();
		    ptr_dereference_time += (end - start);
		    leaf_vstart = sibling_v;
		    start = _rdtsc();
		}

		start = _rdtsc();
		auto ret = leaf->range_lookup(min_key, buf, count, range, key_comparison_time, ptr_dereference_time);
		end = _rdtsc();
		key_comparison_time += (end - start);
		start = _rdtsc();
		auto sibling = leaf->sibling_ptr;
		end = _rdtsc();
		ptr_dereference_time += (end - start);

		start = _rdtsc();
		auto leaf_vend = leaf->get_version(need_restart);
		end = _rdtsc();
		if(need_restart || (leaf_vstart != leaf_vend)){
		    try_readunlock_time_f += (end - start);
		    goto restart;
		}
		try_readunlock_time_s += (end - start);

		if(ret == range){
		    update_global_time(try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, sorting_time, alloc_time, writing_time, split_time, key_comparison_time, ptr_dereference_time); 
		    return ret;
		}

		if(!sibling) break;

		start = _rdtsc();
		auto sibling_vstart = sibling->try_readlock(need_restart);
		end = _rdtsc();
		if(need_restart){
		    try_readlock_time_f += (end - start);
		    goto restart;
		}
		try_readlock_time_s += (end - start);

		start = _rdtsc();
		leaf = static_cast<lnode_t<Key_t>*>(sibling);
		end = _rdtsc();
		ptr_dereference_time += (end - start);
		leaf_vstart = sibling_vstart;
		count = ret;

	    }
	    update_global_time(try_readlock_time_s, try_readlock_time_f, try_readunlock_time_s, try_readunlock_time_f, try_writelock_time_s, try_writelock_time_f, writeunlock_time, sorting_time, alloc_time, writing_time, split_time, key_comparison_time, ptr_dereference_time); 
	    return count;
	}

	void print_leaf(){
	    auto cur = root;
	    while(cur->leftmost_ptr != nullptr){
		cur = cur->leftmost_ptr;
	    }
	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);
	    int cnt = 1;
	    do{
		std::cout << "L" << cnt << "(" << leaf << ": ";
		leaf->print();
		cnt++;
	    }while((leaf = static_cast<lnode_t<Key_t>*>(leaf->sibling_ptr)));
	}

	void print_internal(){
	    auto cur = static_cast<inode_t<Key_t>*>(root);
	    auto internal = cur;
	    int level = 0;
	    int cnt = 1;
	    while(cur->leftmost_ptr != nullptr){
		std::cout << "level " << level << std::endl;
		internal = cur;
		do{
		    std::cout << "I" << cnt << "(" << cur << "): ";
		    cur->print();
		    cnt++;
		}while((cur = static_cast<inode_t<Key_t>*>(cur->sibling_ptr)));
		level++;
		cur = internal;
		cur = static_cast<inode_t<Key_t>*>(cur->leftmost_ptr);
	    }
	}

	void sanity_check(){
	    auto cur = root;
	    while(cur->leftmost_ptr != nullptr){
		auto p = static_cast<inode_t<Key_t>*>(cur);
		p->sanity_check(p->high_key, true);
		cur = cur->leftmost_ptr;
	    }

	    auto l = static_cast<lnode_t<Key_t>*>(cur);
	    l->sanity_check(l->high_key, true);
	}

	uint64_t find_anyway(Key_t key){
	    auto cur = root;
	    while(cur->leftmost_ptr != nullptr){
		cur = cur->leftmost_ptr;
	    }

	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);
	    lnode_t<Key_t>* before;
	    do{
		auto ret = leaf->find(key);
		if(ret){
		    std::cout << "before node(" << before << ")" << std::endl;
		    before->print();
		    std::cout << "current node(" << leaf << ")" << std::endl;
		    leaf->print();
		    return ret; 
		}
		before = leaf;
		leaf = static_cast<lnode_t<Key_t>*>(leaf->sibling_ptr);
	    }while(leaf);

	    return 0;
	}

    private:
	node_t* root;
};
#endif
