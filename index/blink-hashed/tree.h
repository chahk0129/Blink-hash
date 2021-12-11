#ifndef TREE_HASHED_H__
#define TREE_HASHED_H__
/*
#ifdef EXP
#include "node.h"
#elif defined LINKED
//#include "linked_node_spinlock.h"
#include "linked_node.h"
#else
#include "atomic_node.h"
#endif
*/
#include "linked_node.h"
namespace BLINK_HASHED{

template <typename Key_t>
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

#ifdef BREAKDOWN
	std::atomic<uint64_t> time_inode_traversal;
	std::atomic<uint64_t> time_inode_allocation;
	std::atomic<uint64_t> time_inode_write;
	std::atomic<uint64_t> time_inode_sync;
	std::atomic<uint64_t> time_inode_split;

	std::atomic<uint64_t> time_lnode_traversal;
	std::atomic<uint64_t> time_lnode_allocation;
	std::atomic<uint64_t> time_lnode_write;
	std::atomic<uint64_t> time_lnode_sync;
	std::atomic<uint64_t> time_lnode_split;

	std::atomic<uint64_t> time_lnode_key_copy;
	std::atomic<uint64_t> time_lnode_find_median;
	std::atomic<uint64_t> time_lnode_copy;
	std::atomic<uint64_t> time_lnode_update;

#endif

	btree_t(){ 
	    invalid_initialize<Key_t>();

	    root = static_cast<node_t*>(new lnode_t<Key_t>()); 

#ifdef BREAKDOWN
	    time_inode_traversal.store(0, std::memory_order_relaxed);
	    time_inode_allocation.store(0, std::memory_order_relaxed);
	    time_inode_write.store(0, std::memory_order_relaxed);
	    time_inode_sync.store(0, std::memory_order_relaxed);
	    time_inode_split.store(0, std::memory_order_relaxed);
	    time_lnode_traversal.store(0, std::memory_order_relaxed);
	    time_lnode_allocation.store(0, std::memory_order_relaxed);
	    time_lnode_write.store(0, std::memory_order_relaxed);
	    time_lnode_sync.store(0, std::memory_order_relaxed);
	    time_lnode_split.store(0, std::memory_order_relaxed);

	    time_lnode_key_copy.store(0, std::memory_order_relaxed);
	    time_lnode_find_median.store(0, std::memory_order_relaxed);
	    time_lnode_copy.store(0, std::memory_order_relaxed);
	    time_lnode_update.store(0, std::memory_order_relaxed);
#endif
	}
	~btree_t(){ }

	void yield(int count){
	    if(count > 3)
		sched_yield();
	}

	int check_height(){
	    return root->level;
	}

#ifdef BREAKDOWN
	void update_breakdown(uint64_t inode_traversal, uint64_t inode_alloc, uint64_t inode_write, uint64_t inode_sync, uint64_t inode_split, uint64_t lnode_traversal, uint64_t lnode_alloc, uint64_t lnode_write, uint64_t lnode_sync, uint64_t lnode_split, uint64_t lnode_key_copy, uint64_t lnode_find_median, uint64_t lnode_copy, uint64_t lnode_update){
	//void update_breakdown(uint64_t inode_traversal, uint64_t inode_alloc, uint64_t inode_write, uint64_t inode_sync, uint64_t inode_split, uint64_t lnode_traversal, uint64_t lnode_alloc, uint64_t lnode_write, uint64_t lnode_sync, uint64_t lnode_split){
	    time_inode_traversal.fetch_add(inode_traversal, std::memory_order_relaxed);
	    time_inode_allocation.fetch_add(inode_alloc, std::memory_order_relaxed);
	    time_inode_write.fetch_add(inode_write, std::memory_order_relaxed);
	    time_inode_sync.fetch_add(inode_sync, std::memory_order_relaxed);
	    time_inode_split.fetch_add(inode_split, std::memory_order_relaxed);
	    time_lnode_traversal.fetch_add(lnode_traversal, std::memory_order_relaxed);
	    time_lnode_allocation.fetch_add(lnode_alloc, std::memory_order_relaxed);
	    time_lnode_write.fetch_add(lnode_write, std::memory_order_relaxed);
	    time_lnode_sync.fetch_add(lnode_sync, std::memory_order_relaxed);
	    time_lnode_split.fetch_add(0, std::memory_order_relaxed);
	    //time_lnode_split.fetch_add(lnode_split, std::memory_order_relaxed);

	    time_lnode_key_copy.fetch_add(lnode_key_copy, std::memory_order_relaxed);
	    time_lnode_find_median.fetch_add(lnode_find_median, std::memory_order_relaxed);
	    time_lnode_copy.fetch_add(lnode_copy, std::memory_order_relaxed);
	    time_lnode_update.fetch_add(lnode_update, std::memory_order_relaxed);
	}
#endif

	#ifdef THREAD_ALLOC
	void insert(Key_t key, uint64_t value, threadinfo* ti){
	#else
	void insert(Key_t key, uint64_t value){
	#endif
	#ifdef BREAKDOWN
	    uint64_t inode_traversal, inode_alloc, inode_write, inode_sync, inode_split;
	    uint64_t lnode_traversal, lnode_alloc, lnode_write, lnode_sync, lnode_split, lnode_key_copy, lnode_find_median, lnode_copy, lnode_update;
	    //uint64_t lnode_traversal, lnode_alloc, lnode_write, lnode_sync, lnode_split;
	    inode_traversal = inode_alloc = inode_write = inode_sync = inode_split = 0;
	    lnode_traversal = lnode_alloc = lnode_write = lnode_sync = lnode_split = lnode_key_copy = lnode_find_median = lnode_copy = lnode_update = 0;
	    //lnode_traversal = lnode_alloc = lnode_write = lnode_sync = lnode_split = 0;
	    uint64_t start, end;
	#endif
	restart:
	    #ifdef BREAKDOWN
	    start = _rdtsc();
	    #endif
	    auto cur = root;

	    int stack_cnt = 0;
	    inode_t<Key_t>* stack[root->level];

	    bool need_restart = false;
	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    inode_traversal += (end - start);
	    start = _rdtsc();
	    #endif
	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart){
	        #ifdef BREAKDOWN
		end = _rdtsc();
		inode_sync += (end - start);
		#endif
		goto restart;
	    }
	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    inode_sync += (end - start);
	    start = _rdtsc();
	    #endif

	    // tree traversal
	    while(cur->level != 0){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key);
		#ifdef BREAKDOWN 
		end = _rdtsc();
		inode_traversal += (end - start);
		start = _rdtsc();
		#endif
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    inode_sync += (end - start);
		    #endif
		    goto restart;
		}

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend)){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    inode_sync += (end - start);
		    #endif
		    goto restart;
		}
		#ifdef BREAKDOWN
		end = _rdtsc();
		inode_sync += (end - start);
		start = _rdtsc();
		#endif

		if(child != cur->sibling_ptr)
		    stack[stack_cnt++] = static_cast<inode_t<Key_t>*>(cur);

		cur = child;
		cur_vstart = child_vstart;
	    }

	    #ifdef BREAKDOWN 
	    end = _rdtsc();
	    inode_traversal += (end - start);
	    start = _rdtsc();
	    #endif
	    // found leaf
	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		auto sibling = static_cast<lnode_t<Key_t>*>(leaf->sibling_ptr);
		#ifdef BREAKDOWN
		end = _rdtsc();
		lnode_traversal += (end - start);
		start = _rdtsc();
		#endif
		auto sibling_v = sibling->try_readlock(need_restart);
		if(need_restart){
		    #ifdef BREAKDOWN 
		    end = _rdtsc();
		    lnode_sync += (end - start);
		    #endif
		    goto restart;
		}

		auto leaf_vend = (static_cast<node_t*>(leaf))->get_version(need_restart);
		if(need_restart || (leaf_vstart != leaf_vend)){
		    #ifdef BREAKDOWN 
		    end = _rdtsc();
		    lnode_sync += (end - start);
		    #endif
		    goto restart;
		}
		
		#ifdef BREAKDOWN 
		end = _rdtsc();
		lnode_sync += (end - start);
		start = _rdtsc();
		#endif

		leaf = sibling;
		leaf_vstart = sibling_v;
	    }
	    #ifdef BREAKDOWN 
	    end = _rdtsc();
	    lnode_traversal += (end - start);
	    #endif

	    #ifdef BREAKDOWN
	    auto ret = leaf->insert(key, value, leaf_vstart, lnode_traversal, lnode_write, lnode_sync);
	    #else
	    auto ret = leaf->insert(key, value, leaf_vstart);
	    #endif
	    if(ret == -1) // leaf node has been split while inserting
		goto restart;
	    else if(ret == 0){ // insertion succeeded
		#ifdef BREAKDOWN
		update_breakdown(inode_traversal, inode_alloc, inode_write, inode_sync, inode_split,
			 	 lnode_traversal, lnode_alloc, lnode_write, lnode_sync, lnode_split, lnode_key_copy, lnode_find_median, lnode_copy, lnode_update);
		#endif
		return;
	    }
	    else{ // leaf node split
		Key_t split_key;
		#ifdef BREAKDOWN
		#ifdef THREAD_ALLOC
		auto new_leaf = leaf->split(split_key, leaf_vstart, ti, lnode_alloc, lnode_sync, lnode_split, lnode_key_copy, lnode_find_median, lnode_copy, lnode_update);
		#else
#ifdef EXP
		auto new_leaf = leaf->split(split_key, key, value, leaf_vstart, lnode_alloc, lnode_sync, lnode_split, lnode_key_copy, lnode_find_median, lnode_copy, lnode_update);
#elif defined LINKED
		auto new_leaf = leaf->split(split_key, key, value, leaf_vstart, lnode_alloc, lnode_sync, lnode_split, lnode_key_copy, lnode_find_median, lnode_copy, lnode_update, lnode_traversal, lnode_write);
#else
		auto new_leaf = leaf->split(split_key, leaf_vstart, lnode_alloc, lnode_sync, lnode_split, lnode_key_copy, lnode_find_median, lnode_copy, lnode_update);
#endif
		#endif
		#else
		#ifdef THREAD_ALLOC
		auto new_leaf = leaf->split(split_key, leaf_vstart, ti);
		#else
#if (defined EXP) || (defined LINKED)
		auto new_leaf = leaf->split(split_key, key, value, leaf_vstart);
#else
		auto new_leaf = leaf->split(split_key, leaf_vstart);
#endif
		#endif
		#endif
		if(new_leaf == nullptr){
		    goto restart; // another thread has already splitted this leaf node
		}

#if !(defined EXP) && !(defined LINKED) && !(defined LINKED2)
		if(key <= split_key){
		    #ifdef BREAKDOWN
		    leaf->insert_after_split(key, value, lnode_traversal, lnode_write);
		    #else
		    leaf->insert_after_split(key, value);
		    #endif
		}
		else{
		    #ifdef BREAKDOWN
		    new_leaf->insert_after_split(key, value, lnode_traversal, lnode_write); 
		    #else
		    new_leaf->insert_after_split(key, value);
		    #endif
		}
#endif
#ifdef EXP
		//leaf->bucket_release();
		//new_leaf->bucket_release();
#endif

		if(stack_cnt){
		    int stack_idx = stack_cnt-1;
		    auto old_parent = stack[stack_idx];

		    auto original_node = static_cast<node_t*>(leaf);
		    auto new_node = static_cast<node_t*>(new_leaf);
		    #ifdef BREAKDOWN
		    start = _rdtsc();
		    #endif
		    while(stack_idx > -1){ // backtrack parent nodes
			old_parent = stack[stack_idx];

		    parent_restart:
			#ifdef BREAKDOWN
			start = _rdtsc();
			#endif

			need_restart = false;
			auto parent_vstart = old_parent->try_readlock(need_restart);
			if(need_restart){
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    inode_sync += (end - start);
			    #endif
			    goto parent_restart;
			}
			#ifdef BREAKDOWN
			end = _rdtsc();
			inode_sync += (end - start);
			start = _rdtsc();
			#endif
			
			while(old_parent->sibling_ptr && (old_parent->high_key < split_key)){
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    inode_traversal += (end - start);
			    start = _rdtsc();
			    #endif
			    auto p_sibling = old_parent->sibling_ptr;
			    auto p_sibling_v = p_sibling->try_readlock(need_restart);
			    if(need_restart){
				#ifdef BREAKDOWN
				end = _rdtsc();
				inode_sync += (end - start);
				#endif
				goto parent_restart;
			    }

			    auto parent_vend = old_parent->get_version(need_restart);
			    if(need_restart || (parent_vstart != parent_vend)){
				#ifdef BREAKDOWN
				end = _rdtsc();
				inode_traversal += (end - start);
				#endif
				goto parent_restart;
			    }
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    inode_traversal += (end - start);
			    start = _rdtsc();
			    #endif

			    old_parent = static_cast<inode_t<Key_t>*>(p_sibling);
			    parent_vstart = p_sibling_v;
			}
			#ifdef BREAKDOWN
			end = _rdtsc();
			inode_traversal += (end - start);
			start = _rdtsc();
			#endif

			old_parent->try_upgrade_writelock(parent_vstart, need_restart);
			if(need_restart){
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    inode_sync += (end - start);
			    #endif
			    goto parent_restart;
			}

			if(original_node->level != 0){ // internal node
			    original_node->write_unlock();
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    inode_sync += (end - start);
			    start = _rdtsc();
			    #endif
			}
			else{ // leaf node
#ifdef EXP
			    original_node->split_unlock();
			    new_node->split_unlock();
#else
			    (static_cast<lnode_t<Key_t>*>(original_node))->_split_unlock();
#endif
			    #ifdef BREAKDOWN 
			    end = _rdtsc();
			    lnode_sync += (end - start);
			    #endif
			}


			if(!old_parent->is_full()){ // normal insert
			    #ifdef BREAKDOWN
			    start = _rdtsc();
			    #endif
			    old_parent->insert(split_key, new_node);
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    inode_write += (end - start);
			    start = _rdtsc();
			    #endif
			    old_parent->write_unlock();
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    inode_sync += (end - start);
			    update_breakdown(inode_traversal, inode_alloc, inode_write, inode_sync, inode_split,
				    	     lnode_traversal, lnode_alloc, lnode_write, lnode_sync, lnode_split, lnode_key_copy, lnode_find_median, lnode_copy, lnode_update);
			    #endif
			    return;
			}

			// internal node split
			Key_t _split_key;
			#ifdef BREAKDOWN
			auto new_parent = old_parent->split(_split_key, inode_alloc, inode_split);
			#else
			auto new_parent = old_parent->split(_split_key);
			#endif

			#ifdef BREAKDOWN
			start = _rdtsc();
			#endif
			if(split_key <= _split_key)
			    old_parent->insert(split_key, new_node);
			else
			    new_parent->insert(split_key, new_node);
			#ifdef BREAKDOWN
			end = _rdtsc();
			inode_write += (end - start);
			start =  _rdtsc();
			#endif

			if(stack_idx){
			    original_node = static_cast<node_t*>(old_parent);
			    new_node = static_cast<node_t*>(new_parent);
			    split_key = _split_key;
			    old_parent = stack[--stack_idx];
			}
			else{ // set new root
			    if(old_parent == root){ // current node is root
				auto new_root = new inode_t<Key_t>(_split_key, old_parent, new_parent, nullptr, old_parent->level+1, new_parent->high_key);
				#ifdef BREAKDOWN
				end = _rdtsc();
				inode_alloc += (end - start);
				start =  _rdtsc();
				#endif
				root = static_cast<node_t*>(new_root);
				#ifdef BREAKDOWN
				end = _rdtsc();
				inode_write += (end - start);
				start =  _rdtsc();
				#endif
				old_parent->write_unlock();
				#ifdef BREAKDOWN
				end = _rdtsc();
				inode_sync += (end - start);
				update_breakdown(inode_traversal, inode_alloc, inode_write, inode_sync, inode_split,
				    	         lnode_traversal, lnode_alloc, lnode_write, lnode_sync, lnode_split, lnode_key_copy, lnode_find_median, lnode_copy, lnode_update);
				#endif
				return;
			    }
			    else{ // other thread has already created a new root
#ifdef BREAKDOWN
				insert_key(_split_key, new_parent, old_parent, inode_traversal, inode_alloc, inode_write, inode_sync, inode_split, lnode_sync);
				update_breakdown(inode_traversal, inode_alloc, inode_write, inode_sync, inode_split,
				    	         lnode_traversal, lnode_alloc, lnode_write, lnode_sync, lnode_split, lnode_key_copy, lnode_find_median, lnode_copy, lnode_update);
#else
				insert_key(_split_key, new_parent, old_parent);
#endif
				return;
			    }
			}
		    }
		}
		else{ // set new root
		    if(root == leaf){ // current node is root
			#ifdef BREAKDOWN
			start = _rdtsc();
			#endif
			auto new_root = new inode_t<Key_t>(split_key, leaf, new_leaf, nullptr, root->level+1, new_leaf->high_key);
			#ifdef BREAKDOWN
			end = _rdtsc();
			inode_alloc += (end - start);
			start = _rdtsc();
			#endif
			root = static_cast<node_t*>(new_root);
			#ifdef BREAKDOWN
			end = _rdtsc();
			inode_write += (end - start);
			start = _rdtsc();
			#endif
#ifdef EXP
			leaf->split_unlock();
			new_leaf->split_unlock();
#else
			leaf->_split_unlock();
#endif
			#ifdef BREAKDOWN 
			end = _rdtsc();
			lnode_sync += (end - start);
			update_breakdown(inode_traversal, inode_alloc, inode_write, inode_sync, inode_split,
					 lnode_traversal, lnode_alloc, lnode_write, lnode_sync, lnode_split, lnode_key_copy, lnode_find_median, lnode_copy, lnode_update);
			#endif
			return;
		    }
		    else{ // other thread has already created a new root
			#ifdef BREAKDOWN
			insert_key(split_key, new_leaf, leaf, inode_traversal, inode_alloc, inode_write, inode_sync, inode_split, lnode_sync);
			update_breakdown(inode_traversal, inode_alloc, inode_write, inode_sync, inode_split,
					 lnode_traversal, lnode_alloc, lnode_write, lnode_sync, lnode_split, lnode_key_copy, lnode_find_median, lnode_copy, lnode_update);
			#else
			insert_key(split_key, new_leaf, leaf);
			#endif
			return;
		    }
		}
	    }
	}

	/* this function is called when root has been split by another threads */
	#ifdef BREAKDOWN
	void insert_key(Key_t key, node_t* value, node_t* prev, uint64_t& inode_traversal, uint64_t& inode_alloc, uint64_t& inode_write, uint64_t& inode_sync, uint64_t& inode_split, uint64_t& lnode_sync){ 
	#else
	void insert_key(Key_t key, node_t* value, node_t* prev){
	#endif

	    #ifdef BREAKDOWN
	    uint64_t start, end;
	    #endif
	restart:
	    auto cur = root;
	    #ifdef BREAKDOWN 
	    start = _rdtsc();
	    #endif
	    bool need_restart = false;

	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart){
		#ifdef BREAKDOWN
		end = _rdtsc();
		inode_sync += (end - start);
		#endif
		goto restart;
	    }
	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    inode_sync += (end - start);
	    start = _rdtsc();
	    #endif

	    // since we need to find exact internal node which has been previously the root, we use readlock for traversal
	    while(cur->level != prev->level+1){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key);
		#ifdef BREAKDOWN
		end = _rdtsc();
		inode_traversal += (end - start);
		start = _rdtsc();
		#endif
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart){
		    #ifdef BREAKDOWN 
		    end = _rdtsc();
		    inode_sync += (end - start);
		    #endif
		    goto restart;
		}

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend)){
		    #ifdef BREAKDOWN 
		    end = _rdtsc();
		    inode_sync += (end - start);
		    #endif
		    goto restart;
		}

		#ifdef BREAKDOWN 
		end = _rdtsc();
		inode_sync += (end - start);
		start = _rdtsc();
		#endif
		cur = child;
		cur_vstart = child_vstart;
	    }

	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    inode_traversal += (end - start);
	    start = _rdtsc();
	    #endif
	    // found parent of prev node
	    while(cur->sibling_ptr && ((static_cast<inode_t<Key_t>*>(cur))->high_key < key)){
		auto sibling = cur->sibling_ptr;
		#ifdef BREAKDOWN
		end = _rdtsc();
		inode_traversal += (end - start);
		start = _rdtsc();
		#endif
		auto sibling_vstart = sibling->try_readlock(need_restart);
		if(need_restart){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    inode_sync += (end - start);
		    #endif
		    goto restart;
		}

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend)){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    inode_sync += (end - start);
		    #endif
		    goto restart;
		}

		#ifdef BREAKDOWN
		end = _rdtsc();
		inode_sync += (end - start);
		start = _rdtsc();
		#endif
		cur = static_cast<inode_t<Key_t>*>(sibling);
		cur_vstart = sibling_vstart;
	    }

	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    inode_traversal += (end - start);
	    start = _rdtsc();
	    #endif
	    cur->try_upgrade_writelock(cur_vstart, need_restart);
	    if(need_restart){
		#ifdef BREAKDOWN
		end = _rdtsc();
		inode_sync += (end - start);
		#endif
		goto restart;
	    }
	    #ifdef BREAKDOWN 
	    end = _rdtsc();
	    inode_sync += (end - start);
	    start = _rdtsc();
	    #endif
	    if(prev->level != 0){
		prev->write_unlock();
		#ifdef BREAKDOWN
		end = _rdtsc();
		inode_sync += (end - start);
		#endif
	    }
	    else{
#ifdef EXP 
		prev->split_unlock();
		value->split_unlock();
#else
		(static_cast<lnode_t<Key_t>*>(prev))->_split_unlock();
#endif
		#ifdef BREAKDOWN 
		end = _rdtsc();
		lnode_sync += (end - start);
		#endif
	    }

	    auto node = static_cast<inode_t<Key_t>*>(cur);
	    if(!node->is_full()){
		#ifdef BREAKDOWN
		start = _rdtsc();
		#endif
		node->insert(key, value);
		#ifdef BREAKDOWN
		end = _rdtsc();
		inode_write += (end - start);
		start = _rdtsc();
		#endif
		node->write_unlock();
		#ifdef BREAKDOWN
		end = _rdtsc();
		inode_sync += (end - start);
		#endif
		return;
	    }
	    else{
		Key_t split_key;
		#ifdef BREAKDOWN
		auto new_node = node->split(split_key, inode_alloc, inode_split);
		#else
		auto new_node = node->split(split_key);
		#endif

		#ifdef BREAKDOWN
		start = _rdtsc();
		#endif
		if(key <= split_key)
		    node->insert(key, value);
		else
		    new_node->insert(key, value);
		#ifdef BREAKDOWN
		end = _rdtsc();
		inode_write += (end - start);
		#endif

		if(node == root){ // if current nodes is root
		    #ifdef BREAKDOWN
		    start = _rdtsc();
		    #endif
		    auto new_root = new inode_t<Key_t>(split_key, node, new_node, nullptr, node->level+1, new_node->high_key);
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    inode_alloc += (end - start);
		    start = _rdtsc();
		    #endif
		    root = static_cast<node_t*>(new_root);
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    inode_write += (end - start);
		    start = _rdtsc();
		    #endif
		    node->write_unlock();
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    inode_sync += (end - start);
		    #endif
		    return;
		}
		else{ // other thread has already created a new root
		    #ifdef BREAKDOWN
		    insert_key(split_key, new_node, node, inode_traversal, inode_alloc, inode_write, inode_sync, inode_split, lnode_sync);
		    #else
		    insert_key(split_key, new_node, node);
		    #endif
		    return;
		}
	    }
	}



	bool update(Key_t key, uint64_t value){


	restart:
	    auto cur = root;
	    bool need_restart = false;
	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart) goto restart;

	    // traversal
	    while(cur->level != 0){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key);
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart) goto restart;

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend))
		    goto restart;

		cur = child;
		cur_vstart = child_vstart;
	    }

	    // found leaf
	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    // move right if necessary
	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		auto sibling = leaf->sibling_ptr;
		auto sibling_v = sibling->try_readlock(need_restart);
		if(need_restart) goto restart;

		auto leaf_vend = leaf->get_version(need_restart);
		if(need_restart || (leaf_vstart != leaf_vend)) goto restart;

		leaf = static_cast<lnode_t<Key_t>*>(sibling);
		leaf_vstart = sibling_v;
	    }

	    auto ret = leaf->update(key, value);
	    if(ret == 0)
		return true;
	    else if(ret == -1)
		goto restart;

	    auto leaf_vend = leaf->get_version(need_restart);
	    if(need_restart || (leaf_vstart != leaf_vend))
		goto restart;
	    return false;
	}


	uint64_t lookup(Key_t key){
	restart:
	    auto cur = root;
	    bool need_restart = false;

	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart) goto restart;

	    // traversal
	    while(cur->level != 0){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key);
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart) goto restart;

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend))
		    goto restart;

		cur = child;
		cur_vstart = child_vstart;
	    }

	    // found leaf
	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    // move right if necessary
	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		auto sibling = leaf->sibling_ptr;

		auto sibling_v = sibling->try_readlock(need_restart);
		if(need_restart) goto restart;

		auto leaf_vend = leaf->get_version(need_restart);
		if(need_restart || (leaf_vstart != leaf_vend)) goto restart;

		leaf = static_cast<lnode_t<Key_t>*>(sibling);
		leaf_vstart = sibling_v;
	    }
	    #ifdef LINKED 
	    auto ret = leaf->find(key, need_restart);
	    if(need_restart) goto restart;
	    #else
	    auto ret = leaf->find(key);
	    #endif
	    auto leaf_vend = leaf->get_version(need_restart);
	    if(need_restart || (leaf_vstart != leaf_vend)) goto restart;

	    return ret;
	}


	int range_lookup(Key_t min_key, int range, uint64_t* buf){

	restart:
	    auto cur = root;
	    bool need_restart = false;
	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart) goto restart;

	    // traversal
	    while(cur->level != 0){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(min_key);
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart) goto restart;

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend)) goto restart;

		cur = child;
		cur_vstart = child_vstart;
	    }

	    // found leaf
	    int count = 0;
	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    while(count < range){

		// move right if necessary
		while(leaf->sibling_ptr && (leaf->high_key < min_key)){
		    auto sibling = leaf->sibling_ptr;

		    auto sibling_v = sibling->try_readlock(need_restart);
		    if(need_restart) goto restart;

		    auto leaf_vend = leaf->get_version(need_restart);
		    if(need_restart || (leaf_vstart != leaf_vend)) goto restart;

		    leaf = static_cast<lnode_t<Key_t>*>(sibling);
		    leaf_vstart = sibling_v;
		}

		auto ret = leaf->range_lookup(min_key, buf, count, range);
		if(ret == -1)
		    goto restart;

		auto sibling = leaf->sibling_ptr;

		auto leaf_vend = leaf->get_version(need_restart);
		if(need_restart || (leaf_vstart != leaf_vend)) goto restart;

		if(ret == range){
		    return ret;
		}

		// reaches to the rightmost leaf
		if(!sibling) break;

		auto sibling_vstart = sibling->try_readlock(need_restart);
		if(need_restart) goto restart;

		leaf = static_cast<lnode_t<Key_t>*>(sibling);
		leaf_vstart = sibling_vstart;
		count = ret;
	    }
	    return count;
	}

	void print_leaf(){
	    auto cur = root;
	    while(cur->level != 0){
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
	    while(cur->level != 0){
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
	    while(cur->level != 0){
		auto p = static_cast<inode_t<Key_t>*>(cur);
		p->sanity_check(p->high_key, true);
		cur = cur->leftmost_ptr;
	    }

	    auto l = static_cast<lnode_t<Key_t>*>(cur);
	    l->sanity_check(l->high_key, true);
	}

	uint64_t find_anyway(Key_t key){
	    auto cur = root;
	    while(cur->level != 0){
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

	double utilization(){
	    auto cur = root;
	    while(cur->level != 0){
		cur = cur->leftmost_ptr;
	    }

	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);
	    int leaf_cnt = 0;
	    double util = 0;
	    do{
		leaf_cnt++;
		util += leaf->utilization();

		leaf = static_cast<lnode_t<Key_t>*>(leaf->sibling_ptr);
	    }while(leaf);
	    return util/leaf_cnt*100.0;
	}

	int height(){
	    auto cur = root;
	    return cur->level;
	}

	friend class leaf_allocator_t;

    private:
	node_t* root;
};
}
#endif
