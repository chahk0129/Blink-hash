#ifndef TREE_OPTIMIZED_H__
#define TREE_OPTIMIZED_H__
#include "node_optimized.h"

// BACKOFF flag can be enabled for high contention workload
//#define BACKOFF

namespace BLINK_OPTIMIZED{

#ifdef BREAKDOWN
static thread_local uint64_t time_traversal;
static thread_local uint64_t time_abort;
static thread_local uint64_t time_latch;
static thread_local uint64_t time_node;
static thread_local uint64_t time_split;
static thread_local bool abort;
#endif

template <typename Key_t>
class btree_t{
    public:
	inline uint64_t _rdtsc(){
	    uint32_t lo, hi;
	    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
	    return (((uint64_t)hi << 32) | lo);
	}

	btree_t(){ 
	    root = static_cast<node_t*>(new lnode_t<Key_t>()); 
	}
	~btree_t(){ }

	#ifdef BREAKDOWN
	void get_breakdown(uint64_t& _time_traversal, uint64_t& _time_abort, uint64_t& _time_latch, uint64_t& _time_node, uint64_t& _time_split){
	    _time_traversal = time_traversal;
	    _time_abort = time_abort;
	    _time_latch = time_latch;
	    _time_node = time_node;
	    _time_split = time_split;
	}
        #endif

	void backoff(int count){
	    #ifdef BACKOFF
	    if(count > 20){
		for(int i=0; i<count*5; i++){
		    asm volatile("pause\n" : : : "memory");
		    std::atomic_thread_fence(std::memory_order_seq_cst);
		}
	    }
	    else{
		if(count > 5)
		    sched_yield();
	    }
	    #endif
	}

	int check_height(){
	    return root->level;
	}
	void insert(Key_t key, uint64_t value) {
	    #ifdef BREAKDOWN
            uint64_t start, end;
            abort = false;
            #endif
	    int restart_cnt = -1;
	restart:
	    #ifdef BREAKDOWN
            start = _rdtsc();
            #endif
	    auto cur = root;

	    int stack_cnt = 0;
	    inode_t<Key_t>* stack[root->level];

	    bool need_restart = false;
	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart){
		#ifdef BREAKDOWN
                end = _rdtsc();
                if(abort) time_abort += (end - start);
                else time_traversal += (end - start);
		abort = true;
                #endif
		goto restart;
	    }

	    // tree traversal
	    while(cur->level != 0){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key);
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart){
		    #ifdef BREAKDOWN
                    end = _rdtsc();
                    if(abort) time_abort += (end - start);
                    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend)){
		    #ifdef BREAKDOWN
                    end = _rdtsc();
                    if(abort) time_abort += (end - start);
                    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

		if(child != (static_cast<inode_t<Key_t>*>(cur))->sibling_ptr)
		    stack[stack_cnt++] = static_cast<inode_t<Key_t>*>(cur);

		cur = child;
		cur_vstart = child_vstart;
	    }

	    // found leaf
	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		auto sibling = static_cast<lnode_t<Key_t>*>(leaf->sibling_ptr);
		auto sibling_v = sibling->try_readlock(need_restart);
		if(need_restart){
		    #ifdef BREAKDOWN
                    end = _rdtsc();
                    if(abort) time_abort += (end - start);
                    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

		auto leaf_vend = leaf->get_version(need_restart);
		if(need_restart || (leaf_vstart != leaf_vend)){
		    #ifdef BREAKDOWN
                    end = _rdtsc();
                    if(abort) time_abort += (end - start);
                    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}
		
		leaf = sibling;
		leaf_vstart = sibling_v;
	    }

	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    if(abort) time_abort += (end - start);
	    else time_traversal += (end - start);
	    start = _rdtsc();
            #endif

	    leaf->try_upgrade_writelock(leaf_vstart, need_restart);
	    #ifdef BREAKDOWN
            end = _rdtsc();
            time_latch += (end - start);
            start = _rdtsc();
            #endif
	    if(need_restart){
		#ifdef BREAKDOWN
                abort = true;
                #endif
		goto restart;
	    }
		
	    if(!leaf->is_full()){ // normal insert
		leaf->insert(key, value);
		#ifdef BREAKDOWN
                end = _rdtsc();
                time_node += (end - start);
                start = _rdtsc();
                #endif
		leaf->write_unlock();
		#ifdef BREAKDOWN
                end = _rdtsc();
                time_latch += (end - start);
                #endif
		return;
	    }
	    else{ // leaf node split
		Key_t split_key; /// here
		auto new_leaf = leaf->split(split_key);
		if(key <= split_key)
		    leaf->insert(key, value);
		else
		    new_leaf->insert(key, value);

		#ifdef BREAKDOWN
                end = _rdtsc();
                time_split += (end - start);
                #endif

		if(stack_cnt){
		    int stack_idx = stack_cnt-1;
		    auto old_parent = stack[stack_idx];

		    auto original_node = static_cast<node_t*>(leaf);
		    auto new_node = static_cast<node_t*>(new_leaf);

		    while(stack_idx > -1){ // backtrack
			old_parent = stack[stack_idx];
			#ifdef BREAKDOWN
			bool parent_abort = false;
			#endif

		    parent_restart:
			#ifdef BREAKDOWN
                        start = _rdtsc();
                        #endif
			need_restart = false;
			auto parent_vstart = old_parent->try_readlock(need_restart);
			if(need_restart){
			    #ifdef BREAKDOWN
                            end = _rdtsc();
                            if(parent_abort) time_abort += (end - start);
                            else time_traversal += (end - start);
			    parent_abort = true;
                            #endif
			    goto parent_restart;
			}
		
			while(old_parent->sibling_ptr && (old_parent->high_key < split_key)){
			    auto p_sibling = old_parent->sibling_ptr;
			    auto p_sibling_v = p_sibling->try_readlock(need_restart);
			    if(need_restart){
			    	#ifdef BREAKDOWN
				end = _rdtsc();
				if(parent_abort) time_abort += (end - start);
				else time_traversal += (end - start);
				parent_abort = true;
                            	#endif
				goto parent_restart;
			    }

			    auto parent_vend = old_parent->get_version(need_restart);
			    if(need_restart || (parent_vstart != parent_vend)){
			    	#ifdef BREAKDOWN
				end = _rdtsc();
				if(parent_abort) time_abort += (end - start);
				else time_traversal += (end - start);
				parent_abort = true;
                            	#endif
				goto parent_restart;
			    }

			    old_parent = static_cast<inode_t<Key_t>*>(p_sibling);
			    parent_vstart = p_sibling_v;
			}

			#ifdef BREAKDOWN
			end = _rdtsc();
			if(parent_abort) time_abort += (end - start);
			else time_traversal += (end - start);
			start = _rdtsc();
                        #endif

			old_parent->try_upgrade_writelock(parent_vstart, need_restart);
			if(need_restart){
			    #ifdef BREAKDOWN
                            end = _rdtsc();
                            time_latch += (end - start);
                            parent_abort = true;
                            #endif
			    goto parent_restart;
			}

			original_node->write_unlock();
			#ifdef BREAKDOWN
                        end = _rdtsc();
                        time_latch += (end - start);
                        start = _rdtsc();
                        #endif

			if(!old_parent->is_full()){ // normal insert
			    old_parent->insert(split_key, new_node);
			    #ifdef BREAKDOWN
                            end = _rdtsc();
                            time_split += (end - start);
                            start = _rdtsc();
                            #endif
			    old_parent->write_unlock();
			    #ifdef BREAKDOWN
                            end = _rdtsc();
                            time_latch += (end - start);
			    #endif
			    return;
			}

			// internal node split
			Key_t _split_key;
			auto new_parent = old_parent->split(_split_key);
			if(split_key <= _split_key)
			    old_parent->insert(split_key, new_node);
			else
			    new_parent->insert(split_key, new_node);

			#ifdef BREAKDOWN
                        end = _rdtsc();
                        time_split += (end - start);
			start = _rdtsc();
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
				root = static_cast<node_t*>(new_root);
				#ifdef BREAKDOWN
                                end = _rdtsc();
                                time_split += (end - start);
                                start = _rdtsc();
                                #endif
				old_parent->write_unlock();
				#ifdef BREAKDOWN
                                end = _rdtsc();
                                time_latch += (end - start);
                                #endif
				return;
			    }
			    else{ // other thread has already created a new root
				insert_key(_split_key, new_parent, old_parent);
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
			root = static_cast<node_t*>(new_root);
			#ifdef BREAKDOWN
                        end = _rdtsc();
                        time_split += (end - start);
                        start = _rdtsc();
                        #endif
			leaf->write_unlock();
			#ifdef BREAKDOWN
                        end = _rdtsc();
                        time_latch += (end - start);
                        #endif
			return;
		    }
		    else{ // other thread has already created a new root
			insert_key(split_key, new_leaf, leaf);
			return;
		    }
		}
	    }
	}

	/* this function is called when root has been split by another threads */
	void insert_key(Key_t key, node_t* value, node_t* prev){
	    #ifdef BREAKDOWN
            uint64_t start, end;
            abort = false;
            #endif
	restart:
	    #ifdef BREAKDOWN
            start = _rdtsc();
            #endif
	    auto cur = root;
	    bool need_restart = false;

	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart){
		#ifdef BREAKDOWN
                end = _rdtsc();
                if(abort) time_abort += (end - start);
                else time_traversal += (end - start);
		abort = true;
                #endif
		goto restart;
	    }

	    // since we need to find the internal node which has been previously the root, we use readlock for traversal
	    while(cur->level != prev->level+1){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key);
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend)){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

		cur = child;
		cur_vstart = child_vstart;
	    }

	    // found parent level node
	    while((static_cast<inode_t<Key_t>*>(cur))->sibling_ptr && ((static_cast<inode_t<Key_t>*>(cur))->high_key < key)){
		auto sibling = (static_cast<inode_t<Key_t>*>(cur))->sibling_ptr;
		auto sibling_vstart = sibling->try_readlock(need_restart);
		if(need_restart){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend)){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

		cur = static_cast<inode_t<Key_t>*>(sibling);
		cur_vstart = sibling_vstart;
	    }

	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    if(abort) time_abort += (end - start);
	    else time_traversal += (end - start);
	    start = _rdtsc();
            #endif

	    cur->try_upgrade_writelock(cur_vstart, need_restart);
	    if(need_restart){
		#ifdef BREAKDOWN
                end = _rdtsc();
                time_latch += (end - start);
                abort = true;
                #endif
		goto restart;
	    }
	    prev->write_unlock();
	    #ifdef BREAKDOWN
            end = _rdtsc();
            time_latch += (end - start);
            start = _rdtsc();
            #endif

	    auto node = static_cast<inode_t<Key_t>*>(cur);
	    
	    if(!node->is_full()){
		node->insert(key, value);
		#ifdef BREAKDOWN
                end = _rdtsc();
                time_split += (end - start);
                start = _rdtsc();
                #endif
		node->write_unlock();
		#ifdef BREAKDOWN
                end = _rdtsc();
                time_latch += (end - start);
                #endif
		return;
	    }
	    else{
		Key_t split_key;
		auto new_node = node->split(split_key);
		if(key <= split_key)
		    node->insert(key, value);
		else
		    new_node->insert(key, value);
		#ifdef BREAKDOWN
                end = _rdtsc();
                time_split += (end - start);
                start = _rdtsc();
                #endif

		if(node == root){ // if current nodes is root
		    auto new_root = new inode_t<Key_t>(split_key, node, new_node, nullptr, node->level+1, new_node->high_key);
		    root = static_cast<node_t*>(new_root);
		    #ifdef BREAKDOWN
                    end = _rdtsc();
                    time_split += (end - start);
                    start = _rdtsc();
                    #endif
		    node->write_unlock();
		    #ifdef BREAKDOWN
                    end = _rdtsc();
                    time_latch += (end - start);
                    #endif
		    return;
		}
		else{ // other thread has already created a new root
		    insert_key(split_key, new_node, node);
		    return;
		}
	    }
	}



	bool update(Key_t key, uint64_t value){
	    #ifdef BREAKDOWN
            uint64_t start, end;
            abort = false;
            #endif

	    int restart_cnt = -1;
	restart:
	    backoff(restart_cnt++);
	    #ifdef BREAKDOWN
            start = _rdtsc();
            #endif
	    auto cur = root;
	    bool need_restart = false;
	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart){
		#ifdef BREAKDOWN
                end = _rdtsc();
                if(abort) time_abort += (end - start);
                else time_traversal += (end - start);
		abort = true;
                #endif
		goto restart;
	    }

	    // traversal
	    while(cur->level != 0){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key);
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

		auto cur_vend = cur->get_version(need_restart);
		#ifdef UPDATE_LOCK
                if(need_restart || ((cur_vstart & (~0u)) != (cur_vend & (~0u)))){
                #else
		if(need_restart || (cur_vstart != cur_vend)){
		#endif
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

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
		if(need_restart){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

		auto leaf_vend = leaf->get_version(need_restart);
		#ifdef UPDATE_LOCK
		if(need_restart || ((leaf_vstart & (~0u)) != (leaf_vend & (~0u)))){
		#else
		if(need_restart || (leaf_vstart != leaf_vend)){
		#endif
		    #ifdef BREAKDOWN
                    end = _rdtsc();
                    if(abort) time_abort += (end - start);
                    else time_traversal += (end - start);
                    abort = true;
                    #endif
		    goto restart;
		}

		leaf = static_cast<lnode_t<Key_t>*>(sibling);
		leaf_vstart = sibling_v;
	    }

	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    if(abort) time_abort += (end - start);
	    else time_traversal += (end - start);
	    start = _rdtsc();
            #endif
    
	    #ifdef UPDATE_LOCK
	    leaf->try_upgrade_updatelock(leaf_vstart, need_restart);
	    #else
	    leaf->try_upgrade_writelock(leaf_vstart, need_restart);
	    #endif
	    if(need_restart){
		#ifdef BREAKDOWN
                end = _rdtsc();
                time_latch += (end - start);
                abort = true;
                #endif
		goto restart;
	    }

	    #ifdef BREAKDOWN
            end = _rdtsc();
            time_latch += (end - start);
            start = _rdtsc();
            #endif

	    bool ret = leaf->update(key, value);

	    #ifdef BREAKDOWN
            end = _rdtsc();
            time_node += (end - start);
            start = _rdtsc();
            #endif

	    #ifdef UPDATE_LOCK
	    leaf->update_unlock();
	    #else
	    leaf->write_unlock();
	    #endif

	    #ifdef BREAKDOWN
            end = _rdtsc();
            time_latch += (end - start);
            #endif

	    return ret;
	}


	uint64_t lookup(Key_t key){
	    #ifdef BREAKDOWN
            abort = false;
            uint64_t start, end;
            #endif
	restart:
	    #ifdef BREAKDOWN
            start = _rdtsc();
            #endif
	    auto cur = root;
	    bool need_restart = false;

	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart){
		#ifdef BREAKDOWN
                end = _rdtsc();
                if(abort) time_abort += (end - start);
                else time_traversal += (end - start);
		abort = true;
                #endif
		goto restart;
	    }

	    // traversal
	    while(cur->level != 0){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key);
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

		auto cur_vend = cur->get_version(need_restart);
		#ifdef UPDATE_LOCK
                if(need_restart || ((cur_vstart & (~0u)) != (cur_vend & (~0u)))){
                #else
		if(need_restart || (cur_vstart != cur_vend)){
		#endif
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

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
		if(need_restart){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

		auto leaf_vend = leaf->get_version(need_restart);
		#ifdef UPDATE_LOCK
		if(need_restart || ((leaf_vstart & (~0u)) != (leaf_vend & (~0u)))){
		#else
		if(need_restart || (leaf_vstart != leaf_vend)){
		#endif
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

		leaf = static_cast<lnode_t<Key_t>*>(sibling);
		leaf_vstart = sibling_v;
	    }

	    #ifdef BREAKDOWN
            end = _rdtsc();
            if(abort) time_abort += (end - start);
            else time_traversal += (end - start);
            start = _rdtsc();
            #endif

	    auto ret = leaf->find(key);

	    #ifdef BREAKDOWN
            end = _rdtsc();
            time_node += (end - start);
            start = _rdtsc();
            #endif

	    auto leaf_vend = leaf->get_version(need_restart);
  	    #ifdef UPDATE_LOCK
	    if(need_restart || ((leaf_vstart & (~0u)) != (leaf_vend & (~0u)))){
	    #else
	    if(need_restart || (leaf_vstart != leaf_vend)){
	    #endif
		#ifdef BREAKDOWN
		end = _rdtsc();
		if(abort) time_abort += (end - start);
		else time_traversal += (end - start);
		abort = true;
                #endif
		goto restart;
	    }

	    return ret;
	}

	bool remove(Key_t key){
	restart:
	    auto cur = root;
	    bool need_restart = false;

	    int stack_cnt = 0;
	    inode_t<Key_t>* stack[root->level];

	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart){
		goto restart;
	    }

	    // traversal
	    while(cur->level != 0){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key);
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart){
		    goto restart;
		}

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend)){
		    goto restart;
		}

		if(child != (static_cast<inode_t<Key_t>*>(cur))->sibling_ptr)
		    stack[stack_cnt++] = static_cast<inode_t<Key_t>*>(cur);

		cur = child;
		cur_vstart = child_vstart;
	    }

	    // found leaf
	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		auto sibling = static_cast<lnode_t<Key_t>*>(leaf->sibling_ptr);
		auto sibling_v = sibling->try_readlock(need_restart);
		if(need_restart){
		    goto restart;
		}

		auto leaf_vend = leaf->get_version(need_restart);
		if(need_restart || (leaf_vstart != leaf_vend)){
		    goto restart;
		}

		leaf = sibling;
		leaf_vstart = sibling_v;
	    }

	    leaf->try_upgrade_writelock(leaf_vstart, need_restart);
	    if(need_restart){
		goto restart;
	    }

	    auto ret = leaf->remove(key);
	    leaf->write_unlock();
	    return ret;
	}

	    

	int range_lookup(Key_t min_key, int range, uint64_t* buf){
	    #ifdef BREAKDOWN
            uint64_t start, end;
            abort = false;
            #endif
	restart:
	    #ifdef BREAKDOWN
            start = _rdtsc();
            #endif
	    auto cur = root;
	    bool need_restart = false;
	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart){
		#ifdef BREAKDOWN
                end = _rdtsc();
                if(abort) time_abort += (end - start);
                else time_traversal += (end - start);
		abort = true;
                #endif
		goto restart;
	    }

	    // traversal
	    while(cur->level != 0){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(min_key);
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

		auto cur_vend = cur->get_version(need_restart);
		#ifdef UPDATE_LOCK
                if(need_restart || ((cur_vstart & (~0u)) != (cur_vend & (~0u)))){
                #else
		if(need_restart || (cur_vstart != cur_vend)){ 
		#endif
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

		cur = child;
		cur_vstart = child_vstart;
	    }

	    // found leaf
	    int count = 0;
	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    // move right if necessary
	    while(leaf->sibling_ptr && (leaf->high_key < min_key)){
		auto sibling = leaf->sibling_ptr;

		auto sibling_v = sibling->try_readlock(need_restart);
		if(need_restart){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

		auto leaf_vend = leaf->get_version(need_restart);
		#ifdef UPDATE_LOCK
                if(need_restart || ((leaf_vstart & (~0u)) != (leaf_vend & (~0u)))){
                #else
		if(need_restart || (leaf_vstart != leaf_vend)){
		#endif
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
                    #endif
		    goto restart;
		}

		leaf = static_cast<lnode_t<Key_t>*>(sibling);
		leaf_vstart = sibling_v;
	    }

	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    if(abort) time_abort += (end - start);
	    else time_traversal += (end - start);
	    start = _rdtsc();
            #endif

	    auto idx = leaf->find_lowerbound(min_key);
	    while(count < range){
		auto ret = leaf->range_lookup(idx, buf, count, range);
		auto sibling = leaf->sibling_ptr;
		#ifdef BREAKDOWN
                end = _rdtsc();
                time_node += (end - start);
                start = _rdtsc();
                #endif
		// collected all keys within range or reaches the rightmost leaf
		if((ret == range) || !sibling){
		    auto leaf_vend = leaf->get_version(need_restart);
		    #ifdef UPDATE_LOCK
		    if(need_restart || ((leaf_vstart & (~0u)) != (leaf_vend & (~0u)))){
                    #else
		    if(need_restart || (leaf_vstart != leaf_vend)){
		    #endif
			#ifdef BREAKDOWN
			end = _rdtsc();
			if(abort) time_abort += (end - start);
			else time_traversal += (end - start);
			abort = true;
                        #endif
			goto restart;
		    }
		    return ret;
		}
		auto sibling_vstart = sibling->try_readlock(need_restart);
		if(need_restart){
		    #ifdef BREAKDOWN
                    end = _rdtsc();
                    if(abort) time_abort += (end - start);
                    else time_traversal += (end - start);
                    abort = true;
                    #endif
		    goto restart;
		}

		auto leaf_vend = leaf->get_version(need_restart);
		#ifdef UPDATE_LOCK
                if(need_restart || ((leaf_vstart & (~0u)) != (leaf_vend & (~0u)))){
                #else
		if(need_restart || (leaf_vstart != leaf_vend)){
		#endif
		    #ifdef BREAKDOWN
                    end = _rdtsc();
                    if(abort) time_abort += (end - start);
                    else time_traversal += (end - start);
                    abort = true;
                    #endif
		    goto restart;
		}

		leaf = static_cast<lnode_t<Key_t>*>(sibling);
		leaf_vstart = sibling_vstart;
		count = ret;
		idx = 0;
		#ifdef BREAKDOWN
                start = _rdtsc();
                #endif
	    }
	    return count;
	}

	void print_leaf(){
	    auto cur = root;
	    while(cur->level != 0){
		cur = (static_cast<inode_t<Key_t>*>(cur))->leftmost_ptr();
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
		cur = static_cast<inode_t<Key_t>*>(cur->leftmost_ptr());
	    }
	}

	void sanity_check(){
	    auto cur = root;
	    while(cur->level != 0){
		auto p = static_cast<inode_t<Key_t>*>(cur);
		p->sanity_check(p->high_key, true);
		cur = (static_cast<inode_t<Key_t>*>(cur))->leftmost_ptr();
	    }

	    auto l = static_cast<lnode_t<Key_t>*>(cur);
	    l->sanity_check(l->high_key, true);
	}

	uint64_t find_anyway(Key_t key){
	    auto cur = root;
	    while(cur->level != 0){
		cur = (static_cast<inode_t<Key_t>*>(cur))->leftmost_ptr();
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
		cur = (static_cast<inode_t<Key_t>*>(cur))->leftmost_ptr();
	    }
	    
	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);
	    double util = 0;
	    int leaf_cnt = 0;
	    while(leaf){
		util += leaf->utilization();
		leaf_cnt++;
		leaf = static_cast<lnode_t<Key_t>*>(leaf->sibling_ptr);
	    }

	    return util / leaf_cnt;
	}

	int height(){
	    auto cur = root;
	    return cur->level;
	}

	void footprint(uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied){
	    auto cur = root;
	    auto leftmost_node = cur;
	    while(cur->level != 0){
		leftmost_node = cur;
		do{
		    meta += sizeof(node_t) + sizeof(Key_t) - sizeof(node_t*);
		    auto cnt = cur->get_cnt();
		    auto invalid_num = inode_t<Key_t>::cardinality - cnt;
		    structural_data_occupied += sizeof(entry_t<Key_t, node_t*>)*cnt + sizeof(node_t*);
		    structural_data_unoccupied += sizeof(entry_t<Key_t, node_t*>)*invalid_num;
		    cur = static_cast<node_t*>((static_cast<inode_t<Key_t>*>(cur))->sibling_ptr);
		}while(cur);
		cur = (static_cast<inode_t<Key_t>*>(leftmost_node))->leftmost_ptr();
	    }

	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);
	    do{
		meta += sizeof(node_t) + sizeof(Key_t);
		auto cnt = leaf->get_cnt();
		auto invalid_num = lnode_t<Key_t>::cardinality - cnt;
		key_data_occupied += sizeof(entry_t<Key_t, uint64_t>)*cnt;
		key_data_unoccupied += sizeof(entry_t<Key_t, uint64_t>)*invalid_num;
		leaf = static_cast<lnode_t<Key_t>*>(leaf->sibling_ptr);
	    }while(leaf);
	}

    private:
	node_t* root;
};
}
#endif
