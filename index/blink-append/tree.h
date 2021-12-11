#ifndef TREE_APPEND_H__
#define TREE_APPEND_H__
#include "node.h"
#include <stack>

namespace BLINK_APPEND{

template <typename Key_t>
class btree_t{
    public:
#ifdef TIME
	std::atomic<uint64_t> time_internal_traverse;
	std::atomic<uint64_t> time_internal_sync;
	std::atomic<uint64_t> time_internal_write;
	std::atomic<uint64_t> time_leaf_traverse;
	std::atomic<uint64_t> time_leaf_sync;
	std::atomic<uint64_t> time_leaf_write;

	inline uint64_t _rdtsc(){
	    uint32_t lo, hi;
	    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
	    return (((uint64_t)hi << 32) | lo);
	}
#endif


	btree_t(){ 
	    root = static_cast<node_t*>(new lnode_t<Key_t>()); 
#ifdef TIME
	    time_internal_traverse.store(0, std::memory_order_relaxed);
	    time_internal_sync.store(0, std::memory_order_relaxed);
	    time_internal_write.store(0, std::memory_order_relaxed);
	    time_leaf_traverse.store(0, std::memory_order_relaxed);
	    time_leaf_sync.store(0, std::memory_order_relaxed);
	    time_leaf_write.store(0, std::memory_order_relaxed);
#endif
	}
	~btree_t(){ }

	void yield(int count){
	    if(count > 3)
		sched_yield();
	    else
		_mm_pause();
	}

	int check_height(){
	    return root->level;
	}

#ifdef TIME
	void update_time(uint64_t _time_internal_traverse, uint64_t _time_internal_sync, uint64_t _time_internal_write, uint64_t _time_leaf_traverse, uint64_t _time_leaf_sync, uint64_t _time_leaf_write){
	    time_internal_traverse.fetch_add(_time_internal_traverse, std::memory_order_relaxed);
	    time_internal_sync.fetch_add(_time_internal_sync, std::memory_order_relaxed);
	    time_internal_write.fetch_add(_time_internal_write, std::memory_order_relaxed);
	    time_leaf_traverse.fetch_add(_time_leaf_traverse, std::memory_order_relaxed);
	    time_leaf_sync.fetch_add(_time_leaf_sync, std::memory_order_relaxed);
	    time_leaf_write.fetch_add(_time_leaf_write, std::memory_order_relaxed);
	}
#endif

	void insert(Key_t key, uint64_t value){

#ifdef TIME
	    uint64_t _time_internal_sync, _time_internal_traverse, _time_internal_write, _time_leaf_sync, _time_leaf_traverse, _time_leaf_write;
	    _time_internal_sync = _time_internal_traverse = _time_internal_write = _time_leaf_sync = _time_leaf_traverse = _time_leaf_write = 0;
	    uint64_t start, end;
#endif
	    auto cur = root;

	    int stack_cnt = 0;
	    inode_t<Key_t>* stack[root->level];

	restart:
#ifdef TIME
	    start = _rdtsc();
#endif
	    bool need_restart = false;
	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart){
#ifdef TIME
		end = _rdtsc();
		_time_internal_sync += (end - start);
#endif
		goto restart;
	    }
#ifdef TIME
	    end = _rdtsc();
	    _time_internal_sync += (end - start);
	    start = _rdtsc();
#endif

	    // tree traversal
	    while(cur->level != 0){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key);
#ifdef TIME
		end = _rdtsc();
		_time_internal_traverse += (end - start);
		start = _rdtsc();
#endif
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart){
#ifdef TIME
		    end = _rdtsc();
		    _time_internal_sync += (end - start);
#endif
		    goto restart;
		}

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend)){
#ifdef TIME
		    end = _rdtsc();
		    _time_internal_sync += (end - start);
#endif
		    goto restart;
		}

#ifdef TIME
		end = _rdtsc();
		_time_internal_sync += (end - start);
		start = _rdtsc();
#endif
		if(child != cur->sibling_ptr)
		    stack[stack_cnt++] = static_cast<inode_t<Key_t>*>(cur);

		cur = child;
		cur_vstart = child_vstart;
	    }
#ifdef TIME
	    end = _rdtsc();
	    _time_internal_traverse += (end - start);
#endif

	    // found leaf
	    auto leaf = static_cast<lnode_t<Key_t>*>(cur);

	leaf_restart:
#ifdef TIME
	    start = _rdtsc();
#endif
	    need_restart = false;
	    auto leaf_vstart = leaf->try_readlock(need_restart);
	    if(need_restart){
#ifdef TIME
		end = _rdtsc();
		_time_leaf_sync += (end - start);
#endif
		goto leaf_restart;
	    }
#ifdef TIME
	    end = _rdtsc();
	    _time_leaf_sync += (end - start);
	    start = _rdtsc();
#endif

	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		auto sibling = static_cast<lnode_t<Key_t>*>(leaf->sibling_ptr);
#ifdef TIME
		end = _rdtsc();
		_time_leaf_traverse += (end - start);
		start = _rdtsc();
#endif
		auto sibling_v = sibling->try_readlock(need_restart);
		if(need_restart){
#ifdef TIME
		    end = _rdtsc();
		    _time_leaf_sync += (end - start);
#endif
		    goto leaf_restart;
		}

		auto leaf_vend = leaf->get_version(need_restart);
		if(need_restart || (leaf_vstart != leaf_vend)){
#ifdef TIME
		    end = _rdtsc();
		    _time_leaf_sync += (end - start);
#endif
		    goto leaf_restart;
		}
#ifdef TIME
		end = _rdtsc();
		_time_leaf_sync += (end - start);
		start = _rdtsc();
#endif
		
		leaf = sibling;
		leaf_vstart = sibling_v;
	    }

#ifdef TIME
	    end = _rdtsc();
	    _time_leaf_traverse += (end - start);
	    start = _rdtsc();
#endif
	    leaf->try_upgrade_writelock(leaf_vstart, need_restart);
	    if(need_restart){
#ifdef TIME
		end = _rdtsc();
		_time_leaf_sync += (end - start);
#endif
		goto leaf_restart;
	    }
#ifdef TIME
	    end = _rdtsc();
	    _time_leaf_sync += (end - start);
	    start = _rdtsc();
#endif
		
	    if(!leaf->is_full()){ // normal insert
#ifdef TIME
		end = _rdtsc();
		_time_leaf_traverse += (end - start);
		start = _rdtsc();
#endif
		leaf->insert(key, value);
#ifdef TIME
		end = _rdtsc();
		_time_leaf_write += (end - start);
		start = _rdtsc();
#endif
		leaf->write_unlock();
#ifdef TIME
		end = _rdtsc();
		_time_leaf_sync += (end - start);
		update_time(_time_internal_traverse, _time_internal_sync, _time_internal_write, _time_leaf_traverse, _time_leaf_sync, _time_leaf_write);
#endif
		return;
	    }
	    else{ // leaf node split
#ifdef TIME
		end = _rdtsc();
		_time_leaf_traverse += (end - start);
		start = _rdtsc();
#endif
		Key_t split_key; /// here
		auto new_leaf = leaf->split(split_key);
		if(key <= split_key)
		    leaf->insert(key, value);
		else
		    new_leaf->insert(key, value);
#ifdef TIME
		end = _rdtsc();
		_time_leaf_write += (end - start);
#endif

		if(stack_cnt){
		    int stack_idx = stack_cnt-1;
		    auto old_parent = stack[stack_idx];

		    auto original_node = static_cast<node_t*>(leaf);
		    auto new_node = static_cast<node_t*>(new_leaf);
		    //auto high_key = new_leaf->high_key;

		    while(stack_idx > -1){ // backtrack
			old_parent = stack[stack_idx];

		    parent_restart:
#ifdef TIME
			start = _rdtsc();
#endif
			need_restart = false;
			auto parent_vstart = old_parent->try_readlock(need_restart);
			if(need_restart){
#ifdef TIME
			    end = _rdtsc();
			    _time_internal_sync += (end - start);
#endif
			    goto parent_restart;
			}
#ifdef TIME
			end = _rdtsc();
			_time_internal_sync += (end - start);
			start = _rdtsc();
#endif
			
			while(old_parent->sibling_ptr && (old_parent->high_key < split_key)){
			    auto p_sibling = old_parent->sibling_ptr;
#ifdef TIME
			    end = _rdtsc();
			    _time_internal_traverse += (end - start);
			    start = _rdtsc();
#endif
			    auto p_sibling_v = p_sibling->try_readlock(need_restart);
			    if(need_restart){
#ifdef TIME
				end = _rdtsc();
				_time_internal_sync += (end - start);
#endif
				goto parent_restart;
			    }

			    auto parent_vend = old_parent->get_version(need_restart);
			    if(need_restart || (parent_vstart != parent_vend)){
#ifdef TIME
				end = _rdtsc();
				_time_internal_sync += (end - start);
#endif
				goto parent_restart;
			    }
#ifdef TIME
			    end = _rdtsc();
			    _time_internal_sync += (end - start);
			    start = _rdtsc();
#endif

			    old_parent = static_cast<inode_t<Key_t>*>(p_sibling);
			    parent_vstart = p_sibling_v;
			}
#ifdef TIME
			end = _rdtsc();
			_time_internal_traverse += (end - start);
			start = _rdtsc();
#endif

			old_parent->try_upgrade_writelock(parent_vstart, need_restart);
			if(need_restart){
#ifdef TIME
			    end = _rdtsc();
			    _time_internal_sync += (end - start);
#endif
			    goto parent_restart;
			}
#ifdef TIME
			end = _rdtsc();
			_time_internal_sync += (end - start);
			start = _rdtsc();
#endif

			original_node->write_unlock();
#ifdef TIME
			end = _rdtsc();
			if(original_node->level == 0)
			    _time_leaf_sync += (end - start);
			else
			    _time_internal_sync += (end - start);
			start = _rdtsc();
#endif

			if(!old_parent->is_full()){ // normal insert
#ifdef TIME
			    end = _rdtsc();
			    _time_internal_traverse += (end - start);
			    start = _rdtsc();
#endif
			    old_parent->insert(split_key, new_node);
#ifdef TIME
			    end = _rdtsc();
			    _time_internal_write += (end - start);
			    start = _rdtsc();
#endif
			    old_parent->write_unlock();
#ifdef TIME
			    end = _rdtsc();
			    _time_internal_sync += (end - start);
			    update_time(_time_internal_traverse, _time_internal_sync, _time_internal_write, _time_leaf_traverse, _time_leaf_sync, _time_leaf_write);
#endif
			    return;
			}

#ifdef TIME
			end = _rdtsc();
			_time_internal_traverse += (end - start);
			start = _rdtsc();
#endif
			// internal node split
			Key_t _split_key;
			auto new_parent = old_parent->split(_split_key);
			if(split_key <= _split_key)
			    old_parent->insert(split_key, new_node);
			else
			    new_parent->insert(split_key, new_node);
#ifdef TIME
			end = _rdtsc();
			_time_internal_write += (end - start);
#endif

			if(stack_idx){
#ifdef TIME
			    start = _rdtsc();
#endif
			    original_node = static_cast<node_t*>(old_parent);
			    new_node = static_cast<node_t*>(new_parent);
			    split_key = _split_key;
			    old_parent = stack[--stack_idx];
#ifdef TIME
			    end = _rdtsc();
			    _time_internal_traverse += (end - start);
#endif
			}
			else{ // set new root
			    if(old_parent == root){ // current node is root
#ifdef TIME
				start = _rdtsc();
#endif
				auto new_root = new inode_t<Key_t>(_split_key, old_parent, new_parent, nullptr, old_parent->level+1, new_parent->high_key);
				root = static_cast<node_t*>(new_root);
#ifdef TIME
				end = _rdtsc();
				_time_internal_write += (end - start);
				start = _rdtsc();
#endif
				old_parent->write_unlock();
#ifdef TIME
				end = _rdtsc();
				_time_internal_sync += (end - start);
				update_time(_time_internal_traverse, _time_internal_sync, _time_internal_write, _time_leaf_traverse, _time_leaf_sync, _time_leaf_write);
#endif
				return;
			    }
			    else{ // other thread has already created a new root
#ifdef TIME
				insert_key(_split_key, new_parent, old_parent, _time_internal_traverse, _time_internal_sync, _time_internal_write, _time_leaf_traverse, _time_leaf_sync, _time_leaf_write);
				update_time(_time_internal_traverse, _time_internal_sync, _time_internal_write, _time_leaf_traverse, _time_leaf_sync, _time_leaf_write);
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
#ifdef TIME
			start = _rdtsc();
#endif
			auto new_root = new inode_t<Key_t>(split_key, leaf, new_leaf, nullptr, root->level+1, new_leaf->high_key);
			root = static_cast<node_t*>(new_root);
#ifdef TIME
			end = _rdtsc();
			_time_internal_write += (end - start);
			start = _rdtsc();
#endif
			leaf->write_unlock();
#ifdef TIME
			end = _rdtsc();
			_time_leaf_sync += (end - start);
			update_time(_time_internal_traverse, _time_internal_sync, _time_internal_write, _time_leaf_traverse, _time_leaf_sync, _time_leaf_write);
#endif
			return;
		    }
		    else{ // other thread has already created a new root
#ifdef TIME
			insert_key(split_key, new_leaf, leaf, _time_internal_traverse, _time_internal_sync, _time_internal_write, _time_leaf_traverse, _time_leaf_sync, _time_leaf_write);
			update_time(_time_internal_traverse, _time_internal_sync, _time_internal_write, _time_leaf_traverse, _time_leaf_sync, _time_leaf_write);
#else
			insert_key(split_key, new_leaf, leaf);
#endif
			return;
		    }
		}
	    }
	}

	/* this function is called when root has been split by another threads */
#ifdef TIME
	void insert_key(Key_t key, node_t* value, node_t* prev, uint64_t& _time_internal_traverse, uint64_t& _time_internal_sync, uint64_t& _time_internal_write, uint64_t& _time_leaf_traverse, uint64_t& _time_leaf_sync, uint64_t& _time_leaf_write){
#else
	void insert_key(Key_t key, node_t* value, node_t* prev){
#endif
#ifdef TIME
	    uint64_t start, end;
#endif
	    auto cur = root;
	restart:
#ifdef TIME
	    start = _rdtsc();
#endif
	    
	    bool need_restart = false;

	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart){
#ifdef TIME
		end = _rdtsc();
		_time_internal_sync += (end - start);
#endif
		goto restart;
	    }
#ifdef TIME
	    end = _rdtsc();
	    _time_internal_sync += (end - start);
	    start = _rdtsc();
#endif

	    // since we need to find exact internal node which has been previously the root, we use readlock for traversal
	    while(cur->level != prev->level+1){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key);
#ifdef TIME
		end = _rdtsc();
		_time_internal_traverse += (end - start);
		start = _rdtsc();
#endif
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart){
#ifdef TIME
		    end = _rdtsc();
		    _time_internal_sync += (end - start);
#endif
		    goto restart;
		}

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend)){
#ifdef TIME
		    end = _rdtsc();
		    _time_internal_sync += (end - start);
#endif
		    goto restart;
		}
#ifdef TIME
		end = _rdtsc();
		_time_internal_sync += (end - start);
		start = _rdtsc();
#endif

		cur = child;
		cur_vstart = child_vstart;
	    }

#ifdef TIME
	    end = _rdtsc();
	    _time_internal_traverse += (end - start);
	    start = _rdtsc();
#endif
	    // found parent of prev node
	    while(cur->sibling_ptr && ((static_cast<inode_t<Key_t>*>(cur))->high_key < key)){
		auto sibling = cur->sibling_ptr;
#ifdef TIME
		end = _rdtsc();
		_time_internal_traverse += (end - start);
		start = _rdtsc();
#endif
		auto sibling_vstart = sibling->try_readlock(need_restart);
		if(need_restart){
#ifdef TIME
		    end = _rdtsc();
		    _time_internal_sync += (end - start);
#endif
		    goto restart;
		}

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend)){
#ifdef TIME
		    end = _rdtsc();
		    _time_internal_sync += (end - start);
#endif
		    goto restart;
		}

#ifdef TIME
		end = _rdtsc();
		_time_internal_sync += (end - start);
		start = _rdtsc();
#endif
		cur = static_cast<inode_t<Key_t>*>(sibling);
		cur_vstart = sibling_vstart;
	    }
#ifdef TIME
	    end = _rdtsc();
	    _time_internal_traverse += (end - start);
	    start = _rdtsc();
#endif

	    cur->try_upgrade_writelock(cur_vstart, need_restart);
	    if(need_restart){
#ifdef TIME
		end = _rdtsc();
		_time_internal_sync += (end - start);
#endif
		goto restart;
	    }
#ifdef TIME
	    end = _rdtsc();
	    _time_internal_sync += (end - start);
	    start = _rdtsc();
#endif
	    prev->write_unlock();
#ifdef TIME
	    end = _rdtsc();
	    if(prev->level == 0)
		_time_leaf_sync += (end - start);
	    else
		_time_internal_sync += (end - start);
	    start = _rdtsc();
#endif


	    auto node = static_cast<inode_t<Key_t>*>(cur);
	    
	    if(!node->is_full()){
#ifdef TIME
		end = _rdtsc();
		_time_internal_traverse += (end - start);
		start = _rdtsc();
#endif

		node->insert(key, value);
#ifdef TIME
		end = _rdtsc();
		_time_internal_write += (end - start);
		start = _rdtsc();
#endif
		node->write_unlock();
#ifdef TIME
		end = _rdtsc();
		_time_internal_sync += (end - start);
#endif
		return;
	    }
	    else{
#ifdef TIME
		end = _rdtsc();
		_time_internal_traverse += (end - start);
		start = _rdtsc();
#endif
		Key_t split_key;
		auto new_node = node->split(split_key);
		if(key <= split_key)
		    node->insert(key, value);
		else
		    new_node->insert(key, value);
#ifdef TIME
		end = _rdtsc();
		_time_internal_write += (end - start);
#endif

		if(node == root){ // if current nodes is root
#ifdef TIME
		    start = _rdtsc();
#endif
		    auto new_root = new inode_t<Key_t>(split_key, node, new_node, nullptr, node->level+1, new_node->high_key);
		    root = static_cast<node_t*>(new_root);
#ifdef TIME
		    end = _rdtsc();
		    _time_internal_write += (end - start);
		    start = _rdtsc();
#endif
		    node->write_unlock();
#ifdef TIME
		    end = _rdtsc();
		    _time_internal_sync += (end - start);
#endif
		    return;
		}
		else{ // other thread has already created a new root
#ifdef TIME
		    insert_key(split_key, new_node, node, _time_internal_traverse, _time_internal_sync, _time_internal_write, _time_leaf_traverse, _time_leaf_sync, _time_leaf_write);
#else
		    insert_key(split_key, new_node, node);
#endif
		    return;
		}
	    }
	}



	bool update(Key_t key, uint64_t value){

	    auto cur = root;

	restart:
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

	leaf_restart:
	    need_restart = false;
	    auto leaf_vstart = leaf->try_readlock(need_restart);
	    if(need_restart) goto leaf_restart;

	    // move right if necessary
	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		auto sibling = leaf->sibling_ptr;
		auto sibling_v = sibling->try_readlock(need_restart);
		if(need_restart) goto leaf_restart;

		auto leaf_vend = leaf->get_version(need_restart);
		if(need_restart || (leaf_vstart != leaf_vend)) goto leaf_restart;

		leaf = static_cast<lnode_t<Key_t>*>(sibling);
		leaf_vstart = sibling_v;
	    }

	    leaf->try_upgrade_writelock(leaf_vstart, need_restart);
	    if(need_restart) goto leaf_restart;

	    bool ret = leaf->update(key, value);
	    leaf->write_unlock();
	    return ret;
	}



	uint64_t lookup(Key_t key){
	    auto cur = root;

	restart:
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

	leaf_restart:
	    need_restart = false;
	    auto leaf_vstart = leaf->try_readlock(need_restart);
	    if(need_restart) goto leaf_restart;

	    // move right if necessary
	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		auto sibling = leaf->sibling_ptr;

		auto sibling_v = sibling->try_readlock(need_restart);
		if(need_restart) goto leaf_restart;

		auto leaf_vend = leaf->get_version(need_restart);
		if(need_restart || (leaf_vstart != leaf_vend)) goto leaf_restart;

		leaf = static_cast<lnode_t<Key_t>*>(sibling);
		leaf_vstart = sibling_v;
	    }

	    auto ret = leaf->find(key);
	    auto leaf_vend = leaf->get_version(need_restart);
	    if(need_restart || (leaf_vstart != leaf_vend)) goto leaf_restart;

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

	    auto leaf_vstart = leaf->try_readlock(need_restart);
	    if(need_restart) goto restart;

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

    private:
	node_t* root;
};
}
#endif
