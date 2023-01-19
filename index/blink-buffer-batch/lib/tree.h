#ifndef BLINK_BUFFER_BATCH_TREE_H__
#define BLINK_BUFFER_BATCH_TREE_H__

#include "node.h"

// BACKOFF flag can be enabled for high contention workload
//#define BACKOFF

namespace BLINK_BUFFER_BATCH{

template <typename Key_t, typename Value_t>
class btree_t{
    public:
	inline uint64_t _rdtsc(){
	    uint32_t lo, hi;
	    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
	    return (((uint64_t)hi << 32) | lo);
	}

	btree_t(){ 
	    root = static_cast<node_t*>(new lnode_t<Key_t, Value_t>()); 
	}
	~btree_t(){ }


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

	void insert_batch(lnode_t<Key_t, Value_t>** new_nodes, int num){
	    Key_t key = new_nodes[0]->leftmost_key();
	restart:
	    bool need_restart = false;
	    auto cur = root;

	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart)
		goto restart;

	    // tree traversal
	    while(cur->level != 0){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key);
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart)
		    goto restart;

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend))
		    goto restart;

		cur = child;
		cur_vstart = child_vstart;
	    }

	    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		auto sibling = static_cast<lnode_t<Key_t, Value_t>*>(leaf->sibling_ptr);
		auto sibling_v = sibling->try_readlock(need_restart);
		if(need_restart)
		    goto restart;

		auto leaf_vend = (static_cast<node_t*>(leaf))->get_version(need_restart);
		if(need_restart || (leaf_vstart != leaf_vend))
		    goto restart;

		leaf = sibling;
		leaf_vstart = sibling_v;
	    }

	    leaf->try_upgrade_writelock(leaf_vstart, need_restart);
	    if(need_restart)
		goto restart;

	    leaf->set_high_key();
	    leaf->sibling_ptr = new_nodes[0];
	    Key_t split_key[num];
	    split_key[0] = leaf->high_key;
	    for(int i=1; i<num; i++)
		split_key[i] = new_nodes[i-1]->high_key;
	    
	    batch_insert(split_key, reinterpret_cast<node_t**>(new_nodes), num, static_cast<node_t*>(leaf));
	}

	void new_root_adjustment(Key_t* key, node_t** value, int num, node_t* prev=nullptr){
	    int batch_size = inode_t<Key_t>::cardinality * FILL_FACTOR;
	    int new_num;
	    int numerator = num / (batch_size+1);
	    int remains = num % (batch_size+1);
	    if(numerator == 0)
		new_num = 1;
	    else{
		if(remains == 0)
		    new_num = numerator;
		else{
		    if(remains < inode_t<Key_t>::cardinality-batch_size)
			new_num = numerator;
		    else
			new_num = numerator + 1;
		}
	    }

	    if(new_num == 1){
		auto new_node = new inode_t<Key_t>(value[0]->level + 1);
		new_node->batch_insert_root(key, value, num, prev);
		root = static_cast<node_t*>(new_node);
		if(prev != nullptr)
		    prev->write_unlock();
	    }
	    else{
		auto new_nodes = new inode_t<Key_t>*[new_num];
		int idx = 0;
		for(int i=0; i<new_num; i++)
		    new_nodes[i] = new inode_t<Key_t>(value[0]->level + 1);

		for(int i=0; i<new_num; i++){
		    if(i < new_num-1)
			new_nodes[i]->sibling_ptr = static_cast<node_t*>(new_nodes[i+1]);
		    //new_nodes[i]->batch_insert_root(key, value, batch_size, idx, num, prev);
		    if(i == 0)
			new_nodes[i]->batch_insert_root(key, value, batch_size, idx, num, prev);
		    else
			new_nodes[i]->batch_insert_root(key, value, batch_size, idx, num);
		}
		Key_t split_key[new_num];
		for(int i=0; i<new_num; i++)
		    split_key[i] = new_nodes[i]->high_key;
		new_root_adjustment(split_key, reinterpret_cast<node_t**>(new_nodes), new_num);
		if(prev != nullptr)
		    prev->write_unlock();
		delete[] new_nodes;
	    }
	}
	    
	void batch_insert(Key_t* key, node_t** value, int num, node_t* prev){
	restart:
	    auto cur = root;
	    if(cur == prev){ // prev is root -- lock for prev is already held
		new_root_adjustment(key, value, num, cur);
		//cur->write_unlock();
		return;
	    }

	    bool need_restart = false;
	    auto cur_vstart = cur->get_version(need_restart);
	    if(need_restart)
		goto restart;

	    while(cur->level != prev->level+1){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key[0]);
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart)
		    goto restart;

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend))
		    goto restart;

		cur = child;
		cur_vstart = child_vstart;
	    }

	    // found parent
	    while(cur->sibling_ptr && ((static_cast<inode_t<Key_t>*>(cur))->high_key < key[0])){
		auto sibling = cur->sibling_ptr;
		auto sibling_vstart = sibling->try_readlock(need_restart);
		if(need_restart)
		    goto restart;

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend))
		    goto restart;

		cur = static_cast<inode_t<Key_t>*>(sibling);
		cur_vstart = sibling_vstart;
	    }

	    cur->try_upgrade_writelock(cur_vstart, need_restart);
	    if(need_restart)
		goto restart;

	    prev->write_unlock();

	    auto parent = static_cast<inode_t<Key_t>*>(cur);
	    int new_num = 0;
	    auto new_nodes = parent->batch_insert(key, value, num, new_num);

	    if(new_nodes == nullptr){ // inplace update
		parent->write_unlock();
		return;
	    }

	    Key_t split_key[new_num];
	    split_key[0] = parent->high_key;
	    for(int i=1; i<new_num; i++)
		split_key[i] = new_nodes[i-1]->high_key;

	    if(parent != root){ // update non-root parent
		batch_insert(split_key, reinterpret_cast<node_t**>(new_nodes), new_num, static_cast<node_t*>(parent));
	    }
	    else{ // create new root
		// TODO: recursive roots
		new_root_adjustment(split_key, reinterpret_cast<node_t**>(new_nodes), new_num, parent);
		//parent->write_unlock();
	    }
	}
	    
	void insert(Key_t key, Value_t value) {
	    int restart_cnt = -1;
	restart:
	    auto cur = root;

	    int stack_cnt = 0;
	    inode_t<Key_t>* stack[root->level];

	    bool need_restart = false;
	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart){
		goto restart;
	    }

	    // tree traversal
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
	    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		auto sibling = static_cast<lnode_t<Key_t, Value_t>*>(leaf->sibling_ptr);
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
		
	    if(!leaf->is_full()){ // normal insert
		leaf->insert(key, value);
		leaf->write_unlock();
		return;
	    }
	    else{ // leaf node split
		Key_t split_key; /// here
		auto new_leaf = leaf->split(split_key);
		if(key <= split_key)
		    leaf->insert(key, value);
		else
		    new_leaf->insert(key, value);

		if(stack_cnt){
		    int stack_idx = stack_cnt-1;
		    auto old_parent = stack[stack_idx];

		    auto original_node = static_cast<node_t*>(leaf);
		    auto new_node = static_cast<node_t*>(new_leaf);

		    while(stack_idx > -1){ // backtrack
			old_parent = stack[stack_idx];

		    parent_restart:
			need_restart = false;
			auto parent_vstart = old_parent->try_readlock(need_restart);
			if(need_restart){
			    goto parent_restart;
			}
		
			while(old_parent->sibling_ptr && (old_parent->high_key < split_key)){
			    auto p_sibling = old_parent->sibling_ptr;
			    auto p_sibling_v = p_sibling->try_readlock(need_restart);
			    if(need_restart){
				goto parent_restart;
			    }

			    auto parent_vend = old_parent->get_version(need_restart);
			    if(need_restart || (parent_vstart != parent_vend)){
				goto parent_restart;
			    }

			    old_parent = static_cast<inode_t<Key_t>*>(p_sibling);
			    parent_vstart = p_sibling_v;
			}

			old_parent->try_upgrade_writelock(parent_vstart, need_restart);
			if(need_restart){
			    goto parent_restart;
			}

			original_node->write_unlock();

			if(!old_parent->is_full()){ // normal insert
			    old_parent->insert(split_key, new_node);
			    old_parent->write_unlock();
			    return;
			}

			// internal node split
			Key_t _split_key;
			auto new_parent = old_parent->split(_split_key);
			if(split_key <= _split_key)
			    old_parent->insert(split_key, new_node);
			else
			    new_parent->insert(split_key, new_node);

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
				old_parent->write_unlock();
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
			auto new_root = new inode_t<Key_t>(split_key, leaf, new_leaf, nullptr, root->level+1, new_leaf->high_key);
			root = static_cast<node_t*>(new_root);
			leaf->write_unlock();
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
	restart:
	    auto cur = root;
	    bool need_restart = false;

	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart){
		goto restart;
	    }

	    // since we need to find the internal node which has been previously the root, we use readlock for traversal
	    while(cur->level != prev->level+1){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(key);
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart){
		    goto restart;
		}

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend)){
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
		    goto restart;
		}

		auto cur_vend = cur->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend)){
		    goto restart;
		}

		cur = static_cast<inode_t<Key_t>*>(sibling);
		cur_vstart = sibling_vstart;
	    }

	    cur->try_upgrade_writelock(cur_vstart, need_restart);
	    if(need_restart){
		goto restart;
	    }
	    prev->write_unlock();

	    auto node = static_cast<inode_t<Key_t>*>(cur);
	    
	    if(!node->is_full()){
		node->insert(key, value);
		node->write_unlock();
		return;
	    }
	    else{
		Key_t split_key;
		auto new_node = node->split(split_key);
		if(key <= split_key)
		    node->insert(key, value);
		else
		    new_node->insert(key, value);

		if(node == root){ // if current nodes is root
		    auto new_root = new inode_t<Key_t>(split_key, node, new_node, nullptr, node->level+1, new_node->high_key);
		    root = static_cast<node_t*>(new_root);
		    node->write_unlock();
		    return;
		}
		else{ // other thread has already created a new root
		    insert_key(split_key, new_node, node);
		    return;
		}
	    }
	}



	bool update(Key_t key, Value_t value){
	    int restart_cnt = -1;
	restart:
	    backoff(restart_cnt++);
	    auto cur = root;
	    bool need_restart = false;
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
		#ifdef UPDATE_LOCK
                if(need_restart || ((cur_vstart & (~0u)) != (cur_vend & (~0u)))){
                #else
		if(need_restart || (cur_vstart != cur_vend)){
		#endif
		    goto restart;
		}

		cur = child;
		cur_vstart = child_vstart;
	    }

	    // found leaf
	    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    // move right if necessary
	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		auto sibling = leaf->sibling_ptr;
		auto sibling_v = sibling->try_readlock(need_restart);
		if(need_restart){
		    goto restart;
		}

		auto leaf_vend = leaf->get_version(need_restart);
		#ifdef UPDATE_LOCK
		if(need_restart || ((leaf_vstart & (~0u)) != (leaf_vend & (~0u)))){
		#else
		if(need_restart || (leaf_vstart != leaf_vend)){
		#endif
		    goto restart;
		}

		leaf = static_cast<lnode_t<Key_t, Value_t>*>(sibling);
		leaf_vstart = sibling_v;
	    }

	    #ifdef UPDATE_LOCK
	    leaf->try_upgrade_updatelock(leaf_vstart, need_restart);
	    #else
	    leaf->try_upgrade_writelock(leaf_vstart, need_restart);
	    #endif
	    if(need_restart){
		goto restart;
	    }

	    bool ret = leaf->update(key, value);

	    #ifdef UPDATE_LOCK
	    leaf->update_unlock();
	    #else
	    leaf->write_unlock();
	    #endif

	    return ret;
	}


	Value_t lookup(Key_t key){
	restart:
	    auto cur = root;
	    bool need_restart = false;

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
		#ifdef UPDATE_LOCK
                if(need_restart || ((cur_vstart & (~0u)) != (cur_vend & (~0u)))){
                #else
		if(need_restart || (cur_vstart != cur_vend)){
		#endif
		    goto restart;
		}

		cur = child;
		cur_vstart = child_vstart;
	    }

	    // found leaf
	    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    // move right if necessary
	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		auto sibling = leaf->sibling_ptr;
		auto sibling_v = sibling->try_readlock(need_restart);
		if(need_restart){
		    goto restart;
		}

		auto leaf_vend = leaf->get_version(need_restart);
		#ifdef UPDATE_LOCK
		if(need_restart || ((leaf_vstart & (~0u)) != (leaf_vend & (~0u)))){
		#else
		if(need_restart || (leaf_vstart != leaf_vend)){
		#endif
		    goto restart;
		}

		leaf = static_cast<lnode_t<Key_t, Value_t>*>(sibling);
		leaf_vstart = sibling_v;
	    }


	    auto ret = leaf->find(key);

	    auto leaf_vend = leaf->get_version(need_restart);
  	    #ifdef UPDATE_LOCK
	    if(need_restart || ((leaf_vstart & (~0u)) != (leaf_vend & (~0u)))){
	    #else
	    if(need_restart || (leaf_vstart != leaf_vend)){
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
	    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		auto sibling = static_cast<lnode_t<Key_t, Value_t>*>(leaf->sibling_ptr);
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

	    

	int range_lookup(Key_t min_key, int range, Value_t* buf){
	restart:
	    auto cur = root;
	    bool need_restart = false;
	    auto cur_vstart = cur->try_readlock(need_restart);
	    if(need_restart){
		goto restart;
	    }

	    // traversal
	    while(cur->level != 0){
		auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(min_key);
		auto child_vstart = child->try_readlock(need_restart);
		if(need_restart){
		    goto restart;
		}

		auto cur_vend = cur->get_version(need_restart);
		#ifdef UPDATE_LOCK
                if(need_restart || ((cur_vstart & (~0u)) != (cur_vend & (~0u)))){
                #else
		if(need_restart || (cur_vstart != cur_vend)){ 
		#endif
		    goto restart;
		}

		cur = child;
		cur_vstart = child_vstart;
	    }

	    // found leaf
	    int count = 0;
	    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    // move right if necessary
	    while(leaf->sibling_ptr && (leaf->high_key < min_key)){
		auto sibling = leaf->sibling_ptr;

		auto sibling_v = sibling->try_readlock(need_restart);
		if(need_restart){
		    goto restart;
		}

		auto leaf_vend = leaf->get_version(need_restart);
		#ifdef UPDATE_LOCK
                if(need_restart || ((leaf_vstart & (~0u)) != (leaf_vend & (~0u)))){
                #else
		if(need_restart || (leaf_vstart != leaf_vend)){
		#endif
		    goto restart;
		}

		leaf = static_cast<lnode_t<Key_t, Value_t>*>(sibling);
		leaf_vstart = sibling_v;
	    }


	    auto idx = leaf->find_lowerbound(min_key);
	    while(count < range){
		auto ret = leaf->range_lookup(idx, buf, count, range);
		auto sibling = leaf->sibling_ptr;
		// collected all keys within range or reaches the rightmost leaf
		if((ret == range) || !sibling){
		    auto leaf_vend = leaf->get_version(need_restart);
		    #ifdef UPDATE_LOCK
		    if(need_restart || ((leaf_vstart & (~0u)) != (leaf_vend & (~0u)))){
                    #else
		    if(need_restart || (leaf_vstart != leaf_vend)){
		    #endif
			goto restart;
		    }
		    return ret;
		}
		auto sibling_vstart = sibling->try_readlock(need_restart);
		if(need_restart){
		    goto restart;
		}

		auto leaf_vend = leaf->get_version(need_restart);
		#ifdef UPDATE_LOCK
                if(need_restart || ((leaf_vstart & (~0u)) != (leaf_vend & (~0u)))){
                #else
		if(need_restart || (leaf_vstart != leaf_vend)){
		#endif
		    goto restart;
		}

		leaf = static_cast<lnode_t<Key_t, Value_t>*>(sibling);
		leaf_vstart = sibling_vstart;
		count = ret;
		idx = 0;
	    }
	    return count;
	}

	void check_locks(){
	    std::cout << "checking locks ... ";
	    auto cur = static_cast<inode_t<Key_t>*>(root);
	    int level = 0;
	    while(cur->level != 0){
		auto _cur = cur;
		do{
		    bool need_restart = false;
		    auto v = cur->get_version(need_restart);
		    if(need_restart)
			std::cout << "inode " << cur << " at lv " << cur->level << " is locked! " << std::endl;
		}while((cur = static_cast<inode_t<Key_t>*>(cur->sibling_ptr)));
		level++;
		cur = static_cast<inode_t<Key_t>*>(_cur->leftmost_ptr());
	    }

	    auto leaf = reinterpret_cast<lnode_t<Key_t, Value_t>*>(cur);
	    do{
		bool need_restart = false;
		auto v = leaf->get_version(need_restart);
		if(need_restart)
		    std::cout << "lnode " << leaf << " is locked!" << std::endl;
	    }while((leaf = static_cast<lnode_t<Key_t, Value_t>*>(leaf->sibling_ptr)));
	    std::cout << " done!" << std::endl;
	}

	void print_leaf(){
	    auto cur = root;
	    while(cur->level != 0){
		cur = (static_cast<inode_t<Key_t>*>(cur))->leftmost_ptr();
	    }
	    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    int cnt = 1;
	    do{
		std::cout << "L" << cnt << "(" << leaf << ": ";
		leaf->print();
		cnt++;
	    }while((leaf = static_cast<lnode_t<Key_t, Value_t>*>(leaf->sibling_ptr)));
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

	    auto l = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    l->sanity_check(l->high_key, true);
	}

	uint64_t find_anyway(Key_t key){
	    auto cur = root;
	    while(cur->level != 0){
		cur = (static_cast<inode_t<Key_t>*>(cur))->leftmost_ptr();
	    }

	    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    lnode_t<Key_t, Value_t>* before;
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
		leaf = static_cast<lnode_t<Key_t, Value_t>*>(leaf->sibling_ptr);
	    }while(leaf);

	    return 0;
	}

	double utilization(){
	    auto cur = root;
	    while(cur->level != 0){
		cur = (static_cast<inode_t<Key_t>*>(cur))->leftmost_ptr();
	    }
	    
	    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    double util = 0;
	    int leaf_cnt = 0;
	    while(leaf){
		util += leaf->utilization();
		leaf_cnt++;
		leaf = static_cast<lnode_t<Key_t, Value_t>*>(leaf->sibling_ptr);
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

	    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    do{
		meta += sizeof(node_t) + sizeof(Key_t);
		auto cnt = leaf->get_cnt();
		auto invalid_num = lnode_t<Key_t, Value_t>::cardinality - cnt;
		key_data_occupied += sizeof(entry_t<Key_t, Value_t>)*cnt;
		key_data_unoccupied += sizeof(entry_t<Key_t, Value_t>)*invalid_num;
		leaf = static_cast<lnode_t<Key_t, Value_t>*>(leaf->sibling_ptr);
	    }while(leaf);
	}

    private:
	node_t* root;
};
}
#endif
