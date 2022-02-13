#ifndef BLINK_HASH_TREE_H__
#define BLINK_HASH_TREE_H__

#ifdef OLD
#include "linked_node.h"
#else
#include "linked_node2.h"
#endif

//#include "node.h"

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
	    root = static_cast<node_t*>(new lnode_t<Key_t, Value_t>()); 
	}
	~btree_t(){ }

	void yield(int count){
	    if(count > 3)
		sched_yield();
	}

	int check_height(){
	    auto ret = utilization();
	    return root->level;
	}

	void insert(Key_t key, Value_t value){
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

		if(child != cur->sibling_ptr)
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

		auto leaf_vend = (static_cast<node_t*>(leaf))->get_version(need_restart);
		if(need_restart || (leaf_vstart != leaf_vend)){
		    goto restart;
		}
		
		leaf = sibling;
		leaf_vstart = sibling_v;
	    }

	    auto ret = leaf->insert(key, value, leaf_vstart);
	    if(ret == -1) // leaf node has been split while inserting
		goto restart;
	    else if(ret == 0) // insertion succeeded
		return;
	    else{ // leaf node split
		Key_t split_key;
		//#ifdef LINKED
		auto new_leaf = leaf->split(split_key, key, value, leaf_vstart);
		//#else
		//auto new_leaf = leaf->split(split_key, leaf_vstart);
		//#endif
		if(new_leaf == nullptr){
		    goto restart; // another thread has already splitted this leaf node
		}

		/*
		#ifndef LINKED
		if(key <= split_key){
		    leaf->insert_after_split(key, value);
		}
		else{
		    new_leaf->insert_after_split(key, value);
		}
		#endif
		*/

		if(stack_cnt){
		    int stack_idx = stack_cnt-1;
		    auto old_parent = stack[stack_idx];

		    auto original_node = static_cast<node_t*>(leaf);
		    auto new_node = static_cast<node_t*>(new_leaf);
		    while(stack_idx > -1){ // backtrack parent nodes
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

			if(original_node->level != 0){ // internal node
			    original_node->write_unlock();
			}
			else{ // leaf node
			    (static_cast<lnode_t<Key_t, Value_t>*>(original_node))->_split_unlock();
			}


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
			leaf->_split_unlock();
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

	    // since we need to find exact internal node which has been previously the root, we use readlock for traversal
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

	    // found parent of prev node
	    while(cur->sibling_ptr && ((static_cast<inode_t<Key_t>*>(cur))->high_key < key)){
		auto sibling = cur->sibling_ptr;
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
	    if(prev->level != 0){
		prev->write_unlock();
	    }
	    else{
		(static_cast<lnode_t<Key_t, Value_t>*>(prev))->_split_unlock();
	    }

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
	    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    // move right if necessary
	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		auto sibling = leaf->sibling_ptr;
		auto sibling_v = sibling->try_readlock(need_restart);
		if(need_restart) goto restart;

		auto leaf_vend = leaf->get_version(need_restart);
		if(need_restart || (leaf_vstart != leaf_vend)) goto restart;

		leaf = static_cast<lnode_t<Key_t, Value_t>*>(sibling);
		leaf_vstart = sibling_v;
	    }

	    auto ret = leaf->update(key, value, leaf_vstart);
	    if(ret == -1)
		goto restart;

	    auto leaf_vend = leaf->get_version(need_restart);
	    if(need_restart || (leaf_vstart != leaf_vend))
		goto restart;

	    if(ret == 0)
		return true;
	    return false;
	}


	Value_t lookup(Key_t key){
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
	    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    // move right if necessary
	    while(leaf->sibling_ptr && (leaf->high_key < key)){
		auto sibling = leaf->sibling_ptr;

		auto sibling_v = sibling->try_readlock(need_restart);
		if(need_restart) goto restart;

		auto leaf_vend = leaf->get_version(need_restart);
		if(need_restart || (leaf_vstart != leaf_vend)) goto restart;

		leaf = static_cast<lnode_t<Key_t, Value_t>*>(sibling);
		leaf_vstart = sibling_v;
	    }
	    auto ret = leaf->find(key, need_restart);
	    if(need_restart) goto restart;
	    
	    auto leaf_vend = leaf->get_version(need_restart);
	    if(need_restart || (leaf_vstart != leaf_vend)) 
		goto restart;

	    return ret;
	}


	int range_lookup(Key_t min_key, int range, Value_t* buf){

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
	    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    while(count < range){

		// move right if necessary
		while(leaf->sibling_ptr && (leaf->high_key < min_key)){
		    auto sibling = leaf->sibling_ptr;

		    auto sibling_v = sibling->try_readlock(need_restart);
		    if(need_restart) goto restart;

		    auto leaf_vend = leaf->get_version(need_restart);
		    if(need_restart || (leaf_vstart != leaf_vend)) goto restart;

		    leaf = static_cast<lnode_t<Key_t, Value_t>*>(sibling);
		    leaf_vstart = sibling_v;
		}

		auto ret = leaf->range_lookup(min_key, buf, count, range);
		if(ret == -1)
		    goto restart;

		auto sibling = leaf->sibling_ptr;

		auto leaf_vend = leaf->get_version(need_restart);
		if(need_restart || (leaf_vstart != leaf_vend))
		    goto restart;

		if(ret == range)
		    return ret;

		// reaches to the rightmost leaf
		if(!sibling) break;

		auto sibling_vstart = sibling->try_readlock(need_restart);
		if(need_restart) goto restart;

		leaf = static_cast<lnode_t<Key_t, Value_t>*>(sibling);
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
		cur = static_cast<inode_t<Key_t>*>(cur->leftmost_ptr);
	    }
	}

	void print(){
	    print_internal();
	    print_leaf();
	}

	void sanity_check(){
	    auto cur = root;
	    while(cur->level != 0){
		auto p = static_cast<inode_t<Key_t>*>(cur);
		p->sanity_check(p->high_key, true);
		cur = cur->leftmost_ptr;
	    }

	    auto l = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    l->sanity_check(l->high_key, true);
	}

	Value_t find_anyway(Key_t key){
	    auto cur = root;
	    while(cur->level != 0){
		cur = cur->leftmost_ptr;
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
	    auto node = cur;
	    while(cur->level != 0){
		uint64_t total = 0;
		uint64_t count = 0;
		while(node){
		    total += inode_t<Key_t>::cardinality;
		    count += node->get_cnt();
		    node = node->sibling_ptr;
		}
		std::cout << "inode lv " << cur->level-1 << ": " << (double)count/total*100.0 << " \%" << std::endl;
		cur = cur->leftmost_ptr;
		node = cur;
	    }

	    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    int leaf_cnt = 0;
	    double util = 0;
	    do{
		leaf_cnt++;
		util += leaf->utilization();

		leaf = static_cast<lnode_t<Key_t, Value_t>*>(leaf->sibling_ptr);
	    }while(leaf);
	    std::cout << "leaf " << (double)util/leaf_cnt*100.0 << " \%" << std::endl;
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
