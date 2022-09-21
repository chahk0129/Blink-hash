#include "tree.h"

namespace BLINK_HASH{

template <typename Key_t, typename Value_t>
inline void btree_t<Key_t, Value_t>::yield(int count){
    if(count > 3)
	sched_yield();
}

template <typename Key_t, typename Value_t>
int btree_t<Key_t, Value_t>::check_height(){
    auto ret = utilization();
    return root->level;
}

template <typename Key_t, typename Value_t>
void btree_t<Key_t, Value_t>::insert(Key_t key, Value_t value, ThreadInfo& epocheThreadInfo){
    EpocheGuard epocheGuard(epocheThreadInfo);
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
	if(need_restart)
	    goto restart;

	auto cur_vend = cur->get_version(need_restart);
	if(need_restart || (cur_vstart != cur_vend))
	    goto restart;

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
	if(need_restart)
	    goto restart;

	auto leaf_vend = (static_cast<node_t*>(leaf))->get_version(need_restart);
	if(need_restart || (leaf_vstart != leaf_vend))
	    goto restart;

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
	auto new_leaf = leaf->split(split_key, key, value, leaf_vstart);
	if(new_leaf == nullptr)
	    goto restart; // another thread has already splitted this leaf node

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
		    if(need_restart)
			goto parent_restart;

		    auto parent_vend = old_parent->get_version(need_restart);
		    if(need_restart || (parent_vstart != parent_vend))
			goto parent_restart;

		    old_parent = static_cast<inode_t<Key_t>*>(p_sibling);
		    parent_vstart = p_sibling_v;
		}

		old_parent->try_upgrade_writelock(parent_vstart, need_restart);
		if(need_restart)
		    goto parent_restart;

		if(original_node->level != 0) // internal node
		    original_node->write_unlock();
		else // leaf node
		    (static_cast<lnode_t<Key_t, Value_t>*>(original_node))->write_unlock();

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
		auto new_root = new inode_t<Key_t>(split_key, leaf, new_leaf, nullptr, root->level+1, (static_cast<lnode_t<Key_t, Value_t>*>(new_leaf))->high_key);
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
template <typename Key_t, typename Value_t>
void btree_t<Key_t, Value_t>::insert_key(Key_t key, node_t* value, node_t* prev){
    restart:
    auto cur = root;
    bool need_restart = false;

    auto cur_vstart = cur->try_readlock(need_restart);
    if(need_restart)
	goto restart;

    // since we need to find exact internal node which has been previously the root, we use readlock for traversal
    while(cur->level != prev->level+1){
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

    // found parent of prev node
    while(cur->sibling_ptr && ((static_cast<inode_t<Key_t>*>(cur))->high_key < key)){
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

    if(prev->level != 0)
	prev->write_unlock();
    else
	(static_cast<lnode_t<Key_t, Value_t>*>(prev))->write_unlock();

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


template <typename Key_t, typename Value_t>
bool btree_t<Key_t, Value_t>::update(Key_t key, Value_t value, ThreadInfo& threadEpocheInfo){
    EpocheGuardReadonly epocheGuard(threadEpocheInfo);
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

    if(ret == 0)
	return true;
    return false;
}

template <typename Key_t, typename Value_t>
Value_t btree_t<Key_t, Value_t>::lookup(Key_t key, ThreadInfo& threadEpocheInfo){
    EpocheGuardReadonly epocheGuard(threadEpocheInfo);
    restart:
    auto cur = root;
    bool need_restart = false;

    auto cur_vstart = cur->try_readlock(need_restart);
    if(need_restart)
	goto restart;

    // traversal
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

template <typename Key_t, typename Value_t>
inode_t<Key_t>** btree_t<Key_t, Value_t>::new_root_for_adjustment(Key_t* key, node_t** value, int num, int& new_num){
    size_t batch_size = inode_t<Key_t>::cardinality * FILL_FACTOR;
    if(num % batch_size == 0)
	new_num = num / batch_size;
    else
	new_num = num / batch_size + 1;

    auto new_roots = new inode_t<Key_t>*[new_num];
    int idx = 0;
    for(int i=0; i<new_num; i++){
	new_roots[i] = new inode_t<Key_t>(value[0]->level+1); // level
//	new_roots[i]->batch_insert(key, value, idx, num, batch_size);
	if(i < new_num-1)
	    new_roots[i]->sibling_ptr = static_cast<node_t*>(new_roots[i+1]);
    }
    return new_roots;
}

template <typename Key_t, typename Value_t>
void btree_t<Key_t, Value_t>::batch_insert(Key_t* key, node_t** value, int num, node_t* prev, ThreadInfo& threadEpocheInfo){
    restart:
    auto cur = root;
    bool need_restart = false;

    auto cur_vstart = cur->try_readlock(need_restart);
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

    if(prev->level == 0){
	value[0]->write_unlock();
	(static_cast<lnode_t<Key_t, Value_t>*>(prev))->convert_unlock_obsolete();
	threadEpocheInfo.getEpoche().markNodeForDeletion(prev, threadEpocheInfo);
    }
    else{
	prev->write_unlock();
    }

    auto parent = static_cast<inode_t<Key_t>*>(cur);
    int new_num = 0;
    inode_t<Key_t>** new_nodes;
    if(parent->level == 1)
	new_nodes = parent->batch_insert_last_level(key, value, num, new_num);
    else
	new_nodes = parent->batch_insert(key, value, num, new_num);
    delete[] value;

    if(new_nodes == nullptr){
	parent->write_unlock();
	return;
    }

    Key_t split_key[new_num];
    split_key[0] = parent->high_key;
    for(int i=1; i<new_num; i++)
	split_key[i] = new_nodes[i-1]->high_key;

    if(parent != root){ // update non-root parent
	batch_insert(split_key, reinterpret_cast<node_t**>(new_nodes), new_num, static_cast<node_t*>(parent), threadEpocheInfo);
	return;
    }
    else{ // create new root
	// TODO: recursive roots
	while(inode_t<Key_t>::cardinality < new_num){
	    int _new_num = 0;
	    auto new_roots = new_root_for_adjustment(split_key, reinterpret_cast<node_t**>(new_nodes), new_num, _new_num);
	    new_nodes = new_roots;
	    new_num = _new_num;
	}

	auto new_root = new inode_t<Key_t>(new_nodes[0]->level+1);
	new_root->insert_for_root(split_key, reinterpret_cast<node_t**>(new_nodes), static_cast<node_t*>(parent), new_num);
	root = new_root;
	parent->write_unlock();
	return;
    }
}


template <typename Key_t, typename Value_t>
int btree_t<Key_t, Value_t>::range_lookup(Key_t min_key, int range, Value_t* buf, ThreadInfo& threadEpocheInfo){
    EpocheGuard epocheGuard(threadEpocheInfo);
    restart:
    auto cur = root;
    bool need_restart = false;
    auto cur_vstart = cur->try_readlock(need_restart);
    if(need_restart)
	goto restart;

    // traversal
    while(cur->level != 0){
	auto child = (static_cast<inode_t<Key_t>*>(cur))->scan_node(min_key);
	auto child_vstart = child->try_readlock(need_restart);
	if(need_restart)
	    goto restart;

	auto cur_vend = cur->get_version(need_restart);
	if(need_restart || (cur_vstart != cur_vend))
	    goto restart;

	cur = child;
	cur_vstart = child_vstart;
    }

    // found leaf
    int count = 0;
    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
    auto leaf_vstart = cur_vstart;

    bool continued = false;
    while(count < range){
	// move right if necessary
	while(leaf->sibling_ptr && (leaf->high_key < min_key)){
	    auto sibling = leaf->sibling_ptr;

	    auto sibling_v = sibling->try_readlock(need_restart);
	    if(need_restart)
		goto restart;

	    auto leaf_vend = leaf->get_version(need_restart);
	    if(need_restart || (leaf_vstart != leaf_vend))
		goto restart;

	    leaf = static_cast<lnode_t<Key_t, Value_t>*>(sibling);
	    leaf_vstart = sibling_v;
	}

	auto ret = leaf->range_lookup(min_key, buf, count, range, continued);
	if(ret == -1)
	    goto restart;
	else if(ret == -2){
	    auto ret_ = convert(leaf, leaf_vstart, threadEpocheInfo);
	    goto restart;
	}
	continued = true;

	auto sibling = leaf->sibling_ptr;

	auto leaf_vend = leaf->get_version(need_restart);
	if(need_restart || (leaf_vstart != leaf_vend))
	    goto restart;

	if(ret == range)
	    return ret;

	// reaches to the rightmost leaf
	if(!sibling) break;

	auto sibling_vstart = sibling->try_readlock(need_restart);
	if(need_restart)
	    goto restart;

	leaf = static_cast<lnode_t<Key_t, Value_t>*>(sibling);
	leaf_vstart = sibling_vstart;
	count = ret;
    }
    return count;
}

template <typename Key_t, typename Value_t>
bool btree_t<Key_t, Value_t>::convert(lnode_t<Key_t, Value_t>* leaf, uint64_t leaf_version, ThreadInfo& threadEpocheInfo){
    int num = 0;
    auto nodes = (static_cast<lnode_hash_t<Key_t, Value_t>*>(leaf))->convert(num, leaf_version);
    if(nodes == nullptr){
	return false;
    }

    Key_t split_key[num];
    split_key[0] = nodes[0]->high_key;
    for(int i=1; i<num; i++)
	split_key[i] = nodes[i-1]->high_key;

    batch_insert(split_key, reinterpret_cast<node_t**>(nodes), num, static_cast<node_t*>(leaf), threadEpocheInfo);
    return true;
}


template <typename Key_t, typename Value_t>
void btree_t<Key_t, Value_t>::convert_all(ThreadInfo& threadEpocheInfo){
    EpocheGuard epocheGuard(threadEpocheInfo);
    auto cur = root;
    while(cur->level != 0){
	cur = cur->leftmost_ptr;
    }

    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
    bool need_restart = false;
    auto cur_vstart = cur->get_version(need_restart);

    int count = 0;
    do{
	if(leaf->type == lnode_t<Key_t, Value_t>::BTREE_NODE){
	    if(!leaf->sibling_ptr)
		return;
	    leaf = static_cast<lnode_t<Key_t, Value_t>*>(leaf->sibling_ptr);
	    continue;
	}

	cur_vstart = leaf->get_version(need_restart);
	auto ret = convert(leaf, cur_vstart, threadEpocheInfo);
	if(!ret)
	    blink_printf("Something wrong!! -- converting leaf %llx failed\n", leaf);
    }while((leaf = static_cast<lnode_t<Key_t, Value_t>*>(leaf->sibling_ptr)));
}


template <typename Key_t, typename Value_t>
void btree_t<Key_t, Value_t>::print_leaf(){
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

template <typename Key_t, typename Value_t>
void btree_t<Key_t, Value_t>::print_internal(){
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

template <typename Key_t, typename Value_t>
void btree_t<Key_t, Value_t>::print(){
    print_internal();
    print_leaf();
}

template <typename Key_t, typename Value_t>
void btree_t<Key_t, Value_t>::sanity_check(){
    auto cur = root;
    while(cur->level != 0){
	auto p = static_cast<inode_t<Key_t>*>(cur);
	p->sanity_check(p->high_key, true);
	cur = cur->leftmost_ptr;
    }

    auto l = static_cast<lnode_t<Key_t, Value_t>*>(cur);
    l->sanity_check(l->high_key, true);
}


template <typename Key_t, typename Value_t>
Value_t btree_t<Key_t, Value_t>::find_anyway(Key_t key){
    auto cur = root;
    while(cur->level != 0){
	cur = cur->leftmost_ptr;
    }

    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
    lnode_t<Key_t, Value_t>* before;
    do{
	bool need_restart = false;
	auto ret = leaf->find(key, need_restart);
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

template <typename Key_t, typename Value_t>
double btree_t<Key_t, Value_t>::utilization(){
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

template <typename Key_t, typename Value_t>
void btree_t<Key_t, Value_t>::footprint(uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied){
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
        cur = (static_cast<inode_t<Key_t>*>(leftmost_node))->leftmost_ptr;
    }

    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
    do{
        meta += sizeof(lnode_t<Key_t, Value_t>);
        auto type = leaf->type;
        if(type == lnode_t<Key_t, Value_t>::BTREE_NODE){
            auto lnode = static_cast<lnode_btree_t<Key_t, Value_t>*>(leaf);
            auto cnt = lnode->get_cnt();
            auto invalid_num = lnode_btree_t<Key_t, Value_t>::cardinality - cnt;
            key_data_occupied += sizeof(entry_t<Key_t, Value_t>)*cnt;
            key_data_unoccupied += sizeof(entry_t<Key_t, Value_t>)*invalid_num;
            leaf = static_cast<lnode_t<Key_t, Value_t>*>(lnode->sibling_ptr);
        }
        else{
            auto lnode = static_cast<lnode_hash_t<Key_t, Value_t>*>(leaf);
            lnode->footprint(meta, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
            leaf = static_cast<lnode_t<Key_t, Value_t>*>(lnode->sibling_ptr);
        }
    }while(leaf);
}


template <typename Key_t, typename Value_t>
inline int btree_t<Key_t, Value_t>::height(){
    auto cur = root;
    return cur->level;

}

template <typename Key_t, typename Value_t>
inline ThreadInfo btree_t<Key_t, Value_t>::getThreadInfo(){
    return ThreadInfo(this->epoche);
}

template class btree_t<StringKey, value64_t>;
}
