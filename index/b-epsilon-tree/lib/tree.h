#ifndef BETREE_TREE_H__
#define BETREE_TREE_H__

#include "node.h"

namespace B_EPSILON_TREE{

template <typename Key_t, typename Value_t>
class betree_t{
    public:
	betree_t(){
	    root = static_cast<node_t*>(new lnode_t<Key_t, Value_t>());
	}
	~betree_t(){ }

	Value_t lookup(Key_t key){
	    Value_t value;
	restart:
	    auto cur = root;
            bool need_restart = false;
            auto cur_vstart = cur->get_version(need_restart);
            if(need_restart){
                goto restart;
            }

            // traversal
            while(cur->level != 0){
		auto found = (static_cast<inode_t<Key_t, Value_t>*>(cur))->scan_msg(key, value);
		if(found){
		    auto cur_vend = cur->get_version(need_restart);
		    if(need_restart || (cur_vstart != cur_vend))
			goto restart;
		    return value;
		}

                auto child = (static_cast<inode_t<Key_t, Value_t>*>(cur))->scan_pivot(key);
                auto child_vstart = child->get_version(need_restart);
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

	    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    auto leaf_vstart = cur_vstart;
	    value = leaf->find(key);
	    auto leaf_vend = cur->get_version(need_restart);
	    if(need_restart || (leaf_vstart != leaf_vend))
		goto restart;
	    return value;
	}

	int range_lookup(Key_t min_key, int range, Value_t* buf){
	restart:
	    std::map<Key_t, Value_t> map;
	    auto cur = root;
	    bool need_restart = false;
	    auto cur_vstart = cur->get_version(need_restart);
	    if(need_restart)
		goto restart;

	    while(cur->level != 0){
		auto inode = static_cast<inode_t<Key_t, Value_t>*>(cur);
		inode->scan_msg(min_key, map);
		auto child = inode->scan_pivot(min_key);
		auto child_vstart = child->get_version(need_restart);
		if(need_restart)
		    goto restart;

		auto cur_vend = inode->get_version(need_restart);
		if(need_restart || (cur_vstart != cur_vend))
		    goto restart;

		cur = child;
		cur_vstart = child_vstart;
	    }

	    auto leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);
	    auto leaf_vstart = cur_vstart;

	    while(leaf){
		leaf->scan_node(min_key, map);
		auto sibling = leaf->sibling_ptr;
		auto leaf_vend = leaf->get_version(need_restart);
		if(need_restart || (leaf_vstart != leaf_vend))
		    goto restart;

		if(!sibling || (map.size() >= range)){
		    int num = 0;
		    for(auto it=map.begin(); it!=map.end(); it++){
			buf[num++] = it->second;
			if(num == range)
			    break;
		    }
		    return num;
		}

		auto sibling_vstart = sibling->get_version(need_restart);
		if(need_restart)
		    goto restart;

		leaf = sibling;
		leaf_vstart = sibling_vstart;
	    }

	    int num = 0;
	    for(auto it=map.begin(); it!=map.end(); it++){
		buf[num++] = it->second;
		if(num == range)
		    break;
	    }
	    return num;
	}


	void update(Key_t key, Value_t value){
	    put(key, value, OP_UPDATE);
	}

	void insert(Key_t key, Value_t value){
	    put(key, value, OP_INSERT);
	}


	void put(Key_t key, Value_t value, opcode_t op){
	    int restart_cnt = 0;
	restart:
	    if(restart_cnt++ > RETRY_THRESHOLD)
		yield(restart_cnt);
	    auto cur = root;
	    bool need_restart = false;
	    auto cur_vstart = cur->get_version(need_restart);
	    if(need_restart) goto restart;

	    if(cur->level != 0){
		auto inode = static_cast<inode_t<Key_t, Value_t>*>(cur);
		inode->try_upgrade_writelock(cur_vstart, need_restart);
		if(need_restart){
		    goto restart;
		}
		if(cur != root){
		    inode->write_unlock();
		    goto restart;
		}

		if(inode->is_pivot_full()){
		    Key_t split_key;
		    auto new_node = inode->split(split_key);
		    if(key < split_key)
			inode->put_msg(key, value, op);
		    else
			new_node->put_msg(key, value, op);
		    auto new_root = new inode_t<Key_t, Value_t>(inode->level+1, static_cast<node_t*>(inode), static_cast<node_t*>(new_node), split_key);
		    root = static_cast<node_t*>(new_root);
		}
		else{
		    if(!inode->is_msg_full())
			inode->put_msg(key, value, op);
		    else{
			auto ret = flush(nullptr, inode);
			if(!ret){
			    inode->write_unlock();
			    goto restart;
			}
			inode->put_msg(key, value, op);
			//inode->write_unlock();
			//goto restart;
		    }
		}
		inode->write_unlock();
	    }
	    else{ // leaf
		auto lnode = static_cast<lnode_t<Key_t, Value_t>*>(cur);
		lnode->try_upgrade_writelock(cur_vstart, need_restart);
		if(need_restart){
		    goto restart;
		}

		if(cur != root){
		    lnode->write_unlock();
		    goto restart;
		}

		if(!lnode->is_full()){
		    lnode->put(key, value, op);
		}
		else{
		    Key_t split_key;
		    auto new_node = lnode->split(split_key);
		    if(key <= split_key)
			lnode->put(key, value, op);
		    else
			new_node->put(key, value, op);
		    auto new_root = new inode_t<Key_t, Value_t>(1, static_cast<node_t*>(lnode), static_cast<node_t*>(new_node), split_key);
		    root = static_cast<node_t*>(new_root);
		}
		lnode->write_unlock();
	    }
	}

	void print(node_t* cur){
	    if(cur->level == 0){
		(static_cast<lnode_t<Key_t, Value_t>*>(cur))->print();
	    }
	    else{
		auto node = static_cast<inode_t<Key_t, Value_t>*>(cur);
		node->print();
		auto cnt = node->get_pivot_cnt();
		for(int i=0; i<=cnt; i++){
		    auto child = node->get_child(i);
		    print(static_cast<node_t*>(child));
		}
	    }
	}



	void print(){
	    print(root);
	}


    private:
	bool flush(inode_t<Key_t, Value_t>* parent, inode_t<Key_t, Value_t>* cur){
	    auto pivot_cnt = cur->get_pivot_cnt();
	    child_info_t child_info[pivot_cnt+1];
	    memset(child_info, 0, sizeof(child_info_t)*(pivot_cnt+1));
	    //std::vector<std::pair<int, std::pair<int, int>>> child_info;
	    //child_info.resize(cur->get_pivot_cnt());
	    //cur->inspect(child_info);
	    int child_info_size = 0;
	    cur->inspect(child_info, child_info_size);

	    if(pivot_cnt > 3){
		int potential_child_split = 0;
		for(int i=0; i<child_info_size; i++){
		    if(child_info[i].ptr->level == 0){ // leaf
			auto child_cnt = (static_cast<lnode_t<Key_t, Value_t>*>(child_info[i].ptr))->get_cnt();
			auto future_child_cnt = child_cnt + (child_info[i].to - child_info[i].from);
			auto future_child_split = future_child_cnt / lnode_t<Key_t, Value_t>::cardinality;
			auto remainder = future_child_cnt % lnode_t<Key_t, Value_t>::cardinality;
			if(remainder)
			    future_child_split++;
			potential_child_split += future_child_split;
		    }
		    else{ // internal
			auto child_cnt = (static_cast<inode_t<Key_t, Value_t>*>(child_info[i].ptr))->get_msg_cnt();
			auto future_child_cnt = child_cnt + (child_info[i].to - child_info[i].from);
			auto future_child_split = future_child_cnt / inode_t<Key_t, Value_t>::msg_cardinality;
			auto remainder = future_child_cnt % inode_t<Key_t, Value_t>::msg_cardinality;
			if(remainder)
			    future_child_split++;
			potential_child_split += future_child_split;
		    }
		}


		if(cur->is_pivot_full(potential_child_split)){
		    Key_t split_key;
		    auto new_node = cur->split(split_key);
		    if(parent){
			parent->insert_pivot(split_key, static_cast<node_t*>(new_node));
		    }
		    else{
			auto new_root = new inode_t<Key_t, Value_t>(cur->level+1, static_cast<node_t*>(cur), static_cast<node_t*>(new_node), split_key);
			root = static_cast<node_t*>(new_root);
		    }
		    return false;
		}
	    }

	    int cleanup_idx = 0;
	    for(int i=0; i<child_info_size; i++){
	    //for(int i=0; i<child_info.size(); i++){
		auto child = child_info[i].ptr;
		auto from = child_info[i].from;
		auto to = child_info[i].to;
		//auto child = cur->get_child(child_info[i].first);
		//auto from = child_info[i].second.first;
		//auto to = child_info[i].second.second;
		if(!child->try_writelock()){
		    cleanup_idx = from;
		    goto cleanup;
		}

		if(child->level == 0){ // lnode
		    auto _child = static_cast<lnode_t<Key_t, Value_t>*>(child);
		    if(!_child->is_full(to - from)){
			for(int j=from; j<to; j++){
			    _child->put(cur->get_msg_key(j), cur->get_msg_value(j));
			}
		    }
		    else{ // leaf split
			Key_t split_key;
			auto new_node = _child->split(split_key);
			for(int j=from; j<to; j++){
			    auto key = cur->get_msg_key(j);
			    if(key <= split_key){
				_child->put(key, cur->get_msg_value(j));
			    }
			    else{
				new_node->put(key, cur->get_msg_value(j));
			    }
			}
			cur->insert_pivot(split_key, static_cast<node_t*>(new_node));
		    }
		    _child->write_unlock();
		}
		else{ // inode
		    auto _child = static_cast<inode_t<Key_t, Value_t>*>(child);
		    if(!_child->is_msg_full(to - from)){
			for(int j=from; j<to; j++){
			    _child->put(cur->get_msg_key(j), cur->get_msg_value(j));
			}
			_child->write_unlock();
		    }
		    else{ // flush
			auto ret = flush(cur, _child);
			if(ret){
			    for(int j=from; j<to; j++){
				_child->put(cur->get_msg_key(j), cur->get_msg_value(j));
			    }
			    _child->write_unlock();
			}
			else{
			    cleanup_idx = from;
			    _child->write_unlock();
			    goto cleanup;
			}
		    }
		}
	    }
	    if(child_info_size > 0){
		cur->move_msg(child_info[child_info_size-1].to);
		//cur->move_msg(child_info[child_info.size()-1].second.second);
	    }

	    return true;

	cleanup:
	    if(cleanup_idx > 0){
		cur->move_msg(cleanup_idx);
	    }
	    return false;
	}

	node_t* root;
};


}




		
#endif
