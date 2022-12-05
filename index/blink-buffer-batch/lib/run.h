#ifndef BLINK_BUFFER_BATCH_RUN_H__
#define BLINK_BUFFER_BATCH_RUN_H__

#include "table.h"
#include "tree.h"

namespace BLINK_BUFFER_BATCH{

#define TABLE_NUM 2

thread_local uint64_t out_of_order = 0;

template <typename Key_t, typename Value_t>
class run_t{
    public:
	run_t(): tab_idx(0){
	    table = new table_t<Key_t, Value_t>*[TABLE_NUM];
	    for(int i=0; i<TABLE_NUM; i++)
		table[i] = new table_t<Key_t, Value_t>();
	    tree = new btree_t<Key_t, Value_t>();
	}

	void insert(Key_t key, Value_t value){
	restart:
	    bool need_restart = false;
	    auto _tree_high_key = tree_high_key.load();
	    #ifdef FLUSH
	    if(key < _tree_high_key){ // out of order key flushes a buffer
		auto idx = tab_idx.load();
		int sub_idx = 0;
		if(idx == 0)
		    sub_idx = 1;

		auto tab = table[idx];
		auto version = tab->get_version(need_restart);
		if(need_restart)
		    goto restart;

		auto cnt = table[idx]->cnt.load();
		auto sub_tab = table[sub_idx];
		auto sub_version = sub_tab->get_version(need_restart);
		if(need_restart)
		    goto restart;

		/*
		tab->try_upgrade_flushlock(version, need_restart);
		if(need_restart)
		    goto restart;

		if(cnt){
		    tab->lock_buckets();
		    update_tab_idx(idx);
		    int num = 0;
		    auto leaf = tab->convert(num);
		    tree->insert_batch(leaf, num);
		    auto new_tree_high_key = leaf[num-1]->get_high_key();
		    update_tree_high_key(new_tree_high_key);
		    tab->clear();
		    delete[] leaf;
		}
		else
		    tab->write_unlock();
		    */
		
		if(cnt){
		    tab->try_upgrade_writelock(version, need_restart);
		    if(need_restart)
			goto restart;

		    update_tab_idx(idx);

		    int num = 0;
		    auto leaf = tab->convert(num);
		    tree->insert_batch(leaf, num);
		    auto new_tree_high_key = leaf[num-1]->get_high_key();
		    update_tree_high_key(new_tree_high_key);
		    tab->clear();
		    delete[] leaf;
		}
		tree->insert(key, value);
		out_of_order++;
		return;
	    }
	    #else
	    if(key < _tree_high_key){ // out of order key goes into tree
		tree->insert(key, value);
		out_of_order++;
		return;
	    }
	    #endif

	    // key goes into table
	    auto idx = tab_idx.load();
	    int sub_idx = 0;
	    if(idx == 0)
		sub_idx = 1;

	    auto tab = table[idx];
	    auto version = tab->get_version(need_restart);
	    if(need_restart)
		goto restart;

	    auto sub_tab = table[sub_idx];
	    auto sub_version = sub_tab->get_version(need_restart);
	    if(need_restart)
		goto restart;

	    auto ret = tab->insert(key, value, version);
	    if(ret == -1)
		goto restart; // conflict
	    else if(ret == 0){
		return; // inserted
	    }
	    // table is full, flush it into tree

	    tab->try_upgrade_writelock(version, need_restart);
	    if(need_restart)
		goto restart;

	    update_tab_idx(idx);

	    int num = 0;
	    auto leaf = tab->convert(num);
	    tree->insert_batch(leaf, num);

	    auto new_tree_high_key = leaf[num-1]->get_high_key();
	    update_tree_high_key(new_tree_high_key);
	    tab->clear();
	    delete[] leaf;
	    goto restart;
	}

	Value_t find(Key_t key){
	    Value_t value;
	restart:
	    bool need_restart = false;
	    auto _tree_high_key = tree_high_key.load();
	    if(key <= _tree_high_key){ // out of order key is in tree
		value = tree->lookup(key);
		return value;
	    }

	    // find table
	    for(int i=0; i<TABLE_NUM; i++){
		auto ret = table[i]->find(key, value, need_restart);
		if(need_restart)
		    goto restart;

		if(ret)
		    return value;
	    }

	    return value;
	}

	void print(){
	    tree->print_internal();
	    tree->print_leaf();
	}

	void check_locks(){
	    tree->check_locks();
	}

	int range_lookup(Key_t min_key, int range, Value_t* buf){
	restart:
	    bool need_restart = false;
	    auto _tree_high_key = tree_high_key.load();
	    int ret = 0;
	    if(min_key < _tree_high_key){
		ret = tree->range_lookup(min_key, range, buf);
		if(ret == range)
		    return ret;
	    }
		
	    #ifdef FLUSH
	    auto idx = tab_idx.load();
	    auto tab = table[idx];
	    auto version = tab->get_version(need_restart);
	    if(need_restart)
		goto restart;

	    auto cnt = tab->cnt.load();
	    if(cnt){
		tab->try_upgrade_writelock(version, need_restart);
		if(need_restart)
		    goto restart;

		update_tab_idx(idx);

		int num = 0;
		auto leaf = tab->convert(num);
		if(num){
		    tree->insert_batch(leaf, num);
		    auto new_tree_high_key = leaf[num-1]->get_high_key();
		    update_tree_high_key(new_tree_high_key);
		}
		tab->clear();
		delete[] leaf;
	    }
	    #else
	    auto idx = tab_idx.load();
	    auto tab = table[idx];
	    auto version = tab->get_version(need_restart);
	    if(need_restart)
		goto restart;

	    tab->try_upgrade_writelock(version, need_restart);
	    if(need_restart)
		goto restart;

	    update_tab_idx(idx);

	    int num = 0;
	    auto leaf = tab->convert(num);
	    if(num){
		tree->insert_batch(leaf, num);
		auto new_tree_high_key = leaf[num-1]->get_high_key();
		update_tree_high_key(new_tree_high_key);
	    }
	    tab->clear();
	    delete[] leaf;
	    #endif
	    ret = tree->range_lookup(min_key, range, buf);
	    return ret;
	}

	uint64_t get_outoforder(){
	    return out_of_order;
	}

    private:
	void update_tab_idx(int idx){
	    auto _idx = idx;
	    while(true){
		if(_idx == 0){
		    if(tab_idx.compare_exchange_strong(_idx, 1))
			break;
		}
		else{
		    if(tab_idx.compare_exchange_strong(_idx, 0))
			break;
		}
		_idx = tab_idx.load();
	    }
	}

	void update_tree_high_key(Key_t& _tree_high_key){
	    auto new_tree_high_key = _tree_high_key;
	    while(!tree_high_key.compare_exchange_strong(_tree_high_key, new_tree_high_key)){
		_tree_high_key = tree_high_key.load();
		if(new_tree_high_key < _tree_high_key) // other thread updated tree high key
		    return;
	    }
	}

	btree_t<Key_t, Value_t>* tree;
	table_t<Key_t, Value_t>** table;
	std::atomic<int> tab_idx;
	std::atomic<Key_t> tree_high_key;
};
}
#endif
