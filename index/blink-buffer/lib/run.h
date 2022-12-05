#ifndef BLINK_BUFFER_RUN_H__
#define BLINK_BUFERF_RUN_H__

#include "table.h"
#include "tree.h"

namespace BLINK_BUFFER{

#define TABLE_NUM 16

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
		    flush(tab, _tree_high_key);
		    update_tree_high_key(_tree_high_key);

		    tab->clear();
		}

		tree->insert(key, value);
		return;
	    }
	    #else
	    if(key < _tree_high_key){ // out of order key goes into tree
		tree->insert(key, value);
		return;
	    }
	    #endif

	    // else, monotonic key goes into buffer
	    auto idx = tab_idx.load();
	    auto tab = table[idx];
	    auto version = tab->get_version(need_restart);
	    if(need_restart)
		goto restart;

	    auto ret = tab->insert(key, value, version);
	    if(ret == -1)
		goto restart; // conflict
	    else if(ret == 0)
		return; // inserted
	    // else, table is full --> flush the buffer

	    tab->try_upgrade_writelock(version, need_restart);
	    if(need_restart)
		goto restart;

	    update_tab_idx(idx);
	    flush(tab, _tree_high_key);
	    update_tree_high_key(_tree_high_key);

	    tab->clear();
	    goto restart;
	}

	Value_t find(Key_t key){
	    Value_t value;
	    bool need_restart = false;
	    for(int i=0; i<TABLE_NUM; i++){
	restart_tab:
		auto ret = table[i]->find(key, value, need_restart);
		if(need_restart){
		    need_restart = false;
		    goto restart_tab;
		}

		if(ret)
		    return value;
	    }

	    value = tree->lookup(key);
	    return value;
	}

	int range_lookup(Key_t min_key, int range, Value_t* buf){
	restart:
	    bool need_restart = false;
	    auto _tree_high_key = tree_high_key.load();
	    int num = 0;
	    if(min_key < _tree_high_key){
		num = tree->range_lookup(min_key, range, buf);
		if(num == range)
		    return num;
	    }

	    #ifdef FLUSH
	    for(int i=0; i<TABLE_NUM; i++){
		auto idx = tab_idx.load();
		auto tab = table[idx];
		auto version = tab->get_version(need_restart);
		if(need_restart){ // another thread is flushing the buffer
		    need_restart = false;
		    continue;
		}

		auto cnt = tab->cnt.load();
		if(cnt){
		    tab->try_upgrade_writelock(version, need_restart);
		    if(need_restart){ // another thread is flushing the buffer
			need_restart = false;
			continue;
		    }

		    update_tab_idx(idx);
		    flush(tab, _tree_high_key);
		    update_tree_high_key(_tree_high_key);

		    tab->clear();
		}
	    }
	    #else
	    for(int i=0; i<TABLE_NUM; i++){
		auto idx = tab_idx.load();
		auto tab = table[idx];
		auto version = tab->get_version(need_restart);
		if(need_restart){ // another thread is flushing the buffer
		    need_restart = false;
		    continue;
		}

		tab->try_upgrade_writelock(version, need_restart);
		if(need_restart){ // another thread is flushing the buffer
		    need_restart = false;
		    continue;
		}

		update_tab_idx(idx);
		flush(tab, _tree_high_key);
		update_tree_high_key(_tree_high_key);

		tab->clear();
	    }
	    #endif
	    num = tree->range_lookup(min_key, range, buf);
	    return num;
	}


	void print(){
	    tree->print_internal();
	    tree->print_leaf();
	}

	void check_locks(){
	    tree->check_locks();
	}

    private:
	void update_tab_idx(int idx){
	    auto _idx = idx;
	    while(true){
		if(_idx < TABLE_NUM-1){
		    if(tab_idx.compare_exchange_strong(_idx, _idx+1))
			return;
		}
		else{
		    if(tab_idx.compare_exchange_strong(_idx, 0))
			return;
		}
		_idx = tab_idx.load();
	    }
	}

	void flush(table_t<Key_t, Value_t>* tab, Key_t& _tree_high_key){
	    for(int i=0; i<table_t<Key_t, Value_t>::cardinality; i++){
                auto bucket = tab->get(i);
                for(int j=0; j<entry_num; j++){
                    if(!bucket->is_empty(j)){
                        auto entry = bucket->get(j);
                        tree->insert(entry->key, entry->value);
                        if(_tree_high_key < entry->key)
                            _tree_high_key = entry->key;
                    }
                }
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
