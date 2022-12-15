#ifndef BLINK_BUFFER_BATCH_RUN_H__
#define BLINK_BUFFER_BATCH_RUN_H__

#include "table.h"
#include "tree.h"

namespace BLINK_BUFFER_BATCH{

#define BATCH_TABLE_NUM 2

thread_local uint64_t out_of_order = 0;

template <typename Key_t, typename Value_t>
class run_t{
    public:
	run_t() {
	    version.store(0);
	    tab_idx.store(0);
	    table = new table_t<Key_t, Value_t>*[BATCH_TABLE_NUM];
	    for(int i=0; i<BATCH_TABLE_NUM; i++)
		table[i] = new table_t<Key_t, Value_t>();
	    tree = new btree_t<Key_t, Value_t>();
	    tree_high_key.store(0);
	}

	void insert(Key_t key, Value_t value){
	restart:
	    bool flush_restart = false;
	    bool table_restart = false;
	    auto _tree_high_key = tree_high_key.load();
	    #ifdef FLUSH
	    if(key < _tree_high_key){ // out of order key flushes a buffer
		auto flush_version = get_version(flush_restart);
		if(!flush_restart){
		    auto idx = get_tab_idx(flush_version);
		    auto tab = table[idx];
		    auto tab_version = tab->get_version(table_restart);
		    if(table_restart)
			goto restart;

		    auto cnt = tab->cnt.load();
		    if(cnt){ // table has some entries to flush
			try_upgrade_flushlock(flush_version, flush_restart);
			if(flush_restart)
			    goto restart;

			tab->try_upgrade_writelock(tab_version, table_restart);
			if(table_restart){
			    flushunlock();
			    goto restart;
			}

			flush(tab, _tree_high_key);
		    }
		}
		// else no need to flush them down
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
	    auto flush_version = get_version(flush_restart);
	    if(flush_restart){ // flush is happening
		auto flush_idx = get_flush_idx(flush_version);
		auto sub_idx = get_sub_idx(flush_idx);
		auto flush_tab = table[flush_idx];
		auto tab = table[sub_idx];
		auto flush_tab_version = flush_tab->get_version(table_restart);

		auto ret = insert_in_sub_table(key, value, flush_tab, flush_tab_version, tab, flush_version);
		if(ret)
		    return;
		goto restart;
	    }
	    // else, flush is not happening

	    auto idx = get_tab_idx(flush_version);
	    auto sub_idx = get_sub_idx(idx);
	    auto tab = table[idx];
	    auto sub_tab = table[sub_idx];
	    auto tab_version = tab->get_version(table_restart);
	    auto _flush_version = get_version(flush_restart);
	    if(flush_restart || (flush_version != _flush_version)) {
		auto ret = insert_in_sub_table(key, value, tab, tab_version, sub_tab, _flush_version);
		if(ret)
		    return;
		goto restart;
	    }

	    auto ret = tab->insert(key, value, tab_version);
	    if(ret == 0) // inserted
		return;
	    else if(ret == -1) // simple conflict
		goto restart;
	    // else need to flush

	    try_upgrade_flushlock(flush_version, flush_restart);
	    if(flush_restart) // failed to acquire a flushlock
		goto restart;

	    // table is full, acquire the tablelock
	    tab->try_upgrade_writelock(tab_version, table_restart);
	    if(table_restart)
		goto restart;

	    flush(tab, _tree_high_key); // flush it into the tree
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
	    for(int i=0; i<BATCH_TABLE_NUM; i++){
		auto ret = table[i]->find(key, value, need_restart);
		if(need_restart)
		    goto restart;

		if(ret)
		    return value;
	    }

	    return value;
	}

	void sanity_check(){
	    tree->sanity_check();
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

	    auto flush_version = get_version(need_restart);
	    if(need_restart)
		goto restart;

	    auto idx = get_tab_idx(flush_version);
	    auto tab = table[idx];
	    auto tab_version = tab->get_version(need_restart);

	    auto _flush_version = get_version(need_restart);
	    if(need_restart || (flush_version != _flush_version))
		goto restart;

	    try_upgrade_flushlock(flush_version, need_restart);
	    if(need_restart)
		goto restart;

	    tab->try_upgrade_writelock(tab_version, need_restart);
	    if(need_restart)
		goto restart;

	    flush(tab, _tree_high_key);
	    ret = tree->range_lookup(min_key, range, buf);
	    return ret;
	}

	uint64_t get_outoforder(){
	    return out_of_order;
	}

    private:
	bool insert_in_sub_table(Key_t key, Value_t value, table_t<Key_t, Value_t>* tab, uint64_t tab_version, table_t<Key_t, Value_t>* sub_tab, uint64_t flush_version){
	    bool need_restart = false;

	    auto sub_tab_version = sub_tab->get_version(need_restart);
	    if(need_restart)
		return false;

	    auto _flush_version = get_version(need_restart);
	    if(flush_version != _flush_version)
		return false;

	    need_restart = false;
	    auto tab_high_key = tab->get_high_key(need_restart, tab_version);
	    if(need_restart)
		return false;

	    if(tab_high_key < key){
		auto ret = sub_tab->insert(key, value, sub_tab_version);
		if(ret == 0)
		    return true;
	    }
	    return false;
	}

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


	void update_tree_high_key(Key_t _tree_high_key, Key_t new_tree_high_key){
	    while(!tree_high_key.compare_exchange_strong(_tree_high_key, new_tree_high_key)){
		_mm_pause();
		_tree_high_key = tree_high_key.load();
		if(new_tree_high_key < _tree_high_key) // other thread updated tree high key
		    return;
	    }
	}

	bool is_locked(uint64_t _version){
	    if((_version & 0b10) == 0b10)
		return true;
	    return false;
	}

	void flushunlock(){
	    version.fetch_add(0b10);
	}

	void try_upgrade_flushlock(uint64_t _version, bool& need_restart){
	    uint64_t new_version = _version + 0b10;
	    if((_version & 0b1) == 0) // previous flush buffer idx was 0
		new_version += 0b1;
	    else
		new_version -= 0b1;
	    if(!version.compare_exchange_strong(_version, new_version)){
		_mm_pause();
		need_restart = true;
	    }
	}

	uint64_t get_version(bool& need_restart){
	    auto v = version.load();
	    if(is_locked(v)){
		_mm_pause();
		need_restart = true;
	    }
	    return v;
	}

	int get_sub_idx(int idx){
	    if(idx == 0)
		return 1;
	    else
		return 0;
	}

	int get_tab_idx(uint64_t _version){
	    if((_version & 0b1) == 0)
		return 1;
	    else
		return 0;
	}

	int get_flush_idx(uint64_t _version){
	    if((_version & 0b1) == 0)
		return 0;
	    else
		return 1;
	}

	void flush(table_t<Key_t, Value_t>* tab, Key_t _tree_high_key){
	    int num = 0;
	    auto leaf = tab->convert(num);

	    tree->insert_batch(leaf, num);

	    auto new_tree_high_key = leaf[num-1]->get_high_key(); // after completing append, update the tree high key
	    update_tree_high_key(_tree_high_key, new_tree_high_key);

	    tab->clear(); // clear the flushed table
	    flushunlock(); // release flushlock
	    delete[] leaf;
	}

	btree_t<Key_t, Value_t>* tree;
	table_t<Key_t, Value_t>** table;
	std::atomic<uint64_t> version;
	std::atomic<int> tab_idx;
	std::atomic<Key_t> tree_high_key;
};
}
#endif
