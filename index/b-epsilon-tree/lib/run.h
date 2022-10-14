#ifndef BETREE_RUN_H__
#define BETREE_RUN_H__

#include "buffer.h"
#include "tree.h"

#define BUFFER_NUM (64)
namespace B_EPSILON_TREE{
template <typename Key_t, typename Value_t>
class run_t{
    public:
	run_t(){
	    for(int i=0; i<BUFFER_NUM; i++){
		buffer[i] = new buffer_t<Key_t, Value_t>();
	    }
	    tree = new betree_t<Key_t, Value_t>();
	}

	void insert(Key_t key, Value_t value, uint64_t tid){
	restart:
	    auto buf = get_buf(tid);
	    if(!buf->try_writelock()){
		std::cout << "Lock failed" << std::endl;
		goto restart;
	    }

	    if(!buf->is_full()){
		buf->put(key, value);
		buf->write_unlock();
		return;
	    }

	    auto entry = buf->get_entry();
	    auto cnt = buf->get_cnt();
	    tree->put(entry, cnt);
	    buf->reset();
	    buf->write_unlock();
	    goto restart;
	}

	Value_t lookup(Key_t key){
	    bool need_restart = false;
	    Value_t value;
	restart:
	    for(int i=0; i<BUFFER_NUM; i++){
		auto buf = get_buf((uint64_t)i);
		auto vstart = buf->get_version(need_restart);
		if(need_restart)
		    goto restart;

		auto ret = buf->find(key, value);
		auto vend = buf->get_version(need_restart);
		if(need_restart || (vstart != vend))
		    goto restart;

		if(ret)
		    return value;
	    }

	    auto ret = tree->lookup(key);
	    return ret;
	}

    private:
	buffer_t<Key_t, Value_t>* get_buf(uint64_t id){
	    return buffer[id];
	}

	buffer_t<Key_t, Value_t>* buffer[BUFFER_NUM];
	betree_t<Key_t, Value_t>* tree;
};

}
#endif
