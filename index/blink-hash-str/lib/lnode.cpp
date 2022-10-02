#include "lnode.h"
#include "lnode_btree.cpp"
#include "lnode_hash.cpp"

namespace BLINK_HASH{

template <typename Key_t, typename Value_t>
inline void lnode_t<Key_t, Value_t>::write_unlock(){
    switch(type){
	case BTREE_NODE:
	    (static_cast<node_t*>(this))->write_unlock();
	    return;
	case HASH_NODE:
	    (static_cast<lnode_hash_t<Key_t, Value_t>*>(this))->split_unlock();
	    return;
	default:
	    std::cerr << __func__ << ": node type error: " << type << std::endl;
	    return;
    }
    std::cerr << __func__ << ": should not reach here" << std::endl;
    return;
}

template <typename Key_t, typename Value_t>
inline void lnode_t<Key_t, Value_t>::convert_unlock(){
    switch(type){
	case BTREE_NODE:
	    (static_cast<node_t*>(this))->write_unlock();
	    return;
	case HASH_NODE:
	    (static_cast<lnode_hash_t<Key_t, Value_t>*>(this))->convert_unlock();
	    return;
	default:
	    std::cerr << __func__ << ": node type error: " << type << std::endl;
	    return;
    }
    std::cerr << __func__ << ": should not reach here" << std::endl;
    return;
}

template <typename Key_t, typename Value_t>
inline void lnode_t<Key_t, Value_t>::write_unlock_obsolete(){
    switch(type){
	case BTREE_NODE:
	    (static_cast<node_t*>(this))->write_unlock_obsolete();
	    return;
	case HASH_NODE:
	    (static_cast<lnode_hash_t<Key_t, Value_t>*>(this))->split_unlock_obsolete();
	    return;
	default:
	    std::cerr << __func__ << ": node type error: " << type << std::endl;
	    return;
    }
    std::cerr << __func__ << ": should not reach here" << std::endl;
    return;
}

template <typename Key_t, typename Value_t>
inline void lnode_t<Key_t, Value_t>::convert_unlock_obsolete(){
    switch(type){
	case BTREE_NODE:
	    (static_cast<node_t*>(this))->write_unlock_obsolete();
	    return;
	case HASH_NODE:
	    (static_cast<lnode_hash_t<Key_t, Value_t>*>(this))->convert_unlock_obsolete();
	    return;
	default:
	    std::cerr << __func__ << ": node type error: " << type << std::endl;
	    return;
    }
    std::cerr << __func__ << ": should not reach here" << std::endl;
    return;
}

template <typename Key_t, typename Value_t>
int lnode_t<Key_t, Value_t>::insert(Key_t key, Value_t value, uint64_t version){
    switch(type){
	case BTREE_NODE:
	    return (static_cast<lnode_btree_t<Key_t, Value_t>*>(this))->insert(key, value, version);
	case HASH_NODE:
	    return (static_cast<lnode_hash_t<Key_t, Value_t>*>(this))->insert(key, value, version);
	default:
	    std::cerr << __func__ << ": node type error: " << type << std::endl;
	    return 0;
    }
    std::cerr << __func__ << ": should not reach here" << std::endl;
    return 0;
}

template <typename Key_t, typename Value_t>
node_t* lnode_t<Key_t, Value_t>::split(Key_t& split_key, Key_t key, Value_t value, uint64_t version){
    switch(type){
	case BTREE_NODE:
	    return (static_cast<lnode_btree_t<Key_t, Value_t>*>(this))->split(split_key, key, value);
	case HASH_NODE:
	    return (static_cast<lnode_hash_t<Key_t, Value_t>*>(this))->split(split_key, key, value, version);
	default:
	    std::cerr << __func__ << ": node type error: " << type << std::endl;
	    return nullptr;
    }
    std::cerr << __func__ << ": should not reach here" << std::endl;
    return nullptr;
}

template <typename Key_t, typename Value_t>
int lnode_t<Key_t, Value_t>::update(Key_t key, Value_t value, uint64_t version){
    switch(type){
	case BTREE_NODE:
	    return (static_cast<lnode_btree_t<Key_t, Value_t>*>(this))->update(key, value, version);
	case HASH_NODE:
	    return (static_cast<lnode_hash_t<Key_t, Value_t>*>(this))->update(key, value, version);
	default:
	    std::cerr << __func__ << ": node type error: " << type << std::endl;
	    return 0;
    }
    std::cerr << __func__ << ": should not reach here" << std::endl;
    return 0;
}

template <typename Key_t, typename Value_t>
int lnode_t<Key_t, Value_t>::remove(Key_t key, uint64_t version){
    switch(type){
	case BTREE_NODE:
	    return (static_cast<lnode_btree_t<Key_t, Value_t>*>(this))->remove(key, version);
	case HASH_NODE:
	    return (static_cast<lnode_hash_t<Key_t, Value_t>*>(this))->remove(key, version);
	default:
	    std::cerr << __func__ << ": node type error: " << type << std::endl;
	    return 0;
    }
    std::cerr << __func__ << ": should not reach here" << std::endl;
    return 0;
}

template <typename Key_t, typename Value_t>
Value_t lnode_t<Key_t, Value_t>::find(Key_t key, bool& need_restart){
    switch(type){
	case BTREE_NODE:
	    return (static_cast<lnode_btree_t<Key_t, Value_t>*>(this))->find(key);
	case HASH_NODE:
	    return (static_cast<lnode_hash_t<Key_t, Value_t>*>(this))->find(key, need_restart);
	default:
	    std::cerr << __func__ << ": node type error: " << type << std::endl;
	    return 0;
    }
    std::cerr << __func__ << ": should not reach here" << std::endl;
    return 0;
}

template <typename Key_t, typename Value_t>
int lnode_t<Key_t, Value_t>::range_lookup(Key_t key, Value_t* buf, int count, int range, bool continued){
    switch(type){
	case BTREE_NODE:
	    return (static_cast<lnode_btree_t<Key_t, Value_t>*>(this))->range_lookup(key, buf, count, range, continued);
	case HASH_NODE:
	    #ifdef ADAPTATION
	    if(sibling_ptr != nullptr) // convert flag
		return -2;
	    #endif
	    return (static_cast<lnode_hash_t<Key_t, Value_t>*>(this))->range_lookup(key, buf, count, range);
	default:
	    std::cerr << __func__ << ": node type error: " << type << std::endl;
	    return 0;
    }
    std::cerr << __func__ << ": should not reach here" << std::endl;
    return 0;
}

template <typename Key_t, typename Value_t>
void lnode_t<Key_t, Value_t>::print(){
    switch(type){
	case BTREE_NODE:
	    (static_cast<lnode_btree_t<Key_t, Value_t>*>(this))->print();
	    return;
	case HASH_NODE:
	    (static_cast<lnode_hash_t<Key_t, Value_t>*>(this))->print();
	    return;
	default:
	    std::cerr << __func__ << ": node type error: " << type << std::endl;
	    return;
    }
    std::cerr << __func__ << ": should not reach here" << std::endl;
}

template <typename Key_t, typename Value_t>
void lnode_t<Key_t, Value_t>::sanity_check(Key_t key, bool first){
    switch(type){
	case BTREE_NODE:
	    (static_cast<lnode_btree_t<Key_t, Value_t>*>(this))->sanity_check(key, first);
	    return;
	case HASH_NODE:
	    (static_cast<lnode_hash_t<Key_t, Value_t>*>(this))->sanity_check(key, first);
	    return;
	default:
	    std::cerr << __func__ << ": node type error: " << type << std::endl;
	    return;
    }
    std::cerr << __func__ << ": should not reach here" << std::endl;
}

template <typename Key_t, typename Value_t>
double lnode_t<Key_t, Value_t>::utilization(){
    switch(type){
	case BTREE_NODE:
	    return (static_cast<lnode_btree_t<Key_t, Value_t>*>(this))->utilization();
	case HASH_NODE:
	    return (static_cast<lnode_hash_t<Key_t, Value_t>*>(this))->utilization();
	default:
	    std::cerr << __func__ << ": node type error: " << type << std::endl;
	    return 0;
    }
    std::cerr << __func__ << ": should not reach here" << std::endl;
    return 0;
}

template class lnode_t<StringKey, value64_t>;
}
