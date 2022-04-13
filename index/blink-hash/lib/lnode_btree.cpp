#include "lnode.h"

namespace BLINK_HASH{

template <typename Key_t, typename Value_t>
inline void lnode_btree_t<Key_t, Value_t>::writelock(){
    (static_cast<node_t*>(this))->writelock();
}

template <typename Key_t, typename Value_t>
inline bool lnode_btree_t<Key_t, Value_t>::try_writelock(){
    return (static_cast<node_t*>(this))->try_writelock();
}

template <typename Key_t, typename Value_t>
inline void lnode_btree_t<Key_t, Value_t>::write_unlock(){
    (static_cast<node_t*>(this))->write_unlock();
}

template <typename Key_t, typename Value_t>
inline bool lnode_btree_t<Key_t, Value_t>::is_full(){
    return (this->cnt == cardinality);
}

template <typename Key_t, typename Value_t>
int lnode_btree_t<Key_t, Value_t>::find_lowerbound(Key_t key){
    if constexpr(LEAF_BTREE_SIZE < 2048)
	return lowerbound_linear(key);
    else
	return lowerbound_binary(key);
}

template <typename Key_t, typename Value_t>
Value_t lnode_btree_t<Key_t, Value_t>::find(Key_t key){
    if constexpr(LEAF_BTREE_SIZE < 2048)
	return find_linear(key);
    else
	return find_binary(key);
}

template <typename Key_t, typename Value_t>
int lnode_btree_t<Key_t, Value_t>::insert(Key_t key, Value_t value, uint64_t version){
    bool need_restart = false;
    this->try_upgrade_writelock(version, need_restart);
    if(need_restart){
	return -1;
    }

    if(this->cnt < cardinality){
	if(this->cnt){
	    int pos = find_lowerbound(key);
	    memmove(&entry[pos+1], &entry[pos], sizeof(entry_t<Key_t, Value_t>)*(this->cnt-pos));
	    entry[pos].key = key;
	    entry[pos].value = value;
	}
	else{
	    entry[0].key = key;
	    entry[0].value= value;
	}
	this->cnt++;
	write_unlock();
	return 0;
    }
    else{ // need split
	return 1;
    }
}

template <typename Key_t, typename Value_t>
void lnode_btree_t<Key_t, Value_t>::insert_after_split(Key_t key, Value_t value){
    int pos = find_lowerbound(key);
    memmove(&entry[pos+1], &entry[pos], sizeof(entry_t<Key_t, Value_t>)*(this->cnt - pos));
    entry[pos].key = key;
    entry[pos].value = value;
    this->cnt++;
}

template <typename Key_t, typename Value_t>
lnode_btree_t<Key_t, Value_t>* lnode_btree_t<Key_t, Value_t>::split(Key_t& split_key, Key_t key, Value_t value){
    int half = this->cnt/2;
    int new_cnt = this->cnt - half;
    split_key = entry[half-1].key;

    auto new_leaf = new lnode_btree_t<Key_t, Value_t>(this->sibling_ptr, new_cnt, this->level);
    new_leaf->high_key = this->high_key;
    memcpy(new_leaf->entry, entry+half, sizeof(entry_t<Key_t, Value_t>)*new_cnt);

    this->sibling_ptr = static_cast<node_t*>(new_leaf);
    this->high_key = split_key;
    this->cnt = half;

    if(split_key < key)
	new_leaf->insert_after_split(key, value);
    else
	insert_after_split(key, value);

    return new_leaf;
}

template <typename Key_t, typename Value_t>
void lnode_btree_t<Key_t, Value_t>::batch_insert(entry_t<Key_t, Value_t>* buf, size_t batch_size, int& from, int to){
    if(from + batch_size < to){
	memcpy(entry, &buf[from], sizeof(entry_t<Key_t, Value_t>)*batch_size);
	from += batch_size;
	this->cnt += batch_size;
	this->high_key = entry[this->cnt-1].key;
    }
    else{
	memcpy(entry, &buf[from], sizeof(entry_t<Key_t, Value_t>)*(to - from));
	this->cnt += (to - from);
	from = to;
	this->high_key = entry[this->cnt-1].key;
    }
}


template <typename Key_t, typename Value_t>
int lnode_btree_t<Key_t, Value_t>::remove(Key_t key, uint64_t version){
    bool need_restart = false;
    this->try_upgrade_writelock(version, need_restart);
    if(need_restart) return -1;

    if(this->cnt){
	int pos = find_pos_linear(key);
	// no matching key found
	if(pos == -1) return false;
	memmove(&entry[pos], &entry[pos+1], sizeof(entry_t<Key_t, Value_t>)*(lnode_t<Key_t, Value_t>::cnt - pos - 1));
	this->cnt--;
	write_unlock();
	return 0;
    }
    write_unlock();
    return -2;
}

template <typename Key_t, typename Value_t>
int lnode_btree_t<Key_t, Value_t>::update(Key_t key, Value_t value, uint64_t version){
    bool need_restart = false;
    this->try_upgrade_writelock(version, need_restart);
    if(need_restart)
	return -1;

    if(update_linear(key, value)){
	write_unlock();
	return 0;
    }

    write_unlock();
    return -2;
}

template <typename Key_t, typename Value_t>
int lnode_btree_t<Key_t, Value_t>::range_lookup(Key_t key, Value_t* buf, int count, int range, bool continued){
    auto _count = count;
    if(continued){
	for(int i=0; i<this->cnt; i++){
	    buf[_count++] = entry[i].value;
	    if(_count == range) return _count;
	}
	return _count;
    }
    else{
	int pos = find_lowerbound(key);
	for(int i=pos+1; i<this->cnt; i++){
	    buf[_count++] = entry[i].value;
	    if(_count == range) return _count;
	}
	return _count;
    }
}

template <typename Key_t, typename Value_t>
void lnode_btree_t<Key_t, Value_t>::print(){
    for(int i=0; i<this->cnt; i++){
	std::cout << "[" << i << "]" << entry[i].key << " ";
    }
    std::cout << "  high_key: " << this->high_key << "\n\n";
}

template <typename Key_t, typename Value_t>
void lnode_btree_t<Key_t, Value_t>::sanity_check(Key_t _high_key, bool first){
    for(int i=0; i<this->cnt-1; i++){
	for(int j=i+1; j<this->cnt; j++){
	    if(entry[i].key > entry[j].key){
		std::cerr << "lnode_t::key order is not perserved!!" << std::endl;
		std::cout << "[" << i << "].key: " << entry[i].key << "\t[" << j << "].key: " << entry[j].key << std::endl;
	    }
	}
    }
    for(int i=0; i<this->cnt; i++){
	if(this->sibling_ptr && (entry[i].key > this->high_key)){
	    std::cout << i << "lnode_t:: " << "(" << entry[i].key << ") is higher than high key " << this->high_key << std::endl;
	}
	if(!first){
	    if(this->sibling_ptr && (entry[i].key < _high_key)){
		std::cout << "lnode_t:: " << i << "(" << entry[i].key << ") is smaller than previous high key " << _high_key << std::endl;
		std::cout << "--------- node_address " << this << " , current high_key " << this->high_key << std::endl;
	    }
	}
    }
    if(this->sibling_ptr != nullptr)
	(static_cast<lnode_t<Key_t, Value_t>*>(this->sibling_ptr))->sanity_check(this->high_key, false);
}

template <typename Key_t, typename Value_t>
int lnode_btree_t<Key_t, Value_t>::get_cnt(){
    return this->cnt;
}

template <typename Key_t, typename Value_t>
double lnode_btree_t<Key_t, Value_t>::utilization(){
    return (double)this->cnt / cardinality;
}

template <typename Key_t, typename Value_t>
int lnode_btree_t<Key_t, Value_t>::lowerbound_linear(Key_t key){
    for(int i=0; i<this->cnt; i++){
	if(key <= entry[i].key)
	    return i;
    }
    return this->cnt;
}

template <typename Key_t, typename Value_t>
int lnode_btree_t<Key_t, Value_t>::lowerbound_binary(Key_t key){
    int lower = 0;
    int upper = this->cnt;
    do{
	int mid = ((upper - lower) / 2) + lower;
	if(key < entry[mid].key)
	    upper = mid;
	else if(key > entry[mid].key)
	    lower = mid + 1;
	else
	    return mid;
    }while(lower < upper);
    return lower;
}


template <typename Key_t, typename Value_t>
bool lnode_btree_t<Key_t, Value_t>::update_linear(Key_t key, uint64_t value){
    for(int i=0; i<this->cnt; i++){
	if(key == entry[i].key){
	    #ifdef UPDATE_LOCK
	    auto _value = entry[i].value;
	    while(!CAS(&entry[i].value, &_value, value)){
		_mm_pause();
		_value = entry[i].value;
	    }
	    #else
	    entry[i].value = value;
	    #endif
	    return true;
	}
    }
    return false;
}

template <typename Key_t, typename Value_t>
Value_t lnode_btree_t<Key_t, Value_t>::find_linear(Key_t key){
    for(int i=0; i<this->cnt; i++){
	if(key == entry[i].key){
	    auto ret = entry[i].value;
	    return ret;
	}
    }
    return 0;
}

template <typename Key_t, typename Value_t>
Value_t lnode_btree_t<Key_t, Value_t>::find_binary(Key_t key){
    int lower = 0;
    int upper = this->cnt;
    do{
	int mid = ((upper - lower) / 2) + lower;
	if(key < entry[mid].key)
	    upper = mid;
	else if(key > entry[mid].key)
	    lower = mid+1;
	else
	    return entry[mid].value;
    }while(lower < upper);
    return 0;
}

template <typename Key_t, typename Value_t>
int lnode_btree_t<Key_t, Value_t>::find_pos_linear(Key_t key){
    for(int i=0; i<this->cnt; i++){
	if(key == entry[i].key){
	    return i;
	}
    }
    return -1;
}

template <typename Key_t, typename Value_t>
int lnode_btree_t<Key_t, Value_t>::find_pos_binary(Key_t key){
    int lower = 0;
    int upper = this->cnt;
    do{
	int mid = ((upper - lower) / 2) + lower;
	if(key < entry[mid].key)
	    upper = mid;
	else if(key > entry[mid].key)
	    lower = mid+1;
	else
	    return mid;
    }while(lower < upper);
    return lower;
}


template class lnode_btree_t<key64_t, value64_t>;
}
