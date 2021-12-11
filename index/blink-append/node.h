#ifndef NODE_APPEND_H__
#define NODE_APPEND_H__

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <immintrin.h>
#include <atomic>
#include <iostream>
#include <cmath>
#include <limits.h>
#include <algorithm>

#include <x86intrin.h>
#define BITS_PER_LONG 64
#define BITOP_WORD(nr) ((nr) / BITS_PER_LONG)

#define PAGE_SIZE (512)

#define CACHELINE_SIZE 64

namespace BLINK_APPEND{

inline void mfence(void){
    asm volatile("mfence" ::: "memory");
}

class node_t{
    public:
	std::atomic<uint64_t> lock;
	node_t* sibling_ptr;
	node_t* leftmost_ptr;
	int cnt;
	uint32_t level;


	node_t(): lock(0b100), sibling_ptr(nullptr), leftmost_ptr(nullptr), cnt(0), level(0){ }
	node_t(node_t* sibling, node_t* left, uint32_t count, uint32_t _level): lock(0b100), sibling_ptr(sibling), leftmost_ptr(left), cnt(count), level(_level) { }

	bool is_locked(uint64_t version){
	    return ((version & 0b10) == 0b10);
	}

	bool is_obsolete(uint64_t version){
	    return (version & 1) == 1;
	}

	uint64_t get_version(bool& need_restart){
	    uint64_t version = lock.load();
	    if(is_locked(version) || is_obsolete(version)){
		_mm_pause();
		need_restart = true;
	    }
	    return version;
	}


	uint64_t try_readlock(bool& need_restart){
	    uint64_t version = lock.load();
	    if(is_locked(version) || is_obsolete(version)){
		_mm_pause();
		need_restart = true;
	    }
	    return version;
	}

	bool try_writelock(){ // exclusive lock
	    uint64_t version = lock.load();
	    if(is_locked(version) || is_obsolete(version)){
		_mm_pause();
		return false;
	    }

	    if(lock.compare_exchange_strong(version, version + 0b10)){
		version += 0b10;
		return true;
	    }
	    else{
		_mm_pause();
		return false;
	    }
	}

	void try_upgrade_writelock(uint64_t& version, bool& need_restart){
	    if(lock.compare_exchange_strong(version, version + 0b10)){
		version += 0b10;
	    }
	    else{
		_mm_pause();
		need_restart =true;
	    }
	}

	void write_unlock(){
	    lock.fetch_add(0b10);
	}

	void write_unlock_obsolete(){
	    lock.fetch_add(0b11);
	}

	void read_unlock(uint64_t before, bool& need_restart) const{
	    need_restart = (before != lock.load());
	}

};

template <typename Key_t, typename value_t>
struct entry_t{
    Key_t key;
    value_t value;
};

template <typename Key_t>
class inode_t: public node_t{
    
    public:
#ifdef STRING_KEY
	static constexpr size_t cardinality = (PAGE_SIZE - sizeof(node_t)- sizeof(Key_t)) / sizeof(entry_t<Key_t, node_t*>);
#else
	static constexpr size_t cardinality = (PAGE_SIZE - sizeof(node_t)- sizeof(Key_t) - 8) / sizeof(entry_t<Key_t, node_t*>);
#endif
	Key_t high_key;
    private:
#ifndef STRING_KEY
	char dummy[8];
#endif
        entry_t<Key_t, node_t*> entry[cardinality];
    public:

    	inode_t() { } 

	// constructor when inode needs to split
	inode_t(node_t* sibling, int _cnt, node_t* left, uint32_t _level, Key_t _high_key): node_t(sibling, left, _cnt, _level), high_key(_high_key){ }

	// constructor when tree height grows
	inode_t(Key_t split_key, node_t* left, node_t* right, node_t* sibling, uint32_t _level, Key_t _high_key): node_t(sibling, left, 1, _level){
	    high_key = _high_key;
	    entry[0].value = right;
	    entry[0].key = split_key;
	}

	void* operator new(size_t size) {
	    void* ret;
	    posix_memalign(&ret, 64, size);
	    return ret;
	}

	bool is_full(){
	    return (cnt == cardinality);
	}

	int find_lowerbound(Key_t& key){
#ifdef BINARY 
	    return lowerbound_binary(key);
#else
	    return lowerbound_linear(key);
#endif
	}

	node_t* scan_node(Key_t key){
	    if(sibling_ptr && (high_key < key)){
		return sibling_ptr;
	    }
	    else{
		int idx = find_lowerbound(key);
		if(idx > -1){
		    return entry[idx].value;
		}
		else{
		    return leftmost_ptr;
		}
	    } 
	}

	void insert(Key_t key, node_t* value){
	    int pos = find_lowerbound(key);
	    memmove(&entry[pos+2], &entry[pos+1], sizeof(entry_t<Key_t, node_t*>)*(cnt-pos-1));
	    /*
	    for(int i=cnt-1; i>pos; i--){
		entry[i+1].key = entry[i].key;
		entry[i+1].value = entry[i].value;
	    }*/

	    entry[pos+1].key = key;
	    entry[pos+1].value = value;

	    cnt++;

	}

	void insert(Key_t key, node_t* value, node_t* left){
	    int pos = find_lowerbound(key);
	    memmove(&entry[pos+2], &entry[pos+1], sizeof(entry_t<Key_t, node_t*>)*(cnt-pos-1));
	    entry[pos].value = left;
	    entry[pos+1].key = key;
	    entry[pos+1].value = value;
	}

	inode_t<Key_t>* split(Key_t& split_key){

	    int half = cnt/2;
	    split_key = entry[half].key;

	    int new_cnt = cnt-half-1;
	    auto new_node = new inode_t<Key_t>(sibling_ptr, new_cnt, entry[half].value, level, high_key);
	    //new_node->high_key = high_key;
	    memcpy(new_node->entry, entry+half+1, sizeof(entry_t<Key_t, node_t*>)*new_cnt);

	    sibling_ptr = static_cast<node_t*>(new_node);
	    high_key = entry[half].key;
	    cnt = half;
	    return new_node;
	}

	void print(){
	   std::cout << leftmost_ptr;
	    for(int i=0; i<cnt; i++){
		std::cout << " [" << i << "]" << entry[i].key << " " << entry[i].value << ", ";
	    }
	    std::cout << "  high_key: " << high_key << "\n\n";
	}

	void sanity_check(Key_t _high_key, bool first){
	    for(int i=0; i<cnt-1; i++){
		for(int j=i+1; j<cnt; j++){
		    if(entry[i].key > entry[j].key){
			std::cerr << "inode_t::key order is not perserved!!" << std::endl;
			std::cout << "[" << i << "].key: " << entry[i].key << "\t[" << j << "].key: " << entry[j].key << std::endl;
		    }
		}
	    }
	    for(int i=0; i<cnt; i++){
		if(sibling_ptr && (entry[i].key > high_key)){
		    std::cout << "inode_t:: " << i << "(" << entry[i].key << ") is higher than high key " << high_key << std::endl;
		}
		if(!first){
		    if(sibling_ptr && (entry[i].key <= _high_key)){
			std::cout << "inode_t:: " << i << "(" << entry[i].key << ") is smaller than previous high key " << _high_key << std::endl;
			std::cout << "--------- node_address " << this << " , current high_key " << high_key << std::endl;
		    }
		}
	    }
	    if(sibling_ptr != nullptr)
		(static_cast<inode_t<Key_t>*>(sibling_ptr))->sanity_check(high_key, false);
	}



    private:
	int lowerbound_linear(Key_t key){
	    /*
	    int count;
	    int idx;
	    do{
		count = cnt;
		idx = cardinality;
		for(int i=0; i<count; i++){
		    if(key <= entry[i].key){
			idx = i-1;
			break;
		    }
		}
		if(idx == cardinality)
		    idx = count-1;
	    }while(count != cnt);
	    return idx;
	    */

	    int count = cnt;
	    for(int i=0; i<count; i++){
		if(key <= entry[i].key){
		    return i-1; 
		}
	    }
	    return count-1;
	}

	int lowerbound_binary(Key_t key){
	    int lower = 0;
	    int upper = cnt;
	    do{
		int mid = ((upper - lower)/2) + lower;
		if(key <= entry[mid].key)
		    upper = mid;
		else
		    lower = mid+1;
		/*
		else if(key > entry[mid].key)
		    lower = mid + 1;
		else
		    return mid-1; */
	    }while(lower < upper);
	    return lower-1;
	}



};

template <typename Key_t>
class lnode_t: public node_t{
    public: 
#ifdef STRING_KEY
	static constexpr size_t cardinality = (PAGE_SIZE - sizeof(node_t) - sizeof(Key_t)) / sizeof(entry_t<Key_t, uint64_t>);
#else
	//static constexpr size_t cardinality = (1024 - sizeof(node_t) - sizeof(Key_t) - 8) / sizeof(entry_t<Key_t, uint64_t>);
	static constexpr size_t cardinality = (PAGE_SIZE - sizeof(node_t) - sizeof(Key_t) - 8) / sizeof(entry_t<Key_t, uint64_t>);
#endif
	Key_t high_key;
    private:
#ifndef STRING_KEY
	char dummy[8];
#endif
	entry_t<Key_t, uint64_t> entry[cardinality];
    public:

	// initial constructor
        lnode_t(): node_t() { 
	    /*
	    if constexpr(sizeof(Key_t) == 8)
		high_key = UINT64_MAX;
	    else{
		uint8_t max[] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    		0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    		0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    		0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};
		memset(&high_key, (void*)max, 31);
	    }*/
	} 

	// constructor when leaf splits
        lnode_t(node_t* sibling, int _cnt, uint32_t _level): node_t(sibling, nullptr, _cnt, _level){ }

	void* operator new(size_t size) {
	    void* ret;
	    posix_memalign(&ret, 64, size);
	    return ret;
	}

	bool is_full(){
	    return (cnt == cardinality);
	}

	int find_lowerbound(Key_t key){
	    return lowerbound_linear(key);
	}

	uint64_t scan_node(Key_t key){
	    if(sibling_ptr && high_key < key){ // move right if necessary
		return (static_cast<lnode_t<Key_t>*>(sibling_ptr))->scan_node(key);
	    }
	    return entry[find_lowerbound(key)].value;
	}

	uint64_t find(Key_t key){
	    return find_linear(key);
	}


	void insert(Key_t key, uint64_t value){
	    entry[cnt].key = key;
	    entry[cnt].value = value;
	    cnt++;
	}

	lnode_t<Key_t>* split(Key_t& split_key){
	    entry_t<Key_t, uint64_t> temp[cardinality];
	    int idx = 0;
	    for(int i=0; i<cnt; i++){
		if(entry[i].value != 0)
		    memcpy(&temp[idx++], &entry[i], sizeof(entry_t<Key_t, uint64_t>));
	    }
	    std::sort(temp, temp+idx, [](entry_t<Key_t, uint64_t>& a, entry_t<Key_t, uint64_t>& b){
		     return a.key < b.key;
		     });

	    int half = idx/2;
	    split_key = temp[half-1].key;
	    auto new_leaf = new lnode_t<Key_t>(sibling_ptr, idx-half, level);
	    
	    memcpy(new_leaf->entry, temp+half, (idx-half)*sizeof(entry_t<Key_t, uint64_t>));
	    new_leaf->high_key = high_key;

	    //memset(entry, 0x0, cardinality*sizeof(entry_t<Key_t, uint64_t>));
	    memcpy(entry, temp, half*sizeof(entry_t<Key_t, uint64_t>));
	    cnt = half;

	    sibling_ptr = static_cast<node_t*>(new_leaf);
	    high_key = temp[half-1].key;
	    return new_leaf;
	}


	bool update(Key_t& key, uint64_t value){
	    return update_linear(key, value);
	}

	int range_lookup(Key_t key, uint64_t* buf, int count, int range){
	    int _count = count;
	    entry_t<Key_t, uint64_t> _buf[cnt];
	    memcpy(_buf, entry, sizeof(entry_t<Key_t, uint64_t>)*cnt);
	    std::sort(_buf, _buf+cnt, [](entry_t<Key_t, uint64_t>& a, entry_t<Key_t, uint64_t>& b){
		     return a.key < b.key;
		     });

	    for(int i=0; i<cnt; i++){
		if(key <= _buf[i].key && _buf[i].value != 0){
		    buf[_count++] = _buf[i].value;
		    if(_count == range) return _count;
		}
	    }
	    return _count;
	}


	void print(){
	    for(int i=0; i<cnt; i++){
		std::cout << "[" << i << "]" << entry[i].key << " (" << entry[i].value << ") ";
	    }
	    std::cout << "  high_key: " << high_key << "\n\n";
	}

	void sanity_check(Key_t _high_key, bool first){
	}

    private:
	int lowerbound_linear(Key_t key){
	    return 0;
	}

	int lowerbound_binary(Key_t key){
	    return 0;
	}

	bool update_linear(Key_t key, uint64_t value){
	    for(int i=0; i<cnt; i++){
		if(key == entry[i].key && entry[i].value != 0){
		    entry[i].value = value;
		    return true;
		}
	    }
	    return false;
	}

	bool update_binary(Key_t key, uint64_t value){
	    return false;
	}

	uint64_t find_linear(Key_t key){
	    for(int i=0; i<cnt; i++){
		if(key == entry[i].key)
		    return entry[i].value;
	    }
	    return 0;
	}

	uint64_t find_binary(Key_t key){
	    return 0;
	}

};
}
#endif
