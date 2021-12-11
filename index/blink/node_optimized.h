#ifndef NODE_OPTIMIZED_H__
#define NODE_OPTIMIZED_H__

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <immintrin.h>
#include <atomic>
#include <iostream>
#include <cmath>
#include <limits.h>
#include <utility>

#ifdef URL_KEYS
#define PAGE_SIZE (4096)
#elif defined STRING_KEY
#define PAGE_SIZE (1024)
#else
#define PAGE_SIZE (512)
#endif

#define CACHELINE_SIZE 64

#define LINEAR_SEARCH
// UPDATE_LOCK is enabled when sizeof(value) <= 8 for concurrent updates
//#define UPDATE_LOCK
#define CAS(_p, _u, _v) (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))


namespace BLINK_OPTIMIZED{

class node_t{
    public:
	std::atomic<uint64_t> lock;
	node_t* sibling_ptr;
	int cnt;
	uint32_t level;


	inline uint64_t _rdtsc(){
	    uint32_t lo, hi;
	    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
	    return (((uint64_t)hi << 32) | lo);
	}

	node_t(): lock(0), sibling_ptr(nullptr), cnt(0), level(0){ }
	node_t(node_t* sibling, uint32_t count, uint32_t _level): lock(0), sibling_ptr(sibling), cnt(count), level(_level) { }

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
	    uint64_t version = (uint64_t)lock.load();
	    if(is_locked(version) || is_obsolete(version)){
		_mm_pause();
		need_restart = true;
	    }
	    return version;
	}

	#ifdef UPDATE_LOCK
	bool try_writelock(){
	    uint64_t version = lock.load();
	    if(is_locked(version) || is_obsolete(version) || ((version >> 32) > 0)){
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

	void try_upgrade_writelock(uint64_t version, bool& need_restart){
	    uint64_t _version = lock.load();
	    if(((_version & (~0u)) != (version & (~0u))) || ((_version >> 32) > 0)){
		_mm_pause();
		need_restart = true;
		return;
	    }

	    if(!lock.compare_exchange_strong(_version, _version + 0b10)){
		_mm_pause();
		need_restart = true;
	    }
	}

	void try_upgrade_updatelock(uint64_t version, bool& need_restart){
	    uint64_t _version = lock.load();
	    if((_version & (~0u)) != (version & (~0u))){
		_mm_pause();
		need_restart = true;
		return;
	    }

	    if(!lock.compare_exchange_strong(_version, _version + ((uint64_t)1 << 32))){
		_mm_pause();
		need_restart = true;
	    }
	}

	void write_unlock(){
	    uint64_t version = lock.load();
	    if((version & (~0u)) == UINT32_MAX-1)
		lock.store(version & 0u, std::memory_order_relaxed);
	    else
		lock.fetch_add(0b10);
	}

	void update_unlock(){
	    lock.fetch_sub((uint64_t)1 << 32);
	}

	void write_unlock_obsolete(){
	    uint64_t version = lock.load();
	    if((version & (~0u)) == UINT32_MAX-1)
		lock.store((version & 0u) + 0b1, std::memory_order_relaxed);
	    else
		lock.fetch_add(0b11);
	}

	#else
	bool try_writelock(){ // exclusive lock
	    uint64_t version = lock.load();
	    if(is_locked(version) || is_obsolete(version)){
		_mm_pause();
		return false;
	    }

	    if(lock.compare_exchange_strong(version, version + 0b10)){
		return true;
	    }
	    else{
		_mm_pause();
		return false;
	    }
	}

	void try_upgrade_writelock(uint64_t version, bool& need_restart){
	    uint64_t _version = lock.load();
	    if(version != _version){
		_mm_pause();
		need_restart = true;
		return;
	    }

	    if(!lock.compare_exchange_strong(_version, _version + 0b10)){
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

	#endif

	int get_cnt(){
	    return cnt;
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
	static constexpr size_t cardinality = (PAGE_SIZE - sizeof(node_t) - sizeof(Key_t)) / sizeof(entry_t<Key_t, node_t*>);
	Key_t high_key;
    private:
        entry_t<Key_t, node_t*> entry[cardinality];
    
    public:


    	inode_t() { } 

	// constructor when inode needs to split
	inode_t(node_t* sibling, int _cnt, node_t* left, uint32_t _level, Key_t _high_key): node_t(sibling, _cnt, _level), high_key(_high_key){
	    entry[0].value = left;
	}

	// constructor when tree height grows
	inode_t(Key_t split_key, node_t* left, node_t* right, node_t* sibling, uint32_t _level, Key_t _high_key): node_t(sibling, 1, _level){
	    high_key = _high_key;
	    entry[0].key = split_key;
	    entry[0].value = left;
	    entry[1].value = right;
	}

	void* operator new(size_t size) {
	    void* ret;
	    posix_memalign(&ret, 64, size);
	    return ret;
	}

	bool is_full(){
	    return (cnt == cardinality-1);
	}

	int find_lowerbound(Key_t& key){
	    return lowerbound_linear(key);
	}

	node_t* scan_node(Key_t key){
	    if(sibling_ptr && (high_key < key)){
		return sibling_ptr;
	    }
	    else{
		return entry[find_lowerbound(key)].value;
	    } 
	}

	node_t* leftmost_ptr(){
	    return entry[0].value;
	}

	void insert(Key_t key, node_t* value){
	    int pos = find_lowerbound(key);
	    memmove(entry+pos+1, entry+pos, sizeof(entry_t<Key_t, node_t*>)*(cnt-pos+1));
	    entry[pos].key = key;
	    entry[pos].value = value;
	    std::swap(entry[pos].value, entry[pos+1].value);

	    cnt++;

	}

	inode_t<Key_t>* split(Key_t& split_key){
	    int half = cnt - cnt/2;
	    split_key = entry[half].key;

	    int new_cnt = cnt - half - 1;
	    auto new_node = new inode_t<Key_t>(sibling_ptr, new_cnt, entry[half].value, level, high_key);
	    memcpy(new_node->entry, entry+half+1, sizeof(entry_t<Key_t, node_t*>)*(new_cnt+1));

	    sibling_ptr = static_cast<node_t*>(new_node);
	    high_key = entry[half].key;
	    cnt = half;
	    return new_node;
	}

	void print(){
	    for(int i=0; i<cnt; i++){
		std::cout << " [" << i << "]" << entry[i].key << " " << entry[i].value << " ";
	    }
	    std::cout << "[" << cnt << "]" << entry[cnt].key << " " << entry[cnt].value << " ";
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
	    for(int i=0; i<cnt; i++){
		if(key <= entry[i].key){
		    return i;
		}
	    }
	    return cnt;
	}
};

template <typename Key_t>
class lnode_t: public node_t{
    public: 
	static constexpr size_t cardinality = (PAGE_SIZE - sizeof(node_t) - sizeof(Key_t)) / sizeof(entry_t<Key_t, uint64_t>);

	Key_t high_key;
    private:
	entry_t<Key_t, uint64_t> entry[cardinality];

    public:

	// initial constructor
        lnode_t(): node_t() { } 

	// constructor when leaf splits
        lnode_t(node_t* sibling, int _cnt, uint32_t _level): node_t(sibling, _cnt, _level) { }

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

	uint64_t find(Key_t key){
	    return find_linear(key);
	}

	void insert(Key_t key, uint64_t value){
	    if(cnt){
		int pos = find_lowerbound(key);
		memmove(&entry[pos+1], &entry[pos], sizeof(entry_t<Key_t, uint64_t>)*(cnt-pos));
		entry[pos].key = key;
		entry[pos].value = value;
	    }
	    else{
		entry[0].key = key;
		entry[0].value= value;
	    }

	    cnt++;
	}

	lnode_t<Key_t>* split(Key_t& split_key){
	    int half = cnt/2;
	    int new_cnt = cnt - half;
	    split_key = entry[half-1].key;

	    auto new_leaf = new lnode_t<Key_t>(sibling_ptr, new_cnt, level);
	    new_leaf->high_key = high_key;
	    memcpy(new_leaf->entry, entry+half, sizeof(entry_t<Key_t, uint64_t>)*new_cnt);

	    sibling_ptr = static_cast<node_t*>(new_leaf);
	    high_key = entry[half-1].key;
	    cnt = half;
	    return new_leaf;
	}

	bool remove(Key_t key){
	    if(cnt){
		int pos = find_pos_linear(key);
		// no matching key found
		if(pos == -1) return false;
		memmove(&entry[pos], &entry[pos+1], sizeof(entry_t<Key_t, uint64_t>)*(cnt - pos - 1));
		cnt--;
		return true;
	    }
	    return false;
	}

	bool update(Key_t key, uint64_t value){
	    return update_linear(key, value);
	}

	int range_lookup(int idx, uint64_t* buf, int count, int range){
	    auto _count = count;
	    for(int i=idx; i<cnt; i++){
		buf[_count++] = entry[i].value;
		if(_count == range) return _count;
	    }
	    return _count;
	}

	void print(){
	    for(int i=0; i<cnt; i++){
		std::cout << "[" << i << "]" << entry[i].key << " ";
	    }
	    std::cout << "  high_key: " << high_key << "\n\n";
	}

	void sanity_check(Key_t _high_key, bool first){
	    for(int i=0; i<cnt-1; i++){
		for(int j=i+1; j<cnt; j++){
		    if(entry[i].key > entry[j].key){
			std::cerr << "lnode_t::key order is not perserved!!" << std::endl;
			std::cout << "[" << i << "].key: " << entry[i].key << "\t[" << j << "].key: " << entry[j].key << std::endl;
		    }
		}
	    }
	    for(int i=0; i<cnt; i++){
		if(sibling_ptr && (entry[i].key > high_key)){
		    std::cout << i << "lnode_t:: " << "(" << entry[i].key << ") is higher than high key " << high_key << std::endl;
		}
		if(!first){
		    if(sibling_ptr && (entry[i].key < _high_key)){
			std::cout << "lnode_t:: " << i << "(" << entry[i].key << ") is smaller than previous high key " << _high_key << std::endl;
			std::cout << "--------- node_address " << this << " , current high_key " << high_key << std::endl;
		    }
		}
	    }
	    if(sibling_ptr != nullptr)
		(static_cast<lnode_t<Key_t>*>(sibling_ptr))->sanity_check(high_key, false);
	}

	int get_cnt(){
	    return cnt;
	}

	double utilization(){
	    return (double)cnt / cardinality;
	}
    private:

	int lowerbound_linear(Key_t key){
	    for(int i=0; i<cnt; i++){
		if(key <= entry[i].key)
		    return i;
	    }
	    return cnt;
	}

	bool update_linear(Key_t key, uint64_t value){
	    for(int i=0; i<cnt; i++){
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

	uint64_t find_linear(Key_t key){
	    for(int i=0; i<cnt; i++){
		if(key == entry[i].key){
		    auto ret = entry[i].value;
		    return ret;
		}
	    }
	    return 0;
	}

	int find_pos_linear(Key_t key){
	    for(int i=0; i<cnt; i++){
		if(key == entry[i].key){
		    return i;
		}
	    }
	    return -1;
	}
};
}
#endif
