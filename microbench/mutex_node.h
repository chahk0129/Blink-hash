#ifndef NODE_HASHED_H__
#define NODE_HASHED_H__

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
#include <mutex>
#include <shared_mutex>

#include <x86intrin.h>

#include "hash.h"

#define BITS_PER_LONG 64
#define BITOP_WORD(nr) ((nr) / BITS_PER_LONG)

#define PAGE_SIZE (1024)

#define CACHELINE_SIZE 64

#define CAS(_p, _u, _v)  (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))

namespace BLINK_HASHED{

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


	node_t(): lock(0b0), sibling_ptr(nullptr), leftmost_ptr(nullptr), cnt(0), level(0){ }
	node_t(node_t* sibling, node_t* left, uint32_t count, uint32_t _level): lock(0b0), sibling_ptr(sibling), leftmost_ptr(left), cnt(count), level(_level) { }

	bool is_locked(uint64_t version){
	    return ((version & 0b10) == 0b10);
	}

	bool is_obsolete(uint64_t version){
	    return (version & 1) == 1;
	}

	uint64_t get_version(bool& need_restart){
	    uint64_t version = lock.load();
	    if(is_locked(version)){
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

	bool try_writelock(){
	    uint64_t version = lock.load();
	    if(is_locked(version) || is_obsolete(version)){
		_mm_pause();
		return false;
	    }

	    if(!lock.compare_exchange_strong(version, version + 0b10)){
		_mm_pause();
		return false;

	    }
	    return true;
	}

	void try_upgrade_writelock(uint64_t& version, bool& need_restart){
	    if(!lock.compare_exchange_strong(version, version + 0b10)){
		_mm_pause();
		need_restart =true;
	    }
	}

	void write_unlock(){
	    lock.fetch_add(0b10);
	}

	void write_unlock_obsolete(){
	    lock.fetch_sub(0b11);
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

static constexpr size_t entry_num = 4;

template <typename Key_t, typename Value_t>
struct bucket_t{
    std::shared_mutex mutex;
    entry_t<Key_t, Value_t> entry[entry_num];
};

static constexpr size_t seed = 0xc70697UL;

template <typename Key_t>
class lnode_t : public node_t{
    public: 
	//static constexpr size_t cardinality = (PAGE_SIZE - sizeof(node_t) - sizeof(uint64_t) - sizeof(Key_t)) / sizeof(lentry_t<Key_t, uint64_t>);
	static constexpr size_t cardinality = (1024*512- sizeof(node_t) - sizeof(Key_t))/ sizeof(bucket_t<Key_t, uint64_t>);
	//static constexpr size_t cardinality = (1024*4 - sizeof(node_t) - sizeof(Key_t))/ sizeof(bucket_t<Key_t, uint64_t>);
	static constexpr size_t num_slot = 1024;
	//static constexpr size_t num_slot = 16;
	Key_t high_key;
    private:
	bucket_t<Key_t, uint64_t> bucket[cardinality];
    public:
	bool _try_splitlock(){
	    if(!try_writelock()){
		_mm_pause();
		return false;
	    }

	    for(int i=0; i<cardinality; i++){
		bucket[i].mutex.lock();
	    }
	    return true;
	}

	void _split_unlock(){
	    write_unlock();
	    for(int i=0; i<cardinality; i++){
		bucket[i].mutex.unlock();
	    }
	}

	void _write_unlock(int loc){
	}


	/* tokens 
	   -1= blocked
	   0 = empty
	   1 = occupied (in writing)
	   2 = occupied (written)
	 */

	// initial constructor
        lnode_t(): node_t() { 
	    std::cout << "sizeof mutex: " << sizeof(std::shared_mutex) << std::endl;
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
        lnode_t(node_t* sibling, int _cnt, uint32_t _level): node_t(sibling, nullptr, 0, _level){
	    std::cout << "sizeof mutex: " << sizeof(std::shared_mutex) << std::endl;
	}

	void* operator new(size_t size) {
	    void* ret;
	    posix_memalign(&ret, 64, size);
	    return ret;
	}

	bool is_full(){
	    return false;
	}

	int find_lowerbound(Key_t key){
	    return lowerbound_linear(key);
	}
	uint64_t find(Key_t key){
	    return find_linear(key);
	}

	void insert_after_split(Key_t key, uint64_t value){
	    auto hash_key = hash_funcs[1](&key, sizeof(Key_t), seed);
	    /*
	    size_t hash_key;
	    if constexpr(sizeof(Key_t) > 8)
		hash_key = hash_funcs[1](key, sizeof(Key_t), seed);
	    else
		hash_key = hash_funcs[1](&key, sizeof(Key_t), seed);
		*/
	    auto loc = hash_key % cardinality;

	    for(int j=0; j<num_slot; j++){
		loc = (loc + j) % cardinality;
		for(int i=0; i<entry_num; i++){
		    if(bucket[loc].entry[i].key == 0){
			bucket[loc].entry[i].key = key;
			bucket[loc].entry[i].value = value;
			return;
		    }
		}
	    }
	    std::cout << __func__ << ": failed to insert" << std::endl;
	}


	int insert(Key_t key, uint64_t value){
	    auto hash_key = hash_funcs[1](&key, sizeof(Key_t), seed);
	    /*
	    size_t hash_key;
	    if constexpr(sizeof(Key_t) > 8)
		hash_key = hash_funcs[1](key, sizeof(Key_t), seed);
	    else
		hash_key = hash_funcs[1](&key, sizeof(Key_t), seed);
		*/
	    auto loc = hash_key % cardinality;
	    bool need_restart = false;
	    auto version = (static_cast<node_t*>(this))->get_version(need_restart);
	    if(need_restart) return -1;

	    for(int j=0; j<num_slot; j++){
		loc = (loc + j) % cardinality;
		std::unique_lock<std::shared_mutex> lock(bucket[loc].mutex);
		/*
		if(!bucket[loc].mutex.try_lock()){
		    return -1;
		}*/
		auto _version = (static_cast<node_t*>(this))->get_version(need_restart);
		if(need_restart || (version != _version)){
		   // bucket[loc].mutex.unlock();
		    return -1;
		}
		for(int i=0; i<entry_num; i++){
		    if(bucket[loc].entry[i].key == 0){
			bucket[loc].entry[i].key = key;
			bucket[loc].entry[i].value = value;
		//	bucket[loc].mutex.unlock();
			return 0;
		    }
		}
		//bucket[loc].mutex.unlock();
	    }

	    if(!_try_splitlock()){
		_mm_pause();
		return -1;
	    }

	    auto _version = (static_cast<node_t*>(this))->get_version(need_restart);
	    if(version + 0b10 != _version){
		_split_unlock();
		return -1;
	    }

	    return 1; // return split flag
	}

	bool insert_for_split(Key_t key, uint64_t value){
	    auto hash_key = hash_funcs[1](&key, sizeof(Key_t), seed);
	    /*
	    size_t hash_key;
	    if constexpr(sizeof(Key_t) > 8)
		hash_key = hash_funcs[1](key, sizeof(Key_t), seed);
	    else
		hash_key = hash_funcs[1](&key, sizeof(Key_t), seed);
		*/
	    auto loc = hash_key % cardinality;

	    for(int j=0; j<num_slot; j++){
		loc = (loc + j) % cardinality;
		for(int i=0; i<entry_num; i++){
		    if(bucket[loc].entry[i].key == 0){
			bucket[loc].entry[i].key = key;
			bucket[loc].entry[i].value = value;
			return true;
		    }
		}
	    }
	    return false;
	}
	    
	void swap(entry_t<Key_t, uint64_t>* a, entry_t<Key_t, uint64_t>* b){
	    entry_t<Key_t, uint64_t> temp;
	    memcpy(&temp, a, sizeof(entry_t<Key_t, uint64_t>));
	    memcpy(a, b, sizeof(entry_t<Key_t, uint64_t>));
	    memcpy(b, &temp, sizeof(entry_t<Key_t, uint64_t>));
	}

	int partition(entry_t<Key_t, uint64_t>* entry, int left, int right){
	    Key_t last = entry[right].key;
	    int i = left, j = left;
	    while(j < right){
		if(entry[j].key < last){
		    swap(&entry[i], &entry[j]);
		    i++;
		}
		j++;
	    }
	    swap(&entry[i], &entry[right]);
	    return i;
	}

	int random_partition(entry_t<Key_t, uint64_t>* entry, int left, int right){
	    int n = right - left + 1;
	    int pivot = rand() % n;
	    swap(&entry[left+pivot], &entry[right]);
	    return partition(entry, left, right);
	}

	void median_util(entry_t<Key_t, uint64_t>* entry, int left, int right, int k, int& a, int& b){
	    if(left <= right){
		int partition_idx = random_partition(entry, left, right);
		if(partition_idx == k){
		    b = partition_idx;
		    if(a != -1)
			return;
		}
		else if(partition_idx == k-1){
		    a = partition_idx;
		    if(b != -1)
			return;
		}

		if(partition_idx >= k){
		    return median_util(entry, left, partition_idx-1, k, a, b);
		}
		else{
		    return median_util(entry, partition_idx+1, right, k, a, b);
		}
	    }
	}

	int find_median(entry_t<Key_t, uint64_t>* entry, int n){
	    int ret;
	    int a = -1, b = -1;
	    if(n % 2 == 1){
		median_util(entry, 0, n-1, n/2-1, a, b);
		ret = b;
	    }
	    else{
		median_util(entry, 0, n-1, n/2, a, b);
		ret = (a+b)/2;
	    }
	    return ret;
	}

	lnode_t<Key_t>* split(Key_t& split_key){ 
	    entry_t<Key_t, uint64_t> temp[cardinality*entry_num];
	    int _idx = 0;
	    for(int i=0; i<cardinality; i++){
		auto token = bucket[i].token.load();
		for(int j=0; j<entry_num; j++){
		    if(bucket[i].entry[j].key != 0){
			temp[_idx].key = bucket[i].entry[j].key;
			temp[_idx].value = bucket[i].entry[j].value;
			_idx++;
			bucket[i].entry[j].key = 0;
		    }
		}
	    }

#ifdef SORT
	    std::sort(temp, temp+_idx, [](entry_t<Key_t, uint64_t>& a, entry_t<Key_t, uint64_t>& b){
		    return a.key < b.key;
		    });

	    int half = _idx/2;
	    split_key = temp[half-1].key;
	    auto new_right = new lnode_t<Key_t>(sibling_ptr, 0, level);

	    for(int i=0; i<half; i++){
		if(!this->insert_for_split(temp[i].key, temp[i].value)){
		    std::cerr << "hash collision occurs during split ..." << std::endl;
		    exit(0);
		}
	    }

	    for(int i=half; i<_idx; i++){
		if(!new_right->insert_for_split(temp[i].key, temp[i].value)){
		    std::cerr << "hash collision occurs during split ..." << std::endl;
		    exit(0);
		}
	    }
	    
	    high_key = temp[half-1].key;
	    new_right->high_key = temp[_idx-1].key;
#else
	    int half = find_median(temp, _idx);
	    split_key = temp[half].key;

	    Key_t max_key = split_key; 
	    auto new_right = new lnode_t<Key_t>(sibling_ptr, 0, level);
	    for(int i=0; i<_idx; i++){
		if(max_key < temp[i].key){
		    max_key = temp[i].key;
		}

		if(temp[i].key <= split_key){
		    if(!this->insert_for_split(temp[i].key, temp[i].value)){
			std::cerr << "hash collision occurs during split ..." << std::endl;
			exit(0);
		    }
		}
		else{
		    if(!new_right->insert_for_split(temp[i].key, temp[i].value)){
			std::cerr << "hash collision occurs during split ..." << std::endl;
			exit(0);
		    }
		}
	    }



	    sibling_ptr = new_right;
	    high_key = split_key;
	    new_right->high_key = max_key;
#endif
	    return new_right;
	}


	int update(Key_t& key, uint64_t value){
	    return update_linear(key, value);
	}

	int range_lookup(Key_t key, uint64_t* buf, int count, int range){
	    /*
	    entry_t<Key_t, uint64_t> _buf[cardinality];
	    int _count = count;
	    int idx = 0;
	    for(int j=0; j<num_slot; j++){
	    restart:
		auto token = bucket[j].token.load();
		for(int i=0; i<entry_num; i++){
		    int _idx = (uint32_t)1 << i;
		    if((token & _idx) != 0){
			_buf[idx].key = bucket[j].entry[i].key;
			_buf[idx].value = bucket[j].entry[i].value;
			idx++;
		    }
		}
		auto _token = bucket[j].token.load();
		if(token != _token) goto restart;
	    }

	    std::sort(_buf, _buf+idx, [](entry_t<Key_t, uint64_t>& a, entry_t<Key_t, uint64_t>& b){
		    return a.key < b.key;
		    });

	    for(int i=0; i<idx; i++){
		buf[_count++] = _buf[i].value;
		if(_count == range) return _count;
	    }
	    return _count;
*/
	    return 0;
	}


	void print(){
	    for(int i=0; i<cardinality; i++){
		for(int j=0; j<entry_num; j++){
		    std::cout << "[" << i << "][" << j << "] " << bucket[i].entry[j].key << " (" << bucket[i].entry[j].value << ") ";
		}
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

	int update_linear(Key_t key, uint64_t value){
	    auto hash_key = hash_funcs[1](&key, sizeof(Key_t), seed);
	    auto loc = hash_key % cardinality;

	    bool need_restart = false;
	    auto version = (static_cast<node_t*>(this))->get_version(need_restart);
	    if(need_restart) return -1;

	    for(int j=0; j<num_slot; j++){
		loc = (loc + j) % cardinality;
		if(!bucket[loc].mutex.try_lock()){
		    return -1;
		}

		auto _version = (static_cast<node_t*>(this))->get_version(need_restart);
		if(need_restart || (version != _version)){
		    bucket[loc].mutex.unlock();
		    return -1;
		}

		for(int i=0; i<entry_num; i++){
		    if(bucket[loc].entry[i].key == key){
			bucket[loc].entry[i].value = value;
			bucket[loc].mutex.unlock();
			return 0;
		    }
		}
		bucket[loc].mutex.unlock();
	    }

	    return 1; // key not found
	}

	uint64_t find_linear(Key_t key){
	    auto hash_key = hash_funcs[1](&key, sizeof(Key_t), seed);
	    /*
	    size_t hash_key;
	    if constexpr(sizeof(Key_t) > 8)
		hash_key = hash_funcs[1](key, sizeof(Key_t), seed);
	    else
		hash_key = hash_funcs[1](&key, sizeof(Key_t), seed);
		*/
	restart:
	    auto loc = hash_key % cardinality;

	    for(int j=0; j<num_slot; j++){
		loc = (loc + j) % cardinality;
		std::shared_lock<std::shared_mutex> lock(bucket[loc].mutex);
		/*
		if(!bucket[loc].mutex.try_lock_shared()){
		    goto restart;
		}*/

		for(int i=0; i<entry_num; i++){
		    if(bucket[loc].entry[i].key == key){
			auto ret = bucket[loc].entry[i].value;
		//	bucket[loc].mutex.unlock_shared();
			return ret;
		    }
		}
		//bucket[loc].mutex.unlock_shared();
	    }

	    return 0;
	}

	uint64_t find_binary(Key_t key){
	    return 0;
	}

};
}
#endif
