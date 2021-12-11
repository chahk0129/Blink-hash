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

#define PAGE_SIZE (512)

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

template <typename Key_t>
Key_t INVALID;

template <typename Key_t>
void invalid_initialize(){
    memset(&INVALID<Key_t>, 0, sizeof(Key_t));
}

static constexpr size_t entry_num = 4;

template <typename Key_t, typename Value_t>
struct bucket_t{
    entry_t<Key_t, Value_t> entry[entry_num];
};

static constexpr size_t seed = 0xc70697UL;

static constexpr int hash_funcs_num = 2;
static constexpr size_t num_slot = 4;

template <typename Key_t>
class lnode_t : public node_t{
    public: 
	static constexpr size_t leaf_size = 1024*64;
	static constexpr size_t cardinality = (leaf_size - sizeof(node_t) - sizeof(Key_t))/ (sizeof(bucket_t<Key_t, uint64_t>) + sizeof(std::shared_mutex) + sizeof(uint8_t)*entry_num);
	//static constexpr size_t cardinality = (1024*4 - sizeof(node_t) - sizeof(Key_t))/ sizeof(bucket_t<Key_t, uint64_t>);
	Key_t high_key;
	uint8_t fingerprints[cardinality*entry_num];
	std::shared_mutex mutex[cardinality];
    private:
	bucket_t<Key_t, uint64_t> bucket[cardinality];
    public:

	bool _try_splitlock(){
	    if(!try_writelock()){
		_mm_pause();
		return false;
	    }

	    for(int i=0; i<cardinality; i++){
		mutex[i].lock();
	    }
	    return true;
	}

	void _split_unlock(){
	    write_unlock();

	    for(int i=0; i<cardinality; i++){
		mutex[i].unlock();
	    }
	}

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
        lnode_t(node_t* sibling, int _cnt, uint32_t _level): node_t(sibling, nullptr, 0, _level){
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

	uint8_t _hash(size_t key){
	    return (uint8_t)(key % 256);
	}

	void insert_after_split(Key_t key, uint64_t value){
	    for(int i=0; i<hash_funcs_num; i++){
		auto hash_key = h(&key, sizeof(Key_t), i);
		auto loc = hash_key % cardinality;
		auto fingerprint = _hash(hash_key) | 1;

#ifndef RANDOM
		for(int j=0; j<num_slot; j++){
		    loc = (loc + j) % cardinality;
		    for(int i=0; i<entry_num; i++){
			if((fingerprints[loc*entry_num+i] & 1) == 0){
			    fingerprints[loc*entry_num+i] = fingerprint;
			    bucket[loc].entry[i].key = key;
			    bucket[loc].entry[i].value = value;
			    return;
			}
		    }
		}
#else
		for(int j=0; j<cardinality; j++){
		    loc = (loc + j) % cardinality;
		    for(int i=0; i<entry_num; i++){
			if((fingerprints[loc*entry_num+i] & 1) == 0){
			    fingerprints[loc*entry_num+i] = fingerprint;
			    bucket[loc].entry[i].key = key;
			    bucket[loc].entry[i].value = value;
			    return;
			}
		    }
		}
#endif
	    }

	    std::cout << __func__ << ": failed to insert " << std::endl;
	}


#ifdef BREAKDOWN
	int insert(Key_t key, uint64_t value, uint64_t version, uint64_t& leaf_write, uint64_t& leaf_sync){
#else
	int insert(Key_t key, uint64_t value, uint64_t version){
#endif
	//int insert(Key_t key, uint64_t value){
#ifdef BREAKDOWN
	    uint64_t start, end;
#endif
	    bool need_restart = false;
	    for(int k=0; k<hash_funcs_num; k++){
		auto hash_key = h(&key, sizeof(Key_t), k);
#ifdef BREAKDOWN
		start = _rdtsc();
#endif
		auto loc = hash_key % cardinality;
		auto fingerprint = _hash(hash_key) | 1;
		for(int j=0; j<num_slot; j++){
		    loc = (loc + j) % cardinality;
#ifdef BREAKDOWN
		    end = _rdtsc();
		    leaf_write += (end - start);
		    start = _rdtsc();
#endif
		    std::unique_lock<std::shared_mutex> lock(mutex[loc]);

		    auto _version = (static_cast<node_t*>(this))->get_version(need_restart);
		    if(need_restart || (version != _version)){
#ifdef BREAKDOWN
			end = _rdtsc();
			leaf_sync += (end - start);
#endif
			return -1;
		    }
#ifdef BREAKDOWN
		    end = _rdtsc();
		    leaf_sync += (end - start);
		    start = _rdtsc();
#endif
		    for(int i=0; i<entry_num; i++){
			if((fingerprints[loc*entry_num+i] & 1) == 0){
			    fingerprints[loc*entry_num+i] = fingerprint;
			    bucket[loc].entry[i].key = key;
			    bucket[loc].entry[i].value = value;
#ifdef BREAKDOWN
			    end = _rdtsc();
			    leaf_write += (end- start);
#endif
			    return 0;
			}
		    }
		}
	    }

#ifdef BREAKDOWN
	    end = _rdtsc();
	    leaf_write += (end- start);
	    start = _rdtsc();
#endif
	    if(!_try_splitlock()){
		_mm_pause();
#ifdef BREAKDOWN
		end = _rdtsc();
		leaf_sync+= (end- start);
#endif
		return -1;
	    }

	    auto _version = (static_cast<node_t*>(this))->get_version(need_restart);
	    if(version + 0b10 != _version){
		_split_unlock();
#ifdef BREAKDOWN
		end = _rdtsc();
		leaf_sync+= (end- start);
#endif
		return -1;
	    }

#ifdef BREAKDOWN
	    end = _rdtsc();
	    leaf_sync+= (end- start);
#endif
	    return 1; // return split flag
	}
	    
	void swap(Key_t* a, Key_t* b){
	    Key_t temp;
	    memcpy(&temp, a, sizeof(Key_t));
	    memcpy(a, b, sizeof(Key_t));
	    memcpy(b, &temp, sizeof(Key_t));
	}

	int partition(Key_t* keys, int left, int right){
	    Key_t last = keys[right];
	    int i = left, j = left;
	    while(j < right){
		if(keys[j] < last){
		    swap(&keys[i], &keys[j]);
		    i++;
		}
		j++;
	    }
	    swap(&keys[i], &keys[right]);
	    return i;
	}

	int random_partition(Key_t* keys, int left, int right){
	    int n = right - left + 1;
	    int pivot = rand() % n;
	    swap(&keys[left+pivot], &keys[right]);
	    return partition(keys, left, right);
	}

	void median_util(Key_t* keys, int left, int right, int k, int& a, int& b){
	    if(left <= right){
		int partition_idx = random_partition(keys, left, right);
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
		    return median_util(keys, left, partition_idx-1, k, a, b);
		}
		else{
		    return median_util(keys, partition_idx+1, right, k, a, b);
		}
	    }
	}

	int find_median(Key_t* keys, int n){
	//int find_median(entry_t<Key_t, uint64_t>* entry, int n){
	    int ret;
	    int a = -1, b = -1;
	    if(n % 2 == 1){
		median_util(keys, 0, n-1, n/2-1, a, b);
		ret = b;
	    }
	    else{
		median_util(keys, 0, n-1, n/2, a, b);
		ret = (a+b)/2;
	    }
	    return ret;
	}

#ifdef SPLIT_BREAKDOWN
	lnode_t<Key_t>* split(Key_t& split_key, uint64_t& alloc_time, uint64_t& copy_time, uint64_t& find_median_time){ 
#else
	lnode_t<Key_t>* split(Key_t& split_key){ 
#endif
#ifdef RANDOM
	    for(int i=0; i<cardinality*entry_num; i++){
		if((fingerprints[i] & 1) != 0){
		    split_key = bucket[i/entry_num].entry[i%entry_num].key;
		    break;
		}
	    }
	    high_key = split_key;
#else
#ifdef SPLIT_BREAKDOWN
	    uint64_t start, end;
	    start = _rdtsc();
#endif
	    Key_t temp[cardinality*entry_num];
	    int valid_num = 0;
	    for(int i=0; i<cardinality*entry_num; i++){
		if((fingerprints[i] & 1) != 0){
		    temp[valid_num++] = bucket[i/entry_num].entry[i%entry_num].key;
		}
	    }
	    int half = find_median(temp, valid_num);
	    split_key = temp[half];
	    high_key = temp[half];
#ifdef SPLIT_BREAKDOWN
	    end = _rdtsc();
	    find_median_time += (end - start);
	    start = _rdtsc();
#endif
#endif
	    Key_t max_key = split_key; 
	    auto new_right = new lnode_t<Key_t>(sibling_ptr, 0, level);
	    sibling_ptr = new_right;
#ifdef SPLIT_BREAKDOWN
	    end = _rdtsc();
	    alloc_time += (end - start);
	    start = _rdtsc();
#endif

#ifdef BULK
	    memcpy(new_right->bucket, bucket, sizeof(bucket_t<Key_t, uint64_t>)*cardinality);
	    for(int i=0; i<cardinality*entry_num; i++){
		if((fingerprints[i] & 1) != 0){
		    if(bucket[i/entry_num].entry[i%entry_num].key > split_key){
			if(max_key < bucket[i/entry_num].entry[i%entry_num].key)
			    max_key = bucket[i/entry_num].entry[i%entry_num].key;
			new_right->fingerprints[i] = fingerprints[i];
			fingerprints[i] = 0;
		    }
		}
	    }
#else
	    for(int i=0; i<cardinality*entry_num; i++){
		if((fingerprints[i] & 1) != 0){
		    if(bucket[i/entry_num].entry[i%entry_num].key > split_key){
			if(max_key < bucket[i/entry_num].entry[i%entry_num].key)
			    max_key = bucket[i/entry_num].entry[i%entry_num].key;
			new_right->fingerprints[i] = fingerprints[i];
			fingerprints[i] = 0;
			memcpy(&new_right->bucket[i/entry_num].entry[i%entry_num], &bucket[i/entry_num].entry[i%entry_num], sizeof(entry_t<Key_t, uint64_t>));
		    }
		}
	    }
#endif
#ifdef SPLIT_BREAKDOWN
	    end = _rdtsc();
	    copy_time += (end - start);
#endif

	    new_right->high_key = max_key;
	    sibling_ptr = new_right;

	    return new_right;
	}


	int update(Key_t& key, uint64_t value){
	    return update_linear(key, value);
	}

	int range_lookup(Key_t key, uint64_t* buf, int count, int range){
	    entry_t<Key_t, uint64_t> _buf[cardinality];
	    int _count = count;
	    int idx = 0;
	    bool need_restart = false;

	    for(int j=0; j<num_slot; j++){
		std::shared_lock<std::shared_mutex> lock(mutex[j]);
		for(int i=0; i<entry_num; i++){
		    if((fingerprints[j*entry_num+i] & 1) == 1){
			if(bucket[j].entry[i].key >= key){
			    _buf[idx].key = bucket[j].entry[i].key;
			    _buf[idx].value = bucket[j].entry[i].value;
			    idx++;
			}
		    }
		}
	    }

	    std::sort(_buf, _buf+idx, [](entry_t<Key_t, uint64_t>& a, entry_t<Key_t, uint64_t>& b){
		    return a.key < b.key;
		    });

	    for(int i=0; i<idx; i++){
		buf[_count++] = _buf[i].value;
		if(_count == range) return _count;
	    }
	    return _count;
	}


	void print(){
	    std::cout << "fingerprints[ ";
	    for(int i=0; i<cardinality*entry_num; i++){
		    printf("%d ", fingerprints[i]);
		//std::cout << fingerprints[i] << " ";
	    }
	    std::cout << "\n";
	    for(int i=0; i<cardinality; i++){
		for(int j=0; j<entry_num; j++){
		    std::cout << "[" << i << "][" << j << "] " << bucket[i].entry[j].key << " (" << bucket[i].entry[j].value << ") ";
		}
	    }
	    std::cout << "  high_key: " << high_key << "\n\n";
	}

	void sanity_check(Key_t _high_key, bool first){
	}

	double utilization(){
	    int cnt = 0;
	    for(int i=0; i<cardinality*entry_num; i++){
		if((fingerprints[i] & 1) == 1)
		    cnt++;
	    }
	    return (double)cnt/(cardinality*entry_num);
	}

    private:

	int lowerbound_linear(Key_t key){
	    return 0;
	}

	int lowerbound_binary(Key_t key){
	    return 0;
	}

	int update_linear(Key_t key, uint64_t value){
	    bool need_restart = false;
	    for(int k=0; k<hash_funcs_num; k++){
		auto hash_key = h(&key, sizeof(Key_t), k);
		auto loc = hash_key % cardinality;

		auto version = (static_cast<node_t*>(this))->get_version(need_restart);
		if(need_restart) return -1;
		auto fingerprint = _hash(hash_key) | 1;
		for(int j=0; j<num_slot; j++){
		    loc = (loc + j) % cardinality;
		    std::unique_lock<std::shared_mutex> lock(mutex[loc]);

		    auto _version = (static_cast<node_t*>(this))->get_version(need_restart);
		    if(need_restart || (version != _version)){
			return -1;
		    }

		    for(int i=0; i<entry_num; i++){
			if((fingerprints[loc*entry_num+i] & 1) == 1){
			    if(fingerprints[loc*entry_num+i] == fingerprint){
				if(bucket[loc].entry[i].key == key){
				    bucket[loc].entry[i].value = value;
				    return 0;
				}
			    }
			}
		    }
		}
	    }

	    return 1; // key not found
	}

	uint64_t find_linear(Key_t key){
	    for(int k=0; k<hash_funcs_num; k++){
		auto hash_key = h(&key, sizeof(Key_t), k);
		auto fingerprint = _hash(hash_key) | 1;
restart:
		auto loc = hash_key % cardinality;

		for(int j=0; j<num_slot; j++){
		    loc = (loc + j) % cardinality;
		    std::shared_lock<std::shared_mutex> lock(mutex[loc]);

		    for(int i=0; i<entry_num; i++){
			if((fingerprints[loc*entry_num+i] & 1) == 1){
			    if(fingerprints[loc*entry_num+i] == fingerprint){
				if(bucket[loc].entry[i].key == key){
				    auto ret = bucket[loc].entry[i].value;
				    return ret;
				}
			    }
			}
		    }
		}
	    }

	    return 0;
	}

	uint64_t find_binary(Key_t key){
	    return 0;
	}

};
}
#endif
