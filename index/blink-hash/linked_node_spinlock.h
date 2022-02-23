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
#include <thread>
#include <x86intrin.h>

#include "hash.h"
#include "spinlock.h"
#include "threadinfo.h"

#define BITS_PER_LONG 64
#define BITOP_WORD(nr) ((nr) / BITS_PER_LONG)

#define PAGE_SIZE (512)

#define CACHELINE_SIZE 64

#define CAS(_p, _u, _v)  (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))

static void dummy(const char*, ...) {}

#define BLINK_DEBUG
#ifdef BLINK_DEBUG
#define blink_printf(fmt, ...) \
  do { \
    fprintf(stderr, "%-24s(%8lX): " fmt, \
            __FUNCTION__, \
            std::hash<std::thread::id>()(std::this_thread::get_id()), \
            ##__VA_ARGS__); \
    fflush(stdout); \
  } while (0);

#else

#define blink_printf(fmt, ...)   \
  do {                         \
    dummy(fmt, ##__VA_ARGS__); \
  } while (0);

#endif


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
	node_t(node_t* sibling, node_t* left, uint32_t count, uint32_t _level, bool): lock(0b100), sibling_ptr(sibling), leftmost_ptr(left), cnt(count), level(_level) { }

	void update_meta(node_t* sibling_ptr_, uint32_t level_){
	    lock = 0;
	    sibling_ptr = sibling_ptr_;
	    leftmost_ptr = nullptr;
	    cnt = 0;
	    level = level_;
	}

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

	void try_upgrade_splitlock(uint64_t& version, bool& need_restart){
	    if((version & 0b100)  == 0b100){
		need_restart = true;
		return;
	    }

	    if(!lock.compare_exchange_strong(version, version + 0b100)){
		_mm_pause();
		need_restart = true;
	    }
	}

	void write_unlock(){
	    lock.fetch_add(0b10);
	}

	void split_unlock(){
	    lock.fetch_add(0b100);
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
	static constexpr size_t cardinality = (PAGE_SIZE - sizeof(node_t)- sizeof(Key_t)) / sizeof(entry_t<Key_t, node_t*>);
	Key_t high_key;
    private:
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
	    void* mem;
	    auto ret = posix_memalign(&mem, 64, size);
	    return mem;
	}

	bool is_full(){
	    return (cnt == cardinality);
	}

	int find_lowerbound(Key_t& key){
	    return lowerbound_linear(key);
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

	#ifdef BREAKDOWN
	inode_t<Key_t>* split(Key_t& split_key, uint64_t& inode_alloc, uint64_t& inode_split){
	#else
	inode_t<Key_t>* split(Key_t& split_key){
	#endif

	    #ifdef BREAKDOWN
	    uint64_t start, end;
	    start = _rdtsc();
	    #endif
	    int half = cnt/2;
	    split_key = entry[half].key;

	    int new_cnt = cnt-half-1;
	    auto new_node = new inode_t<Key_t>(sibling_ptr, new_cnt, entry[half].value, level, high_key);
	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    inode_alloc += (end - start);
	    start = _rdtsc();
	    #endif

	    memcpy(new_node->entry, entry+half+1, sizeof(entry_t<Key_t, node_t*>)*new_cnt);

	    sibling_ptr = static_cast<node_t*>(new_node);
	    high_key = entry[half].key;
	    cnt = half;
	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    inode_split += (end - start);
	    #endif
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

//static constexpr size_t entry_num = 8;
static constexpr size_t entry_num = 32;

enum state_t{
    STABLE,
    LINKED_LEFT,
    LINKED_RIGHT,
};

template <typename Key_t, typename Value_t>
struct bucket_t{
    spinlock_t mutex;
    state_t state;
    uint8_t fingerprints[entry_num];
    entry_t<Key_t, Value_t> entry[entry_num];
};

static constexpr size_t seed = 0xc70697UL;
static constexpr int hash_funcs_num = 2;
static constexpr size_t num_slot = 4;

template <typename Key_t>
class lnode_t : public node_t{
    public: 
	static constexpr size_t leaf_size = 1024 * 256;
	static constexpr size_t cardinality = (leaf_size - sizeof(node_t) - sizeof(Key_t) - sizeof(lnode_t<Key_t>*))/ (sizeof(bucket_t<Key_t, uint64_t>));

	lnode_t<Key_t>* left_sibling_ptr;
	Key_t high_key;


    private:
	bucket_t<Key_t, uint64_t> bucket[cardinality];
    public:

	bool _try_splitlock(uint64_t version){
	    bool need_restart = false;
	    //try_upgrade_splitlock(version, need_restart);
	    try_upgrade_writelock(version, need_restart);
	    if(need_restart) return false;
	    for(int i=0; i<cardinality; i++){
		bucket[i].mutex.lock();
	    }
	    return true;
	}

	void _split_unlock(){
	    //split_unlock();
	    write_unlock();
	    for(int i=0; i<cardinality; i++){
		bucket[i].mutex.unlock();
	    }
	}

	void _split_release(){
	    for(int i=0; i<cardinality; i++){
		bucket[i].mutex.unlock_non_atomic();
	    }
	}

	void bucket_acquire(){
	    for(int i=0; i<cardinality; i++){
		bucket[i].mutex.lock_non_atomic();
	    }
	}

	void bucket_release(){
	    for(int i=0; i<cardinality; i++){
		bucket[i].mutex.unlock();
	    }
	}

	// initial constructor
        lnode_t(): node_t() { } 

	// constructor when leaf splits
        lnode_t(node_t* sibling, int _cnt, uint32_t _level): node_t(sibling, nullptr, 0, _level, true){ }

	void* operator new(size_t size) {
	    void* mem;
	    auto ret = posix_memalign(&mem, 64, size);
	    return mem;
	}

	bool is_full(){
	    return false;
	}

	int find_lowerbound(Key_t key){
	    return lowerbound_linear(key);
	}
	uint64_t find(Key_t key, bool& need_restart){
	    return find_linear(key, need_restart);
	}

	uint8_t _hash(size_t key){
	    return (uint8_t)(key % 256);
	}

	#ifdef BREAKDOWN
	int insert(Key_t key, uint64_t value, uint64_t version, uint64_t& lnode_traversal, uint64_t& lnode_write, uint64_t& lnode_sync){
	#else
	int insert(Key_t key, uint64_t value, uint64_t version){
	#endif
	    #ifdef BREAKDOWN
	    uint64_t traversal, write, sync;
	    traversal = write = sync = 0;
	    uint64_t start, end;
	    #endif
	    bool need_restart = false;
	    #ifdef AVX512
	    __m256i empty = _mm256_setzero_si256();
	    #elif defined AVX2
	    __m128i empty = _mm_setzero_si128();
	    #endif

	    for(int k=0; k<hash_funcs_num; k++){
		#ifdef BREAKDOWN
		start = _rdtsc();
		#endif
		auto hash_key = h(&key, sizeof(Key_t), k);
		auto loc = hash_key % cardinality;
		auto fingerprint = _hash(hash_key) | 1;
		for(int j=0; j<num_slot; j++){
		    loc = (loc + j) % cardinality;
		    #ifdef BREAKDOWN 
		    end = _rdtsc();
		    traversal += (end - start);
		    start = _rdtsc();
		    #endif
		    bucket[loc].mutex.lock();
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    sync += (end - start);
		    start = _rdtsc();
		    #endif
		    if(bucket[loc].state == LINKED_LEFT){
			#ifdef BREAKDOWN
			end = _rdtsc();
			traversal += (end - start);
			auto ret = stabilize_bucket_left(loc, true, traversal, sync, write);
			start = _rdtsc();
			#else
			auto ret = stabilize_bucket_left(loc, true);
			#endif
			if(!ret){
			    bucket[loc].mutex.unlock();
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    sync += (end - start);
			    lnode_sync += sync;
			    lnode_traversal += traversal;
			    lnode_write += write;
			    #endif
			    return -1;
			}
		    }
		    else if(bucket[loc].state == LINKED_RIGHT){
			#ifdef BREAKDOWN
			auto ret = stabilize_bucket_right(loc, true, traversal, sync, write);
			#else
			auto ret = stabilize_bucket_right(loc, true);
			#endif
			if(!ret){
			    #ifdef BREAKDOWN
			    start = _rdtsc();
			    #endif
			    bucket[loc].mutex.unlock();
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    sync += (end - start);
			    lnode_sync += sync;
			    lnode_traversal += traversal;
			    lnode_write += write;
			    #endif
			    return -1;
			}
		    }
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    traversal += (end - start);
		    start = _rdtsc();
		    #endif
		    auto _version = get_version(need_restart);
		    if(need_restart || (version != _version)){
			bucket[loc].mutex.unlock();
			#ifdef BREAKDOWN
			end = _rdtsc();
			sync += (end - start);
			lnode_sync += sync;
			lnode_traversal += traversal;
			#endif
			return -1;
		    }
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    sync += (end - start);
		    start = _rdtsc();
		    #endif

		#ifdef AVX512
		    __m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(bucket[loc].fingerprints));
		    __m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
		    uint32_t bitfield = _mm256_movemask_epi8(cmp);
		    for(int i=0; i<32; i++){
			auto bit = (bitfield >> i);
			if((bit & 0x1) == 1){
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    traversal += (end - start);
			    start = _rdtsc();
			    #endif
			    bucket[loc].fingerprints[i] = fingerprint;
			    bucket[loc].entry[i].key = key;
			    bucket[loc].entry[i].value = value;
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    write += (end - start);
			    start = _rdtsc();
			    #endif
			    bucket[loc].mutex.unlock();
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    sync += (end - start);
			    lnode_write += write;
			    lnode_sync += sync;
			    lnode_traversal += traversal;
			    #endif
			    return 0;
			}
		    }
		#elif defined AVX2
		    for(int m=0; m<2; m++){
			__m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[loc].fingerprints + m*16));
			__m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
			uint16_t bitfield = _mm_movemask_epi8(cmp);
			for(int i=0; i<16; i++){
			    auto bit = (bitfield >> i);
			    if((bit & 0x1) == 1){
				auto idx = m*16 + i;
			        #ifdef BREAKDOWN
				end = _rdtsc();
				traversal += (end - start);
				start = _rdtsc();
			        #endif
				bucket[loc].fingerprints[idx] = fingerprint;
				bucket[loc].entry[idx].key = key;
				bucket[loc].entry[idx].value = value;
			        #ifdef BREAKDOWN
				end = _rdtsc();
				write = (end - start);
				start = _rdtsc();
			        #endif
				bucket[loc].mutex.unlock();
			        #ifdef BREAKDOWN
				end = _rdtsc();
				sync += (end - start);
				lnode_write += write;
				lnode_sync += sync;
				lnode_traversal += traversal;
			        #endif
				return 0;
			    }
			}
		    }
		#else
		    for(int i=0; i<entry_num; i++){
			if(bucket[loc].fingerprints[i] == 0){
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    traversal += (end - start);
			    start = _rdtsc();
			    #endif
			    bucket[loc].fingerprints[i] = fingerprint;
			    bucket[loc].entry[i].key = key;
			    bucket[loc].entry[i].value = value;
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    write = (end - start);
			    start = _rdtsc();
			    #endif
			    bucket[loc].mutex.unlock();
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    sync += (end - start);
			    lnode_write += write;
			    lnode_sync += sync;
			    lnode_traversal += traversal;
			    #endif
			    return 0;
			}
		    }
		#endif
		    #ifdef BREAKDOWN
		    start = _rdtsc();
		    #endif
		    bucket[loc].mutex.unlock();
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    sync += (end - start);
		    start = _rdtsc();
		    #endif
		}
	    }
	    #ifdef BREAKDOWN 
	    end = _rdtsc();
	    traversal += (end - start);
	    lnode_sync += sync;
	    lnode_traversal += traversal;
	    #endif

	    return 1; // return split flag
	}
	    
	#ifdef BREAKDOWN
	#ifdef THREAD_ALLOC
	lnode_t<Key_t>* split(Key_t& split_key, uint64_t version, threadinfo* ti, uint64_t& lnode_alloc, uint64_t& lnode_sync, uint64_t& lnode_split, uint64_t& lnode_key_copy, uint64_t& lnode_find_median, uint64_t& lnode_copy, uint64_t& lnode_update){
	#else
	lnode_t<Key_t>* split(Key_t& split_key, Key_t key, uint64_t value, uint64_t version, uint64_t& lnode_alloc, uint64_t& lnode_sync, uint64_t& lnode_split, uint64_t& lnode_key_copy, uint64_t& lnode_find_median, uint64_t& lnode_copy, uint64_t& lnode_update, uint64_t& lnode_traversal, uint64_t& lnode_write){
	//lnode_t<Key_t>* split(Key_t& split_key, uint64_t version, uint64_t& lnode_alloc, uint64_t& lnode_sync, uint64_t& lnode_split, uint64_t& lnode_key_copy, uint64_t& lnode_find_median, uint64_t& lnode_copy, uint64_t& lnode_update){
	#endif
	//lnode_t<Key_t>* split(Key_t& split_key, uint64_t version, uint64_t& lnode_alloc, uint64_t& lnode_sync, uint64_t& lnode_split, uint64_t& lnode_find_median, uint64_t& lnode_copy, uint64_t& lnode_update){
	//lnode_t<Key_t>* split(Key_t& split_key, uint64_t version, uint64_t& lnode_alloc, uint64_t& lnode_sync, uint64_t& lnode_split){
	#else
	#ifdef THREAD_ALLOC
	lnode_t<Key_t>* split(Key_t& split_key, uint64_t version, threadinfo* ti){ 
	#else
	lnode_t<Key_t>* split(Key_t& split_key, Key_t key, uint64_t value, uint64_t version){ 
	//lnode_t<Key_t>* split(Key_t& split_key, uint64_t version){ 
	#endif
	#endif

	    #ifdef BREAKDOWN
	    uint64_t start, end;
	    start = _rdtsc();
	    #endif
	    #ifdef THREAD_ALLOC
	    auto new_right = static_cast<lnode_t<Key_t>*>(ti->allocate(sizeof(lnode_t<Key_t>)));
	    new_right->update_meta(sibling_ptr, level);
	    //new_right->high_key = high_key;
	    for(int i=0; i<cardinality; i++){
		new_right->bucket[i].fingerprints[0] = 0;
	    }
	    #else
	    auto new_right = new lnode_t<Key_t>(sibling_ptr, 0, level);
	    #endif
	    new_right->high_key = high_key;
	    new_right->left_sibling_ptr = this;
	    for(int i=0; i<cardinality; i++){
		new_right->bucket[i].state = LINKED_LEFT;
	    }
	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    lnode_alloc += (end - start);
	    start = _rdtsc();
	    #endif
	    struct target_t{
		uint64_t loc;
		uint64_t fingerprint;
	    };
	    target_t target[hash_funcs_num];
	    for(int k=0; k<hash_funcs_num; k++){
		auto hash_key = h(&key, sizeof(Key_t), k);
		target[k].loc = hash_key % cardinality;
		target[k].fingerprint = (_hash(hash_key) | 1);
	    }
	    bool need_insert = true;
	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    lnode_traversal += (end - start);
	    start = _rdtsc();
	    #endif

	    if(!_try_splitlock(version)){
	        #ifdef BREAKDOWN
		end = _rdtsc();
		lnode_sync += (end - start);
		start = _rdtsc();
	        #endif
		#ifdef THREAD_ALLOC
		ti->deallocate(static_cast<void*>(new_right));
		#else
		delete new_right;
		#endif
	        #ifdef BREAKDOWN
		end = _rdtsc();
		lnode_alloc += (end - start);
	        #endif
		return nullptr;
	    }
	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    lnode_sync += (end - start);
	    start = _rdtsc();
	    #endif

	    /*
	    auto util = utilization() * 100;
	    std::cout << util << std::endl; 
	    */

	    #ifdef AVX512
	    __m256i empty = _mm256_setzero_si256();
	    #elif defined AVX2
	    __m128i empty = _mm_setzero_si128();
	    #endif

	    #ifdef ENTRY_SAMPLING // entry-based sampling
	    Key_t temp[cardinality];
	    int valid_num = 0;
	    for(int j=0; j<cardinality; j++){
		if(bucket[j].state == LINKED_LEFT){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    lnode_key_copy += (end - start);
		    #endif
		    bool ret;
		    #ifdef BREAKDOWN
		    while(!(ret = stabilize_bucket_left(j, true, lnode_traversal, lnode_sync, lnode_write))){
			_mm_pause();
		    }
		    start = _rdtsc();
		    #else
		    while(!(ret = stabilize_bucket_left(j, true))){
			_mm_pause();
		    }
		    #endif
		}
		else if(bucket[j].state == LINKED_RIGHT) // lazy buckets decreases the accuracy of median key prediction
		    continue;

	        #ifdef AVX512
		__m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(bucket[j].fingerprints));
		__m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
		uint32_t bitfield = _mm256_movemask_epi8(cmp);
		for(int i=0; i<32; i++){
		    auto bit = (bitfield >> i);
		    if((bit & 0x1) == 0){
			temp[valid_num++] = bucket[j].entry[i].key;
			if(valid_num == cardinality)
			    goto FIND_MEDIAN;
		    }
		}
		#elif defined AVX2
		for(int k=0; k<2; k++){
		    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[j].fingerprints + k*16));
		    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		    uint16_t bitfield = _mm_movemask_epi8(cmp);
		    for(int i=0; i<16; i++){
			auto bit = (bitfield >> i);
			if((bit & 0x1) == 0){
			    auto idx = k*16 + i;
			    temp[valid_num++] = bucket[j].entry[idx].key;
			    if(valid_num == cardinality)
				goto FIND_MEDIAN;
			}
		    }
		}
		#else
		for(int i=0; i<entry_num; i++){
		    if(bucket[j].fingerprints[i] != 0){
			temp[valid_num++] = bucket[j].entry[i].key;
			if(valid_num == cardinality)
			    goto FIND_MEDIAN;
		    }
		}
		#endif
	    }
FIND_MEDIAN:
	    #else // non-sampling
	    Key_t temp[cardinality*entry_num];
	    int valid_num = 0;
	    for(int j=0; j<cardinality; j++){
	        #ifdef AVX512
		__m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(bucket[j].fingerprints));
		__m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
		uint32_t bitfield = _mm256_movemask_epi8(cmp);
		for(int i=0; i<32; i++){
		    auto bit = (bitfield >> i);
		    if((bit & 0x1) == 0)
			temp[valid_num++] = bucket[j].entry[i].key;
		}
		#elif defined AVX2
		for(int k=0; k<2; k++){
		    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[j].fingerprints + k*16));
		    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		    uint16_t bitfield = _mm_movemask_epi8(cmp);
		    for(int i=0; i<16; i++){
			auto bit = (bitfield >> i);
			if((bit & 0x1) == 0){
			    auto idx = k*16 + i;
			    temp[valid_num++] = bucket[j].entry[idx].key;
			}
		    }
		}
		#else
		for(int i=0; i<entry_num; i++){
		    if(bucket[j].fingerprints[i] != 0){
			temp[valid_num++] = bucket[j].entry[i].key;
		    }
		}
		#endif
	    }

	    #endif
	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    lnode_key_copy += (end - start);
	    start = _rdtsc();
	    #endif

	    int half = find_median(temp, valid_num);
	    split_key = temp[half];
	    high_key = temp[half];
//#define DEBUG
#ifdef DEBUG // to find out the error rate of finding median key for sampling-based approaches
	    Key_t temp_[cardinality*entry_num];
	    int valid_num_ = 0;
	    for(int j=0; j<cardinality; j++){
		for(int i=0; i<entry_num; i++){
		    if(bucket[j].fingerprints[i] != 0){
			temp_[valid_num_++] = bucket[j].entry[i].key;
		    }
		}
	    }
	    std::sort(temp_, temp_+valid_num_);
	    int sampled_median_loc;
	    for(int i=0; i<valid_num_; i++){
		if(temp_[i] == split_key){
		    sampled_median_loc = i;
		    break;
		}
	    }

	    std::cout << (double)(sampled_median_loc - valid_num_/2)/valid_num_ * 100 << std::endl;
#endif
	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    lnode_find_median += (end - start);
	    uint64_t _start, lnode_copy_ = 0;
	    start = _rdtsc();
	    #endif

	    for(int j=0; j<cardinality; j++){
		if(bucket[j].state == LINKED_LEFT){
		    bool ret;
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    lnode_update += (end - start);
		    while(!(ret = stabilize_bucket_left(j, true, lnode_traversal, lnode_sync, lnode_write))){
			_mm_pause();
		    }
		    start = _rdtsc();
		    #else
		    while(!(ret = stabilize_bucket_left(j, true))){
			_mm_pause();
		    }
		    #endif
		}

		#ifdef AVX512
		__m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(bucket[j].fingerprints));
		__m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
		uint32_t bitfield = _mm256_movemask_epi8(cmp);
		for(int i=0; i<32; i++){
		    auto bit = (bitfield >> i);
		    if((bit & 0x1) == 0){
			if(split_key < bucket[j].entry[i].key){
			    bucket[j].state = LINKED_RIGHT;
			    //new_right->bucket[j].state = LINKED_LEFT;
			    goto INSERT_CHECK;
			}
		    }
		}
		#elif defined AVX2
		for(int k=0; k<2; k++){
		    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[j].fingerprints + k*16));
		    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		    uint16_t bitfield = _mm_movemask_epi8(cmp);
		    for(int i=0; i<16; i++){
			auto bit = (bitfield >> i);
			if((bit & 0x1) == 0){
			    auto idx = k*16 + i;
			    if(split_key < bucket[j].entry[idx].key){
				bucket[j].state = LINKED_RIGHT;
				//new_right->bucket[j].state = LINKED_LEFT;
				goto INSERT_CHECK;
			    }
			}
		    }
		}
		#else
		for(int i=0; i<entry_num; i++){
		    if(bucket[j].fingerprints[i] != 0){
			if(split_key < bucket[j].entry[i].key){
			    bucket[j].state = LINKED_RIGHT;
			    //new_right->bucket[j].state = LINKED_LEFT;
			    goto INSERT_CHECK;
			}
		    }
		}
		#endif
		new_right->bucket[j].state = STABLE;
	    INSERT_CHECK:
		if(need_insert){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    lnode_update += (end - start);
		    start = _rdtsc();
		    #endif
		    for(int m=0; m<hash_funcs_num; m++){
			if(j == target[m].loc){
			    if(split_key < key){
				if(new_right->bucket[j].state == LINKED_LEFT){
				    bool ret;
				    #ifdef BREAKDOWN
				    end = _rdtsc();
				    lnode_traversal += (end - start);
				    while(!(ret = new_right->stabilize_bucket_left(j, false, lnode_traversal, lnode_sync, lnode_write))){
					_mm_pause();
				    }
				    start = _rdtsc();
				    #else
				    while(!(ret = new_right->stabilize_bucket_left(j, false))){
					_mm_pause();
				    }
				    #endif
				}

				#ifdef AVX512
				__m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(new_right->bucket[j].fingerprints));
				__m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
				uint32_t bitfield = _mm256_movemask_epi8(cmp);
				for(int i=0; i<32; i++){
				    auto bit = (bitfield >> i);
				    if((bit & 0x1) == 1){
					new_right->bucket[j].fingerprints[i] = target[m].fingerprint;
					new_right->bucket[j].entry[i].key = key;
					new_right->bucket[j].entry[i].value = value;
					need_insert = false;
					goto PROCEED;
				    }
				}
				#elif defined AVX2
				for(int k=0; k<2; k++){
				    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(new_right->bucket[j].fingerprints + k*16));
				    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
				    uint16_t bitfield = _mm_movemask_epi8(cmp);
				    for(int i=0; i<16; i++){
					auto bit = (bitfield >> i);
					if((bit & 0x1) == 1){
					    #ifdef BREAKDOWN
					    end = _rdtsc();
					    lnode_traversal += (end - start);
					    start = _rdtsc();
					    #endif
					    auto idx = k*16 + i;
					    new_right->bucket[j].fingerprints[idx] = target[m].fingerprint;
					    new_right->bucket[j].entry[idx].key = key;
					    new_right->bucket[j].entry[idx].value = value;
					    need_insert = false;
					    #ifdef BREAKDOWN
					    end = _rdtsc();
					    lnode_write += (end - start);
					    start = _rdtsc();
					    #endif
					    goto PROCEED;
					}
				    }
				}
				#else
				for(int i=0; i<entry_num; i++){
				    if(new_right->bucket[j].fingerprints[i] == 0){
					new_right->bucket[j].fingerprints[i] = target[m].fingerprint;
					new_right->bucket[j].entry[i].key = key;
					new_right->bucket[j].entry[i].value = value;
					need_insert = false;
					goto PROCEED;
				    }
				}
				#endif
			    }
			    else{
				if(bucket[j].state == LINKED_RIGHT){
				    state_t state = STABLE;
				    #ifdef AVX512
				    __m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(bucket[j].fingerprints));
				    __m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
				    uint32_t bitfield = _mm256_movemask_epi8(cmp);
				    for(int i=0; i<32; i++){
					auto bit = (bitfield >> i);
					if((bit & 0x1) == 0){
					    if(high_key < bucket[j].entry[i].key){
						new_right->bucket[j].fingerprints[i] = bucket[j].fingerprints[i];
						memcpy(&new_right->bucket[j].entry[i], &bucket[j].entry[i], sizeof(entry_t<Key_t, uint64_t>));
						bucket[j].fingerprints[i] = 0;
						if(state == STABLE){
						    if(sibling_ptr != nullptr){
							if(new_right->high_key < new_right->bucket[j].entry[i].key){
							    state = LINKED_RIGHT;
							}
						    }
						}
					    }
					}
				    }
				    #elif defined AVX2
				    for(int k=0; k<2; k++){
					__m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[j].fingerprints + k*16));
					__m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
					uint16_t bitfield = _mm_movemask_epi8(cmp);
					for(int i=0; i<16; i++){
					    auto bit = (bitfield >> i);
					    if((bit & 0x1) == 0){
						auto idx = k*16 + i;
						if(high_key < bucket[j].entry[idx].key){
					            #ifdef BREAKDOWN
						    end = _rdtsc();
						    lnode_traversal += (end - start);
						    start = _rdtsc();
					            #endif
						    new_right->bucket[j].fingerprints[idx] = bucket[j].fingerprints[idx];
						    memcpy(&new_right->bucket[j].entry[idx], &bucket[j].entry[idx], sizeof(entry_t<Key_t, uint64_t>));
						    bucket[j].fingerprints[idx] = 0;
						    if(state == STABLE){
							if(sibling_ptr != nullptr){
							    if(new_right->high_key < new_right->bucket[j].entry[idx].key){
								state = LINKED_RIGHT;
							    }
							}
						    }
						    #ifdef BREAKDOWN
						    end = _rdtsc();
						    lnode_write += (end - start);
						    start = _rdtsc();
						    #endif
						}
					    }
					}
				    }
				    #else
				    for(int i=0; i<entry_num; i++){
					if(bucket[j].fingerprints[i] != 0){
					    if(high_key < bucket[j].entry[i].key){
						new_right->bucket[j].fingerprints[i] = bucket[j].fingerprints[i];
						memcpy(&new_right->bucket[j].entry[i], &bucket[j].entry[i], sizeof(entry_t<Key_t, uint64_t>));
						bucket[j].fingerprints[i] = 0;
						if(state == STABLE){
						    if(sibling_ptr != nullptr){
							if(new_right->high_key < new_right->bucket[j].entry[i].key){
							    state = LINKED_RIGHT;
							}
						    }
						}
					    }
					}
				    }
				    #endif
				    #ifdef BREAKDOWN
				    end = _rdtsc();
				    lnode_traversal += (end - start);
				    start = _rdtsc();
				    #endif
				    new_right->bucket[j].state = state;
				    bucket[j].state = STABLE;
				    #ifdef BREAKDOWN
				    end = _rdtsc();
				    lnode_write += (end - start);
				    start = _rdtsc();
				    #endif
				}

				#ifdef AVX512
				__m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(bucket[j].fingerprints));
				__m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
				uint32_t bitfield = _mm256_movemask_epi8(cmp);
				for(int i=0; i<32; i++){
				    auto bit = (bitfield >> i);
				    if((bit & 0x1) == 1){
					bucket[j].fingerprints[i] = target[m].fingerprint;
					bucket[j].entry[i].key = key;
					bucket[j].entry[i].value = value;
					need_insert = false;
					goto PROCEED;
				    }
				}
				#elif defined AVX2
				for(int k=0; k<2; k++){
				    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[j].fingerprints + k*16));
				    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
				    uint16_t bitfield = _mm_movemask_epi8(cmp);
				    for(int i=0; i<16; i++){
					auto bit = (bitfield >> i);
					if((bit & 0x1) == 1){
					    #ifdef BREAKDOWN
					    end = _rdtsc();
					    lnode_traversal += (end - start);
					    start = _rdtsc();
					    #endif
					    auto idx = k*16 + i;
					    bucket[j].fingerprints[idx] = target[m].fingerprint;
					    bucket[j].entry[idx].key = key;
					    bucket[j].entry[idx].value = value;
					    need_insert = false;
					    #ifdef BREAKDOWN
					    end = _rdtsc();
					    lnode_write += (end - start);
					    start = _rdtsc();
					    #endif
					    goto PROCEED;
					}
				    }
				}
				#else
				for(int i=0; i<entry_num; i++){
				    if(bucket[j].fingerprints[i] == 0){
					bucket[j].fingerprints[i] = target[m].fingerprint;
					bucket[j].entry[i].key = key;
					bucket[j].entry[i].value = value;
					need_insert = false;
					goto PROCEED;
				    }
				}
				#endif
			    }
			}
		    }
		}
		#ifdef BREAKDOWN
		end = _rdtsc();
		lnode_update += (end - start);
		start = _rdtsc();
		#endif
	    PROCEED:
		(void) need_insert;
	    }
	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    lnode_traversal += (end - start);
	    start = _rdtsc();
	    #endif
	RETRY:
	    if(sibling_ptr != nullptr){
		auto right_sibling = static_cast<lnode_t<Key_t>*>(sibling_ptr);
		auto old = this;
		while(!CAS(&right_sibling->left_sibling_ptr, &old, new_right)){
		    old = this;
		}
	    }
	    sibling_ptr = new_right;

	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    lnode_update += (end - start);
	    #endif


	    if(need_insert){
		blink_printf("%s %s %llu\n", __func__, ": insert after split failed key- ", key);
	    }
	    /*
	    util = utilization() * 100;
	    std::cout << util << std::endl;
	    */
	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    lnode_split += (end - start);
	    #endif
	    return new_right;
	}


	int update(Key_t key, uint64_t value){
	    return update_linear(key, value);
	}

	int range_lookup(Key_t key, uint64_t* buf, int count, int range){
	    bool need_restart = false;

	    entry_t<Key_t, uint64_t> _buf[cardinality];
	    int _count = count;
	    int idx = 0;

	    #ifdef AVX512
	    __m256i empty = _mm256_setzero_si256();
	    #elif defined AVX2
	    __m128i empty = _mm_setzero_si128();
	    #endif

	    for(int j=0; j<num_slot; j++){
		bucket[j].mutex.lock();

		if(bucket[j].state == LINKED_LEFT){
		    #ifndef BREAKDOWN
		    auto ret = stabilize_bucket_left(j, true);
		    if(!ret){
			bucket[j].mutex.unlock();
			return -1;
		    }
		    #endif
		}
		else if(bucket[j].state == LINKED_RIGHT){
		    #ifndef BREAKDOWN
		    auto ret = stabilize_bucket_right(j, true);
		    if(!ret){
			bucket[j].mutex.unlock();
			return -1;
		    }
		    #endif
		}

	        #ifdef AVX512
		__m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(bucket[j].fingerprints));
		__m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
		uint32_t bitfield = _mm256_movemask_epi8(cmp);
		for(int i=0; i<32; i++){
		    auto bit = (bitfield >> i);
		    if((bit & 0x1) == 0){
			if(bucket[j].entry[i].key >= key){
			    _buf[idx].key = bucket[j].entry[i].key;
			    _buf[idx].value = bucket[j].entry[i].value;
			    idx++;
			}
		    }
		}
		#elif defined AVX2
		for(int m=0; m<2; m++){
		    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[j].fingerprints + m*16));
		    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		    uint16_t bitfield = _mm_movemask_epi8(cmp);
		    for(int i=0; i<16; i++){
			auto bit = (bitfield >> i);
			if((bit & 0x1) == 0){
			    auto idx_ = m*16 + i;
			    if(bucket[j].entry[idx_].key >= key){
				_buf[idx].key = bucket[j].entry[idx_].key;
				_buf[idx].value = bucket[j].entry[idx_].value;
				idx++;
			    }
			}
		    }
		}
		#else
		for(int i=0; i<entry_num; i++){
		    if(bucket[j].fingerprints[i] != 0){
			if(bucket[j].entry[i].key >= key){
			    _buf[idx].key = bucket[j].entry[i].key;
			    _buf[idx].value = bucket[j].entry[i].value;
			    idx++;
			}
		    }
		}
		#endif
		bucket[j].mutex.unlock();
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
	    std::cout << "left_sibling: " << left_sibling_ptr << std::endl;
	    for(int i=0; i<cardinality; i++){
		for(int j=0; j<entry_num; j++){
		    if(j % 4 == 0) std::cout <<"\n";
		    std::cout << "[" << i << "][" << j << "] " << "finger(" << (int)bucket[i].fingerprints[j] << ") key(" << bucket[i].entry[j].key << "),  ";
		}
		std::cout << "\n";
	    }
	    std::cout << "right_sibling: " << sibling_ptr << std::endl;
	    std::cout << "node_high_key: " << high_key << "\n\n";
	}

	void sanity_check(Key_t _high_key, bool first){
	}

	double utilization(){
	    int cnt = 0;
	    for(int j=0; j<cardinality; j++){
		for(int i=0; i<entry_num; i++){
		    if(bucket[j].fingerprints[i] != 0)
			cnt++;
		}
	    }
	    return (double)cnt/(cardinality*entry_num);
	}


    private:
   	#ifdef BREAKDOWN
	bool stabilize_bucket_left(int bucket_idx, bool is_normal, uint64_t& traversal, uint64_t& sync, uint64_t& write){
	#else
	bool stabilize_bucket_left(int bucket_idx, bool is_normal){
	#endif
	    #ifdef BREAKDOWN
	    uint64_t start, end;
	    start = _rdtsc();
	    #endif
	RETRY:
	    bool need_restart = false;
	    lnode_t<Key_t>* left = left_sibling_ptr;
	    bucket_t<Key_t, uint64_t>* left_bucket = &left->bucket[bucket_idx];

	    if(is_normal){
		#ifdef BREAKDOWN
		end = _rdtsc();
		traversal += (end - start);
		start = _rdtsc();
		#endif
		auto left_vstart = left->get_version(need_restart);
		#ifdef BREAKDOWN
		end = _rdtsc();
		sync += (end - start);
		start = _rdtsc();
		#endif
		if(need_restart)
		    goto RETRY;

		if(!left_bucket->mutex.try_lock()){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    sync += (end - start);
		    #endif
		    return false;
		}

		auto left_vend = left->get_version(need_restart);
		if(need_restart || (left_vstart != left_vend)){
		    left_bucket->mutex.unlock();
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    sync += (end - start);
		    #endif
		    return false;
		}
		#ifdef BREAKDOWN
		end = _rdtsc();
		sync += (end - start);
		start = _rdtsc();
		#endif
	    }

	    if(left_bucket->state == LINKED_RIGHT){
		state_t state = STABLE;
		#ifdef AVX512
		__m256i empty = _mm256_setzero_si256();
		__m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(left_bucket->fingerprints));
		__m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
		uint32_t bitfield  = _mm256_movemask_epi8(cmp);
		for(int i=0; i<32; i++){
		    auto bit = (bitfield >> i);
		    if((bit & 0x1) == 0){
			if(left->high_key < left_bucket->entry[i].key){
			    bucket[bucket_idx].fingerprints[i] = left_bucket->fingerprints[i];
			    memcpy(&bucket[bucket_idx].entry[i], &left_bucket->entry[i], sizeof(entry_t<Key_t, uint64_t>));
			    left_bucket->fingerprints[i] = 0;
			    if(state == STABLE){
				if(sibling_ptr != nullptr){
				    if(high_key < bucket[bucket_idx].entry[i].key){
					state = LINKED_RIGHT;
				    }
				}
			    }
			}
		    }
		}
		#elif defined AVX2
		__m128i empty = _mm_setzero_si128();
		for(int m=0; m<2; m++){
		    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(left_bucket->fingerprints + m*16));
		    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		    uint16_t bitfield = _mm_movemask_epi8(cmp);
		    for(int i=0; i<16; i++){
			auto bit = (bitfield >> i);
			if((bit & 0x1) == 0){
			    auto idx = m*16 + i;
			    if(left->high_key < left_bucket->entry[idx].key){
			        #ifdef BREAKDOWN
				end = _rdtsc();
				traversal += (end - start);
				start = _rdtsc();
			        #endif
				bucket[bucket_idx].fingerprints[idx] = left_bucket->fingerprints[idx];
				memcpy(&bucket[bucket_idx].entry[idx], &left_bucket->entry[idx], sizeof(entry_t<Key_t, uint64_t>));
				left_bucket->fingerprints[idx] = 0;
				if(state == STABLE){
				    if(sibling_ptr != nullptr){
					if(high_key < bucket[bucket_idx].entry[idx].key){
					    state = LINKED_RIGHT;
					}
				    }
				}
			        #ifdef BREAKDOWN
				end = _rdtsc();
				write += (end - start);
				start = _rdtsc();
			        #endif
			    }
			}
		    }
		}
		#else
		for(int i=0; i<entry_num; i++){
		    if(left_bucket->fingerprints[i] != 0){
			if(left->high_key < left_bucket->entry[i].key){
			    bucket[bucket_idx].fingerprints[i] = left_bucket->fingerprints[i];
			    memcpy(&bucket[bucket_idx].entry[i], &left_bucket->entry[i], sizeof(entry_t<Key_t, uint64_t>));
			    left_bucket->fingerprints[i] = 0;
			    if(state == STABLE){
				if(sibling_ptr != nullptr){
				    if(high_key < bucket[bucket_idx].entry[i].key){
					state = LINKED_RIGHT;
				    }
				}
			    }
			}
		    }
		}
		#endif
		#ifdef BREAKDOWN
		end = _rdtsc();
		traversal += (end - start);
		start = _rdtsc();
		#endif
		left_bucket->state = STABLE;
		#ifdef BREAKDOWN
		end = _rdtsc();
		write += (end - start);
		start = _rdtsc();
		#endif
		if(is_normal){
		    left_bucket->mutex.unlock();
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    sync += (end - start);
		    start = _rdtsc();
		    #endif
		}
		bucket[bucket_idx].state = state;
		#ifdef BREAKDOWN
		end = _rdtsc();
		write += (end - start);
		#endif
		return true;
	    }
	    else if(left_bucket->state == LINKED_LEFT){
		#ifdef BREAKDOWN
		end = _rdtsc();
		traversal += (end - start);
		auto ret = left->stabilize_bucket_left(bucket_idx, true, traversal, sync, write);
		start = _rdtsc();
		#else
		auto ret = left->stabilize_bucket_left(bucket_idx, true);
		#endif
		if(!ret){
		    left_bucket->mutex.unlock();
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    sync += (end - start);
		    #endif
		    return false;
		}

		state_t state = STABLE;
		#ifdef AVX512
		__m256i empty = _mm256_setzero_si256();
		__m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(left_bucket->fingerprints));
		__m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
		uint32_t bitfield = _mm256_movemask_epi8(cmp);
		for(int i=0; i<32; i++){
		    auto bit = (bitfield >> i);
		    if((bit & 0x1) == 0){
			if(left->high_key < left_bucket->entry[i].key){
			    bucket[bucket_idx].fingerprints[i] = left_bucket->fingerprints[i];
			    memcpy(&bucket[bucket_idx].entry[i], &left_bucket->entry[i], sizeof(entry_t<Key_t, uint64_t>));
			    left_bucket->fingerprints[i] = 0;
			    if(state == STABLE){
				if(sibling_ptr != nullptr){
				    if(high_key < bucket[bucket_idx].entry[i].key){
					state = LINKED_RIGHT;
				    }
				}
			    }
			}
		    }
		}
		#elif defined AVX2
		__m128i empty = _mm_setzero_si128();
		for(int m=0; m<2; m++){
		    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(left_bucket->fingerprints + m*16));
		    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		    uint16_t bitfield = _mm_movemask_epi8(cmp);
		    for(int i=0; i<16; i++){
			auto bit = (bitfield >> i);
			if((bit & 0x1) == 0){
			    auto idx = m*16 + i;
			    if(left->high_key < left_bucket->entry[idx].key){
			        #ifdef BREAKDOWN
				end = _rdtsc();
				traversal += (end - start);
				start = _rdtsc();
			        #endif
				bucket[bucket_idx].fingerprints[idx] = left_bucket->fingerprints[idx];
				memcpy(&bucket[bucket_idx].entry[idx], &left_bucket->entry[idx], sizeof(entry_t<Key_t, uint64_t>));
				left_bucket->fingerprints[idx] = 0;
				if(state == STABLE){
				    if(sibling_ptr != nullptr){
					if(high_key < bucket[bucket_idx].entry[idx].key){
					    state = LINKED_RIGHT;
					}
				    }
				}
			        #ifdef BREAKDOWN
				end = _rdtsc();
				write += (end - start);
				start = _rdtsc();
		 	        #endif
			    }
			}
		    }
		}
		#else
		for(int i=0; i<entry_num; i++){
		    if(left_bucket->fingerprints[i] != 0){
			if(left->high_key < left_bucket->entry[i].key){
			    bucket[bucket_idx].fingerprints[i] = left_bucket->fingerprints[i];
			    memcpy(&bucket[bucket_idx].entry[i], &left_bucket->entry[i], sizeof(entry_t<Key_t, uint64_t>));
			    left_bucket->fingerprints[i] = 0;
			    if(state == STABLE){
				if(sibling_ptr != nullptr){
				    if(high_key < bucket[bucket_idx].entry[i].key){
					state = LINKED_RIGHT;
				    }
				}
			    }
			}
		    }
		}
		#endif
		#ifdef BREAKDOWN
		end = _rdtsc();
		traversal += (end - start);
		start = _rdtsc();
		#endif
		left_bucket->state = STABLE;
		#ifdef BREAKDOWN
		end = _rdtsc();
		write += (end - start);
		start = _rdtsc();
		#endif
		left_bucket->mutex.unlock();
		#ifdef BREAKDOWN
		end = _rdtsc();
		sync += (end - start);
		start = _rdtsc();
		#endif
		bucket[bucket_idx].state = state;
		#ifdef BREAKDOWN
		end = _rdtsc();
		write += (end - start);
		#endif
		return true;
	    }
	    else{
		blink_printf("%s: incorrect state: %d\n", __func__, left_bucket->state);
	    }
	}
 	#ifdef BREAKDOWN
	bool stabilize_bucket_right(int bucket_idx, bool is_normal, uint64_t& traversal, uint64_t& sync, uint64_t& write){
	#else
	bool stabilize_bucket_right(int bucket_idx, bool is_normal){
	#endif
	    #ifdef BREAKDOWN
	    uint64_t start, end;
	    start = _rdtsc();
	    #endif
	RETRY:
	    lnode_t<Key_t>* right = static_cast<lnode_t<Key_t>*>(sibling_ptr);
	    bucket_t<Key_t, uint64_t>* right_bucket = &right->bucket[bucket_idx];

	    bool need_restart = false;
	    if(is_normal){
		#ifdef BREAKDOWN
		end = _rdtsc();
		traversal += (end - start);
		start = _rdtsc();
		#endif
		auto right_vstart = right->get_version(need_restart);
		if(need_restart){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    sync += (end - start);
		    #endif
		    goto RETRY;
		}

		if(!right_bucket->mutex.try_lock()){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    sync += (end - start);
		    #endif
		    return false;
		}

		auto right_vend = right->get_version(need_restart);
		if(need_restart || (right_vstart != right_vend)){
		    right_bucket->mutex.unlock();
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    sync += (end - start);
		    #endif
		    return false;
		}
		#ifdef BREAKDOWN
		end = _rdtsc();
		sync += (end - start);
		start = _rdtsc();
		#endif
	    }

	    if(right_bucket->state == LINKED_LEFT){
		state_t state = STABLE;
		#ifdef AVX512
		__m256i empty = _mm256_setzero_si256();
		__m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(bucket[bucket_idx].fingerprints));
		__m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
		uint32_t bitfield = _mm256_movemask_epi8(cmp);
		for(int i=0; i<32; i++){
		    auto bit = (bitfield >> i);
		    if((bit & 0x1) == 0){
			if(high_key < bucket[bucket_idx].entry[i].key){
			    right_bucket->fingerprints[i] = bucket[bucket_idx].fingerprints[i];
			    memcpy(&right_bucket->entry[i], &bucket[bucket_idx].entry[i], sizeof(entry_t<Key_t, uint64_t>));
			    bucket[bucket_idx].fingerprints[i] = 0;
			    if(state == STABLE){
				if(right->sibling_ptr != nullptr){
				    if(right->high_key < right_bucket->entry[i].key){
					state = LINKED_RIGHT;
				    }
				}
			    }
			}
		    }
		}
 		#elif defined AVX2
		__m128i empty = _mm_setzero_si128();
		for(int m=0; m<2; m++){
		    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[bucket_idx].fingerprints + m*16));
		    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		    uint16_t bitfield = _mm_movemask_epi8(cmp);
		    for(int i=0; i<16; i++){
			auto bit = (bitfield >> i);
			if((bit & 0x1) == 0){
			    auto idx = m*16 + i;
			    if(high_key < bucket[bucket_idx].entry[idx].key){
			        #ifdef BREAKDOWN
				end = _rdtsc();
				traversal += (end - start);
				start = _rdtsc();
				#endif
				right_bucket->fingerprints[idx] = bucket[bucket_idx].fingerprints[idx];
				memcpy(&right_bucket->entry[idx], &bucket[bucket_idx].entry[idx], sizeof(entry_t<Key_t, uint64_t>));
				bucket[bucket_idx].fingerprints[idx] = 0;
				if(state == STABLE){
				    if(right->sibling_ptr != nullptr){
					if(right->high_key < right_bucket->entry[idx].key){
					    state = LINKED_RIGHT;
					}
				    }
				}
			        #ifdef BREAKDOWN
				end = _rdtsc();
				write += (end - start);
				start = _rdtsc();
				#endif
			    }
			}
		    }
		}
		#else
		for(int i=0; i<entry_num; i++){
		    if(bucket[bucket_idx].fingerprints[i] != 0){
			if(high_key < bucket[bucket_idx].entry[i].key){
			    right_bucket->fingerprints[i] = bucket[bucket_idx].fingerprints[i];
			    memcpy(&right_bucket->entry[i], &bucket[bucket_idx].entry[i], sizeof(entry_t<Key_t, uint64_t>));
			    bucket[bucket_idx].fingerprints[i] = 0;
			    if(state == STABLE){
				if(right->sibling_ptr != nullptr){
				    if(right->high_key < right_bucket->entry[i].key){
					state = LINKED_RIGHT;
				    }
				}
			    }
			}
		    }
		}
		#endif
		#ifdef BREAKDOWN
		end = _rdtsc();
		traversal += (end - start);
		start = _rdtsc();
		#endif
		right_bucket->state = state;
		#ifdef BREAKDOWN
		end = _rdtsc();
		write += (end - start);
		start = _rdtsc();
		#endif
		if(is_normal){
		    right_bucket->mutex.unlock();
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    sync += (end - start);
		    start = _rdtsc();
		    #endif
		}
		bucket[bucket_idx].state = STABLE;
		#ifdef BREAKDOWN
		end = _rdtsc();
		write += (end - start);
		start = _rdtsc();
		#endif
		return true;
	    }
	    else{ 
		blink_printf("%s: incorrect state: %d\n", __func__, right_bucket->state);
		return false;
	    }
	}




	int lowerbound_linear(Key_t key){
	    return 0;
	}

	int lowerbound_binary(Key_t key){
	    return 0;
	}

	int update_linear(Key_t key, uint64_t value){
	    for(int k=0; k<hash_funcs_num; k++){
		auto hash_key = h(&key, sizeof(Key_t), k);
		auto loc = hash_key % cardinality;

		#ifdef AVX512
		__m256i fingerprint = _mm256_set1_epi8(_hash(hash_key) | 1);
		#elif defined AVX2
		__m128i fingerprint = _mm_set1_epi8(_hash(hash_key) | 1);
		#else
		auto fingerprint = _hash(hash_key) | 1;
		#endif

		for(int j=0; j<num_slot; j++){
		    loc = (loc + j) % cardinality;
		    bucket[loc].mutex.lock();

		    if(bucket[loc].state == LINKED_LEFT){
			#ifndef BREAKDOWN
			auto ret = stabilize_bucket_left(loc, true);
			if(!ret){
			    bucket[loc].mutex.unlock();
			    return -1;
			}
			#endif
		    }
		    else if(bucket[loc].state == LINKED_RIGHT){
			#ifndef BREAKDOWN
			auto ret = stabilize_bucket_right(loc, true);
			if(!ret){
			    bucket[loc].mutex.unlock();
			    return -1;
			}
			#endif
		    }

		    #ifdef AVX512
		    __m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(bucket[loc].fingerprints));
		    __m256i cmp = _mm256_cmpeq_epi8(fingerprint, fingerprints_);
		    uint32_t bitfield = _mm256_movemask_epi8(cmp);
		    for(int i=0; i<32; i++){
			auto bit = (bitfield >> i);
			if((bit & 0x1) == 1){
			    if(bucket[loc].entry[i].key == key){
				bucket[loc].entry[i].value = value;
				bucket[loc].mutex.unlock();
				return 0;
			    }
			}
		    }
		    #elif defined AVX2
		    for(int m=0; m<2; m++){
			__m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[loc].fingerprints + m*16));
			__m128i cmp = _mm_cmpeq_epi8(fingerprint, fingerprints_);
			uint16_t bitfield = _mm_movemask_epi8(cmp);
			for(int i=0; i<16; i++){
			    auto bit = (bitfield >> i);
			    if((bit & 0x1) == 1){
				auto idx = m*16 + i;
				if(bucket[loc].entry[idx].key == key){
				    bucket[loc].entry[idx].value = value;
				    bucket[loc].mutex.unlock();
				    return 0;
				}
			    }
			}
		    }
		    #else
		    for(int i=0; i<entry_num; i++){
		 	if(bucket[loc].fingerprints[i] != 0){
			    if(bucket[loc].fingerprints[i] == fingerprint){
				if(bucket[loc].entry[i].key == key){
				    bucket[loc].entry[i].value = value;
				    bucket[loc].mutex.unlock();
				    return 0;
				}
			    }
			}
		    }
		    #endif
		    bucket[loc].mutex.unlock();
		}
	    }

	    return 1; // key not found
	}

	uint64_t find_linear(Key_t key, bool& need_restart){
	    for(int k=0; k<hash_funcs_num; k++){
		auto hash_key = h(&key, sizeof(Key_t), k);
		auto loc = hash_key % cardinality;

		#ifdef AVX512
		__m256i fingerprint = _mm256_set1_epi8(_hash(hash_key) | 1);
		#elif defined AVX2
		__m128i fingerprint = _mm_set1_epi8(_hash(hash_key) | 1);
		#else
		auto fingerprint = _hash(hash_key) | 1;
		#endif

		for(int j=0; j<num_slot; j++){
		    loc = (loc + j) % cardinality;
		    bucket[loc].mutex.lock();
		    if(bucket[loc].state == LINKED_LEFT){
			#ifndef BREAKDOWN
			auto ret = stabilize_bucket_left(loc, true);
			if(!ret){
			    bucket[loc].mutex.unlock();
			    need_restart = true;
			    return 0;
			}
			#endif
		    }
		    else if(bucket[loc].state == LINKED_RIGHT){
			#ifndef BREAKDOWN
			auto ret = stabilize_bucket_right(loc, true);
			if(!ret){
			    bucket[loc].mutex.unlock();
			    need_restart = true;
			    return 0;
			}
			#endif
		    }

		    #ifdef AVX512
		    __m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(bucket[loc].fingerprints));
		    __m256i cmp = _mm256_cmpeq_epi8(fingerprint, fingerprints_);
		    uint32_t bitfield = _mm256_movemask_epi8(cmp);
		    for(int i=0; i<32; i++){
			auto bit = (bitfield >> i);
			if((bit & 0x1) == 1){
			    if(bucket[loc].entry[i].key == key){
				auto ret = bucket[loc].entry[i].value;
				bucket[loc].mutex.unlock();
				return ret;
			    }
			}
		    }
		    #elif defined AVX2
		    for(int m=0; m<2; m++){
			__m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[loc].fingerprints + m*16));
			__m128i cmp = _mm_cmpeq_epi8(fingerprint, fingerprints_);
			uint16_t bitfield = _mm_movemask_epi8(cmp);
			for(int i=0; i<16; i++){
			    auto bit = (bitfield >> i);
			    if((bit & 0x1) == 1){
				auto idx = m*16 + i;
				if(bucket[loc].entry[idx].key == key){
				    auto ret = bucket[loc].entry[idx].value;
				    bucket[loc].mutex.unlock();
				    return ret;
				}
			    }
			}
		    }
		    #else
		    for(int i=0; i<entry_num; i++){
			if(bucket[loc].fingerprints[i] != 0){
			    if(bucket[loc].fingerprints[i] == fingerprint){
				if(bucket[loc].entry[i].key == key){
				    auto ret = bucket[loc].entry[i].value;
				    bucket[loc].mutex.unlock();
				    return ret;
				}
			    }
			}
		    }
		    #endif
		    bucket[loc].mutex.unlock();
		}
	    }

	    return 0;
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

	inline void prefetch_range(void* addr, size_t len){
	    void* cp;
	    void* end = addr + len;
	    typedef struct {char x[64]; } cacheline_t;
	    for(cp=addr; cp<end; cp+=64){
		asm volatile("prefetcht0 %0" : : "m" (*(const cacheline_t*)cp));
	    }
	    /*
	    for(cp=addr; cp<end; cp+=64){
		__builtin_prefetch(cp);
		//__builtin_prefetch(cp, 1, 2);
	    }*/
	}

	inline void prefetch(const void* addr){
#ifdef ASM 
	    typedef struct {char x[64]; } cacheline_t;
	    asm volatile("prefetcht0 %0" : : "m" (*(const cacheline_t*)addr));
#else
	    __builtin_prefetch(addr);
	    //_mm_prefetch(addr, _MM_HINT_T0);
#endif
	}
};
}
#endif