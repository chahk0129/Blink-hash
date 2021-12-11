#ifndef NODE_HASHED_H__
#define NODE_HASHED_H__

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <atomic>
#include <iostream>
#include <cmath>
#include <limits.h>
#include <algorithm>
#include <mutex>
#include <shared_mutex>

#if (__x86__ || __x86_64__)
#include <immintrin.h>
#include <x86intrin.h>
#else
#include <arm_neon.h>
#endif

#include "hash.h"
#include "spinlock.h"
#include "threadinfo.h"

#define BITS_PER_LONG 64
#define BITOP_WORD(nr) ((nr) / BITS_PER_LONG)

#define PAGE_SIZE (512)

#define CACHELINE_SIZE 64

#define CAS(_p, _u, _v)  (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))

static void dummy(const char*, ...) {}

#ifdef BLINK_DEBUG
#define blink_printf(fmt, ...) \
  do { \
    if(print_flag == false) break; \
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
		#if (__x86__ || __x86_64__)
		_mm_pause();
		#else
		asm volatile("yield");
		#endif
		need_restart = true;
	    }
	    return version;
	}

	uint64_t try_readlock(bool& need_restart){
	    uint64_t version = lock.load();
	    if(is_locked(version) || is_obsolete(version)){
		#if (__x86__ || __x86_64__)
		_mm_pause();
		#else
		asm volatile("yield");
		#endif
		need_restart = true;
	    }
	    return version;
	}

	bool try_writelock(){
	    uint64_t version = lock.load();
	    if(is_locked(version) || is_obsolete(version)){
		#if (__x86__ || __x86_64__)
		_mm_pause();
		#else
		asm volatile("yield");
		#endif
		return false;
	    }

	    if(!lock.compare_exchange_strong(version, version + 0b10)){
		#if (__x86__ || __x86_64__)
		_mm_pause();
		#else
		asm volatile("yield");
		#endif
		return false;

	    }
	    return true;
	}

	void try_upgrade_writelock(uint64_t& version, bool& need_restart){
	    if(!lock.compare_exchange_strong(version, version + 0b10)){
		#if (__x86__ || __x86_64__)
		_mm_pause();
		#else
		asm volatile("yield");
		#endif
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

static constexpr size_t entry_num = 32;

template <typename Key_t, typename Value_t>
struct bucket_t{
    spinlock_t mutex;
    uint8_t fingerprints[entry_num];
    entry_t<Key_t, Value_t> entry[entry_num];
};

static constexpr size_t seed = 0xc70697UL;
#define BLINK_DEBUG
static constexpr int hash_funcs_num = 2;
static constexpr size_t num_slot = 4;

template <typename Key_t>
class lnode_t : public node_t{
    public: 
	static constexpr size_t leaf_size = 1024 * 256;
	static constexpr size_t cardinality = (leaf_size - sizeof(node_t) - sizeof(Key_t))/ (sizeof(bucket_t<Key_t, uint64_t>));

	Key_t high_key;


    private:
	bucket_t<Key_t, uint64_t> bucket[cardinality];
    public:

	bool _try_splitlock(uint64_t version){
	    bool need_restart = false;
	    try_upgrade_writelock(version, need_restart);
	    if(need_restart) return false;
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

	void _split_release(){
	    for(int i=0; i<cardinality; i++){
		bucket[i].mutex.unlock_non_atomic();
	    }
	}

	// initial constructor
        lnode_t(): node_t() { } 

	// constructor when leaf splits
        lnode_t(node_t* sibling, int _cnt, uint32_t _level): node_t(sibling, nullptr, 0, _level){ }

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
	uint64_t find(Key_t key){
	    return find_linear(key);
	}

	uint8_t _hash(size_t key){
	    return (uint8_t)(key % 256);
	}

	#ifdef BREAKDOWN 
	void insert_after_split(Key_t key, uint64_t value, uint64_t& lnode_traversal, uint64_t& lnode_write){
	#else
	void insert_after_split(Key_t key, uint64_t value){
	#endif
	    #ifdef BREAKDOWN
	    uint64_t start, end, _start;
	    _start = _rdtsc();
	    #endif
	    #ifdef SIMD
	    #if (__x86__ || __x86_64__)
	    __m128i empty = _mm_setzero_si128();
	    #else
	    uint8x16_t empty = vdupq_n_u8(0);
	    uint8x16_t ones = vmov_n_u8(1);
	    #endif
	    #endif
	    for(int k=0; k<hash_funcs_num; k++){
		auto hash_key = h(&key, sizeof(Key_t), k);
		auto loc = hash_key % cardinality;
		auto fingerprint = _hash(hash_key) | 1;

		for(int j=0; j<num_slot; j++){
		    loc = (loc + j) % cardinality;
		    #ifdef SIMD
		    for(int m=0; m<2; m++){
			#if (__x86__ || __x86_64__)
			__m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[loc].fingerprints + m*16));
			__m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
			uint16_t bitfield = _mm_movemask_epi8(cmp);
			for(int i=0; i<16; i++){
			    auto bit = (bitfield >> i);
			    if((bit & 0x1) == 1){
				#ifdef BREAKDOWN
				start = _rdtsc();
				#endif
				auto idx = m*16 + i;
				bucket[loc].fingerprints[idx] = fingerprint;
				bucket[loc].entry[idx].key = key;
				bucket[loc].entry[idx].value = value;
			        #ifdef BREAKDOWN
				end = _rdtsc();
				lnode_write = (end - start);
			        #endif
				return;
			    }
			}
		 	#else
			uint8x16_t fingerprints_ = vld1q_u8(bucket[loc].fingerprints + m*16);
			uint8x16_t cmp = vandq_u8(empty, fingerprints_);
			uint8x16_t mask = vbslq_u8(cmp, ones, empty);
			for(int i=0; i<16; i++){
			    auto bit = mask[i];
			    if((bit & 0x1) == 1){
				#ifdef BREAKDOWN
				start = _rdtsc();
				#endif
				auto idx = m*16 + i;
				bucket[loc].fingerprints[idx] = fingerprint;
				bucket[loc].entry[idx].key = key;
				bucket[loc].entry[idx].value = value;
			        #ifdef BREAKDOWN
				end = _rdtsc();
				lnode_write = (end - start);
			        #endif
				return;
			    }
			}
			#endif
		    }
		    #else
		    for(int i=0; i<entry_num; i++){
			if(bucket[loc].fingerprints[i] == 0){
			    #ifdef BREAKDOWN
			    start = _rdtsc();
			    #endif
			    bucket[loc].fingerprints[i] = fingerprint;
			    bucket[loc].entry[i].key = key;
			    bucket[loc].entry[i].value = value;
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    lnode_write += (end - start);
			    lnode_traversal += (end - _start - (end - start));
			    #endif
			    return;
			}
		    }
		    #endif
		}
	    }

	    blink_printf("%s: \n", __func__,  "failed to insert");
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
	    #ifdef SIMD
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
		    #endif
#ifdef SIMD
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
	lnode_t<Key_t>* split(Key_t& split_key, uint64_t version, uint64_t& lnode_alloc, uint64_t& lnode_sync, uint64_t& lnode_split, uint64_t& lnode_key_copy, uint64_t& lnode_find_median, uint64_t& lnode_copy, uint64_t& lnode_update){
	#endif
	//lnode_t<Key_t>* split(Key_t& split_key, uint64_t version, uint64_t& lnode_alloc, uint64_t& lnode_sync, uint64_t& lnode_split, uint64_t& lnode_find_median, uint64_t& lnode_copy, uint64_t& lnode_update){
	//lnode_t<Key_t>* split(Key_t& split_key, uint64_t version, uint64_t& lnode_alloc, uint64_t& lnode_sync, uint64_t& lnode_split){
	#else
	#ifdef THREAD_ALLOC
	lnode_t<Key_t>* split(Key_t& split_key, uint64_t version, threadinfo* ti){ 
	#else
	lnode_t<Key_t>* split(Key_t& split_key, uint64_t version){ 
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
	    
	    

	    //prefetch_range(new_right->bucket, sizeof(bucket_t<Key_t, uint64_t>)*cardinality);
	    /*
	    for(int i=0; i<sizeof(lnode_t<Key_t>)/64; i++){
		prefetch((void*)((uint64_t)new_right + i*64));
	    }*/
	    #else
	    auto new_right = new lnode_t<Key_t>(sibling_ptr, 0, level);
	    #endif
	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    lnode_alloc += (end - start);
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
	    new_right->high_key = high_key;

	    /*
	    auto util = utilization() * 100;
	    std::cout << util << std::endl; 
	    */

	    #ifdef SIMD
	    __m128i empty = _mm_set1_epi8(0);
	    #endif
	    #ifdef BUCKET_SAMPLING // bucket-based sampling
	    Key_t temp[cardinality];
	    int valid_num = 0;
	    for(int j=0; j<cardinality; j++){
	        #ifdef SIMD
		for(int k=0; k<2; k++){
		    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[j].fingerprints + k*16));
		    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		    uint16_t bitfield = _mm_movemask_epi8(cmp);
		    for(int i=0; i<16; i++){
			auto bit = (bitfield >> i);
			if((bit & 0x1) == 0){
			    auto idx = k*16 + i;
			    temp[valid_num++] = bucket[j].entry[idx].key;
			    goto scan_next_bucket;
			}
		    }
		}
		scan_next_bucket:
		(void) valid_num;
		#else
		for(int i=0; i<entry_num; i++){
		    if(bucket[j].fingerprints[i] != 0){
			temp[valid_num++] = bucket[j].entry[i].key;
			break;
		    }
		}
		#endif
	    }
	    #elif defined ENTRY_SAMPLING // entry-based sampling
	    Key_t temp[cardinality];
	    int valid_num = 0;
	    for(int j=0; j<cardinality; j++){
	        #ifdef SIMD
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
	        #ifdef SIMD
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
#ifndef BULK_COPY
	    sibling_ptr = new_right;
#endif
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

#ifdef BULK_COPY // copying all the entries in all the buckets
	    memcpy(new_right->bucket, bucket, sizeof(bucket_t<Key_t, uint64_t>)*cardinality);
	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    lnode_copy += (end - start);
	    start = _rdtsc();
	    #endif
	    //new_right->_split_release();
	    for(int j=0; j<cardinality; j++){
		new_right->bucket[j].mutex.unlock_non_atomic();
		#ifdef SIMD
		#ifdef BATCH
		uint8_t finger_to_be_updated[32] = {0};
		//memcpy(finger_to_be_updated, bucket[j].fingerprints, sizeof(uint8_t)*32);
		for(int k=0; k<2; k++){
		    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[j].fingerprints + k*16));
		    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		    uint16_t bitfield = _mm_movemask_epi8(cmp);
		    for(int i=0; i<16; i++){
			auto bit = (bitfield >> i);
			if((bit & 0x1) == 0){
			    auto idx = k*16 + i;
			    if(split_key < bucket[j].entry[idx].key){
				finger_to_be_updated[idx] = bucket[j].fingerprints[idx];
				bucket[j].fingerprints[idx] = 0;
			    }
			    /*
			    else
				finger_to_be_updated[idx] = 0;
				*/
			}
		    }
		    /*
		    memcpy(bucket[j].fingerprints + k*16, finger_to_be_updated, sizeof(uint8_t)*16);
		    memcpy(new_right->bucket[j].fingerprints + k*16, new_finger_to_be_updated, sizeof(uint8_t)*16);
		    */
		    /*
		    __m128i new_fingerprints = _mm_loadu_si128(reinterpret_cast<__m128i*>(finger_to_be_updated));
		    __m128i new_fingerprints_= _mm_loadu_si128(reinterpret_cast<__m128i*>(new_finger_to_be_updated));
		    _mm_storeu_si128(reinterpret_cast<__m128i*>(bucket[j].fingerprints + k*16), new_fingerprints);
		    _mm_storeu_si128(reinterpret_cast<__m128i*>(new_right->bucket[j].fingerprints + k*16), new_fingerprints_);
		    _mm_storeu_si128(reinterpret_cast<__m128i*>(bucket[j].fingerprints + k*16), *reinterpret_cast<__m128i*>(finger_to_be_updated));
		    _mm_storeu_si128(reinterpret_cast<__m128i*>(new_right->bucket[j].fingerprints + k*16), *reinterpret_cast<__m128i*>(new_finger_to_be_updated));
		    */
		}
		//_mm_storeu_si128(reinterpret_cast<__m128i*>(new_right->bucket[j].fingerprints), *reinterpret_cast<__m128i*>(finger_to_be_updated));
		//_mm_storeu_si128(reinterpret_cast<__m128i*>(new_right->bucket[j].fingerprints + 16), *reinterpret_cast<__m128i*>(finger_to_be_updated + 16));
		memcpy(new_right->bucket[j].fingerprints, finger_to_be_updated, sizeof(uint8_t)*32);
		#else
		for(int k=0; k<2; k++){
		    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[j].fingerprints + k*16));
		    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		    uint16_t bitfield = _mm_movemask_epi8(cmp);
		    for(int i=0; i<16; i++){
			auto bit = (bitfield >> i);
			if((bit & 0x1) == 0){
			    auto idx = k*16 + i;
			    if(split_key < bucket[j].entry[idx].key)
				bucket[j].fingerprints[idx] = 0;
			    else
				new_right->bucket[j].fingerprints[idx] = 0;
			}
		    }
		}
		#endif
		#else
		for(int i=0; i<entry_num; i++){
		    if(bucket[j].fingerprints[i] != 0){
			if(split_key < bucket[j].entry[i].key)
			    bucket[j].fingerprints[i] = 0;
			else
			    new_right->bucket[j].fingerprints[i] = 0;
		    }
		}
		#endif
	    }
	    sibling_ptr = new_right;
#else // copying an entry one by one upon key comparisons
	    for(int j=0; j<cardinality; j++){
		#ifdef CHUNK_COPY 
		for(int k=0; k<2; k++){
		    ///__m128i empty = _mm_set1_epi8(0);
		    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[j].fingerprints + k*16));
		    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		    _mm_storeu_si128(reinterpret_cast<__m128i*>(new_right->bucket[j].fingerprints + k*16), fingerprints_);
		    uint16_t bitfield = _mm_movemask_epi8(cmp);
		    for(int i=0; i<16; i++){
			auto bit = (bitfield >> i);
			if((bit & 0x1) == 0){
			    auto idx = k*16 + i;
			    if(split_key < bucket[j].entry[idx].key){
				bucket[j].fingerprints[idx] = 0;
			        #ifdef BREAKDOWN
				end = _rdtsc();
				lnode_update += (end - start);
				start = _rdtsc();
			        #endif
				memcpy(&new_right->bucket[j].entry[idx], &bucket[j].entry[idx], sizeof(entry_t<Key_t, uint64_t>));
			        #ifdef BREAKDOWN
				end = _rdtsc();
				lnode_copy += (end - start);
				start = _rdtsc();
			        #endif
			    }
			    else{
				new_right->bucket[j].fingerprints[idx] = 0;
			        #ifdef BREAKDOWN
				end = _rdtsc();
				lnode_update += (end - start);
				start = _rdtsc();
			        #endif
			    }
			}
		    }
		}
		#else
		#ifdef SIMD
		for(int k=0; k<2; k++){
		    ///__m128i empty = _mm_set1_epi8(0);
		    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[j].fingerprints + k*16));
		    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		    uint16_t bitfield = _mm_movemask_epi8(cmp);
		    for(int i=0; i<16; i++){
			auto bit = (bitfield >> i);
			if((bit & 0x1) == 0){
			    auto idx = k*16 + i;
			    if(split_key < bucket[j].entry[idx].key){
				new_right->bucket[j].fingerprints[idx] = bucket[j].fingerprints[idx];
				bucket[j].fingerprints[idx] = 0;
			        #ifdef BREAKDOWN
				end = _rdtsc();
				lnode_update += (end - start);
				start = _rdtsc();
			        #endif
				memcpy(&new_right->bucket[j].entry[idx], &bucket[j].entry[idx], sizeof(entry_t<Key_t, uint64_t>));
			        #ifdef BREAKDOWN
				end = _rdtsc();
				lnode_copy += (end - start);
				start = _rdtsc();
			        #endif
			    }
			}
		    }
		}
		#else
		for(int i=0; i<entry_num; i++){
		    if(bucket[j].fingerprints[i] != 0){
			if(split_key < bucket[j].entry[i].key){
			    new_right->bucket[j].fingerprints[i] = bucket[j].fingerprints[i];
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    lnode_update += (end - start);
			    start = _rdtsc();
			    #endif
			    memcpy(&new_right->bucket[j].entry[i], &bucket[j].entry[i], sizeof(entry_t<Key_t, uint64_t>));
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    lnode_copy += (end - start);
			    start = _rdtsc();
			    #endif
			    bucket[j].fingerprints[i] = 0;
			}
		    }
		}
		#endif
		#endif
	    }
#endif

	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    lnode_update += (end - start);
	    #endif


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

	    #ifdef SIMD
	    __m128i empty = _mm_setzero_si128();
	    #endif
	    for(int j=0; j<num_slot; j++){
		bucket[j].mutex.lock();
	        #ifdef SIMD
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
	    for(int i=0; i<cardinality; i++){
		for(int j=0; j<entry_num; j++){
		    std::cout << "[" << i << "][" << j << "] " << "finger(" << (int)bucket[i].fingerprints[j] << "), key(" << bucket[i].entry[j].key << ")";
		}
	    }
	    std::cout << "\nnode_high_key: " << high_key << "\n\n";
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

		#ifdef SIMD
		__m128i fingerprint = _mm_set1_epi8(_hash(hash_key) | 1);
		#else
		auto fingerprint = _hash(hash_key) | 1;
		#endif
		for(int j=0; j<num_slot; j++){
		    loc = (loc + j) % cardinality;
		    bucket[loc].mutex.lock();
		    #ifdef SIMD
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

	uint64_t find_linear(Key_t key){
	    for(int k=0; k<hash_funcs_num; k++){
		auto hash_key = h(&key, sizeof(Key_t), k);
		bool need_restart = false;
		auto loc = hash_key % cardinality;

		#ifdef SIMD
		__m128i fingerprint = _mm_set1_epi8(_hash(hash_key) | 1);
		#else
		auto fingerprint = _hash(hash_key) | 1;
		#endif
		for(int j=0; j<num_slot; j++){
		    loc = (loc + j) % cardinality;
		    bucket[loc].mutex.lock();
		    #ifdef SIMD
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
