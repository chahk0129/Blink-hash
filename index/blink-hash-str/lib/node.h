#ifndef BLINK_HASH_NODE_H__
#define BLINK_HASH_NODE_H__ 

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
#include <thread>
#include "common.h"

#include <x86intrin.h>
#include <immintrin.h>

#define BITS_PER_LONG 64
#define BITOP_WORD(nr) ((nr) / BITS_PER_LONG)

#define PAGE_SIZE (1024)
//#define PAGE_SIZE (512)

#define CACHELINE_SIZE 64
#define FILL_FACTOR (0.8)

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


namespace BLINK_HASH{

inline void mfence(void){
    asm volatile("mfence" ::: "memory");
}

class node_t{
    public:
	std::atomic<uint64_t> lock;
	node_t* sibling_ptr;
	node_t* leftmost_ptr;
	int cnt;
	int level;


	node_t(): lock(0), sibling_ptr(nullptr), leftmost_ptr(nullptr), cnt(0), level(0){ }
	node_t(node_t* sibling, node_t* left, int count, int _level): lock(0), sibling_ptr(sibling), leftmost_ptr(left), cnt(count), level(_level) { }
	node_t(node_t* sibling, node_t* left, int count, int _level, bool): lock(0), sibling_ptr(sibling), leftmost_ptr(left), cnt(count), level(_level) { }
	node_t(uint32_t _level): lock(0), sibling_ptr(nullptr), leftmost_ptr(nullptr), cnt(0), level(_level) { }

	void update_meta(node_t* sibling_ptr_, int level_){
	    lock = 0;
	    sibling_ptr = sibling_ptr_;
	    leftmost_ptr = nullptr;
	    cnt = 0;
	    level = level_;
	}

	int get_cnt(){
	    return cnt;
	}

	bool is_locked(uint64_t version){
	    return ((version & 0b100) == 0b100);
	}

	bool is_converting(uint64_t version){
	    #ifdef CONVERT_LOCK
	    return ((version & 0b10) == 0b10);
	    #else
	    return ((version & 0b100) == 0b100);
	    #endif
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

	void writelock(){
	    uint64_t version = lock.load();
	    if(!lock.compare_exchange_strong(version, version + 0b100)){
		std::cerr << __func__ << ": something wrong at " << this << std::endl;
		exit(0);
	    }
	}

	bool try_writelock(){
	    uint64_t version = lock.load();
	    if(is_locked(version) || is_obsolete(version) || is_converting(version)){
		_mm_pause();
		return false;
	    }

	    if(!lock.compare_exchange_strong(version, version + 0b100)){
		_mm_pause();
		return false;

	    }
	    return true;
	}

	void try_upgrade_writelock(uint64_t version, bool& need_restart){
	    uint64_t _version = lock.load();
	    if(version != _version || is_converting(version)){
		need_restart = true;
		return;
	    }

	    if(!lock.compare_exchange_strong(version, version + 0b100)){
		_mm_pause();
		need_restart = true;
	    }
	}

	void try_upgrade_convertlock(uint64_t version, bool& need_restart){
	    uint64_t _version = lock.load();
	    if(version != _version || is_converting(version)){
		need_restart = true;
		return;
	    }

	    #ifdef CONVERT_LOCK
	    if(!lock.compare_exchange_strong(version, version + 0b10)){
	    #else
	    if(!lock.compare_exchange_strong(version, version + 0b100)){
	    #endif
		_mm_pause();
		need_restart = true;
	    }
	}

	void write_unlock(){
	    lock.fetch_add(0b100);
	}

	void write_unlock_obsolete(){
	    lock.fetch_sub(0b101);
	}
};

}
#endif
