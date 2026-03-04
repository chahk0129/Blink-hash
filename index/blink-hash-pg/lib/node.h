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
#include <thread>
#include "common.h"

#ifdef __x86_64__
#  include <x86intrin.h>
#  include <immintrin.h>
#endif

namespace BLINK_HASH{

#define BITS_PER_LONG 64
#define BITOP_WORD(nr) ((nr) / BITS_PER_LONG)

//#define PAGE_SIZE (1024)
#define PAGE_SIZE (512)

#define CACHELINE_SIZE 64
#define FILL_FACTOR (0.8)

#define CAS(_p, _u, _v)  (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))

//#define BLINK_DEBUG
#ifdef BLINK_DEBUG
#define blink_printf(fmt, ...) \
  do { \
    if(print_flag == false) break; \
    fprintf(stdout, "%-24s(%8lX): " fmt, \
            __FUNCTION__, \
            std::hash<std::thread::id>()(std::this_thread::get_id()), \
            ##__VA_ARGS__); \
    fflush(stdout); \
  } while (0);

#else

static void dummy(const char*, ...) {}
#define blink_printf(fmt, ...)   \
  do {                         \
    dummy(fmt, ##__VA_ARGS__); \
  } while (0);

#endif

extern bool print_flag;

// Portable spin-hint: on x86 this is PAUSE; on ARM this is YIELD.
inline void cpu_pause() noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    _mm_pause();
#elif defined(__aarch64__) || defined(__arm__)
    __asm__ volatile("yield" ::: "memory");
#else
    /* nothing */
#endif
}

inline void mfence(void){
#if defined(__x86_64__) || defined(_M_X64)
    asm volatile("mfence" ::: "memory");
#elif defined(__aarch64__) || defined(__arm__)
    __asm__ volatile("dmb ish" ::: "memory");
#else
    std::atomic_thread_fence(std::memory_order_seq_cst);
#endif
}

class node_t{
    public:
	std::atomic<uint64_t> lock;
	node_t* sibling_ptr;
	node_t* leftmost_ptr;
	int cnt;
	int level;
	uint64_t node_id;

	node_t(): lock(0b0), sibling_ptr(nullptr), leftmost_ptr(nullptr), cnt(0), level(0), node_id(0){ }
	node_t(node_t* sibling, node_t* left, int count, int _level): lock(0b0), sibling_ptr(sibling), leftmost_ptr(left), cnt(count), level(_level), node_id(0) { }
	node_t(node_t* sibling, node_t* left, int count, int _level, bool): lock(0b0), sibling_ptr(sibling), leftmost_ptr(left), cnt(count), level(_level), node_id(0) { }
	node_t(uint32_t _level): lock(0b0), sibling_ptr(nullptr), leftmost_ptr(nullptr), cnt(0), level(_level), node_id(0) { }

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
	    return ((version & 0b10) == 0b10);
	}

	bool is_obsolete(uint64_t version){
	    return (version & 1) == 1;
	}

	uint64_t get_version(bool& need_restart){
	    uint64_t version = lock.load();
	    if(is_locked(version) || is_obsolete(version)){
		cpu_pause();
		need_restart = true;
	    }
	    return version;
	}

	uint64_t try_readlock(bool& need_restart){
	    uint64_t version = lock.load();
	    if(is_locked(version) || is_obsolete(version)){
		cpu_pause();
		need_restart = true;
	    }
	    return version;
	}

	void writelock(){
	    uint64_t version = lock.load();
	    if(!lock.compare_exchange_strong(version, version + 0b10)){
		std::cerr << __func__ << ": something wrong at " << this << std::endl;
		exit(0);
	    }
	}

	bool try_writelock(){
	    uint64_t version = lock.load();
	    if(is_locked(version) || is_obsolete(version)){
		cpu_pause();
		return false;
	    }

	    if(!lock.compare_exchange_strong(version, version + 0b10)){
		cpu_pause();
		return false;
	    }
	    return true;
	}

	void try_upgrade_writelock(uint64_t version, bool& need_restart){
	    uint64_t _version = lock.load();
	    if(version != _version){
		need_restart = true;
		return;
	    }

	    if(!lock.compare_exchange_strong(version, version + 0b10)){
		cpu_pause();
		need_restart = true;
	    }
	}

	void write_unlock(){
	    lock.fetch_add(0b10);
	}

	void write_unlock_obsolete(){
	    lock.fetch_sub(0b11);
	}
};
}
#endif
