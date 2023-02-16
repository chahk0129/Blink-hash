#include "include/indexkey.h"
#include "include/microbench.h"
#include "include/index.h"

#ifndef _UTIL_H
#define _UTIL_H

struct breakdown_t{
    uint64_t traversal;
    uint64_t abort;
    uint64_t latch;
    uint64_t node;
    uint64_t split;
    uint64_t consolidation;
};


//bool hyperthreading = true;
bool hyperthreading = false;

//This enum enumerates index types we support
enum {
  TYPE_BWTREE = 0,
  TYPE_MASSTREE,
  TYPE_ARTOLC,
  TYPE_ARTROWEX,
  TYPE_HOT,
  TYPE_BLINKTREE,
  TYPE_BLINKHASH,
  TYPE_CUCKOOHASH,
  TYPE_BTREEOLC,
  TYPE_BLINKBUFFER,
  TYPE_BLINKBUFFERBATCH,
  TYPE_NONE,
};

// These are workload operations
enum {
  OP_INSERT,
  OP_RANDOMINSERT,
  OP_READ,
  OP_UPSERT,
  OP_SCAN,
};

// These are YCSB workloads
enum {
  WORKLOAD_LOAD,
  WORKLOAD_A,
  WORKLOAD_B,
  WORKLOAD_C,
  WORKLOAD_E,
  WORKLOAD_MIXED,
};

// These are key types we use for running the benchmark
enum {
  RAND_KEY,
  MONO_KEY,
  RDTSC_KEY,
  EMAIL_KEY,
  TIMESTAMP_KEY,
  URL_KEY,
};

//==============================================================
// GET INSTANCE
//==============================================================
template<typename KeyType, 
         typename KeyComparator=std::less<KeyType>, 
         typename KeyEuqal=std::equal_to<KeyType>, 
         typename KeyHash=std::hash<KeyType>>
Index<KeyType, KeyComparator> *getInstance(const int type, const uint64_t kt) {
  if (type == TYPE_BWTREE)
    return new BwTreeIndex<KeyType, KeyComparator, KeyEuqal, KeyHash>(kt);
  else if (type == TYPE_MASSTREE)
    return new MassTreeIndex<KeyType, KeyComparator>(kt);
  else if (type == TYPE_ARTOLC)
      return new ArtOLCIndex<KeyType, KeyComparator>(kt);
  else if (type == TYPE_ARTROWEX)
      return new ArtROWEXIndex<KeyType, KeyComparator>(kt);
  else if (type == TYPE_HOT)
      return new HOTIndex<KeyType, KeyComparator>(kt);
  else if (type == TYPE_BTREEOLC)
      return new BTreeOLCIndex<KeyType, KeyComparator>(kt);
  else if (type == TYPE_BLINKTREE)
      return new BlinkIndex<KeyType, KeyComparator>(kt);
  #ifndef STRING_KEY
  else if (type == TYPE_BLINKBUFFER)
      return new BlinkBufferIndex<KeyType, KeyComparator>(kt);
  else if (type == TYPE_BLINKBUFFERBATCH)
      return new BlinkBufferBatchIndex<KeyType, KeyComparator>(kt);
  #endif
  else if (type == TYPE_CUCKOOHASH)
      return new CuckooIndex<KeyType, KeyComparator>(kt);
  else if (type == TYPE_BLINKHASH)
      return new BlinkHashIndex<KeyType, KeyComparator>(kt);
  else {
    fprintf(stderr, "Unknown index type: %d\n", type);
    exit(1);
  }
  
  return nullptr;
}

inline double get_now() { 
struct timeval tv; 
  gettimeofday(&tv, 0); 
  return tv.tv_sec + tv.tv_usec / 1000000.0; 
} 

/*
 * Rdtsc() - This function returns the value of the time stamp counter
 *           on the current core
 */
inline uint64_t Rdtsc()
{
#if (__x86__ || __x86_64__)
    uint32_t lo, hi;
    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
    return (((uint64_t) hi << 32) | lo);
#else
    uint64_t val;
    asm volatile("mrs %0, cntvct_el0" : "=r" (val));
    return val;
#endif
}

// This is the order of allocation
static int core_alloc_map_hyper[] = {
    /*
    0,  64,   1,  65,   2,  66,   3,  67,
    4,  68,   5,  69,   6,  70,   7,  71,
    8,  72,   9,  73,  10,  74,  11,  75,
   12,  76,  13,  77,  14,  78,  15,  79,
   16,  80,  17,  81,  18,  82,  19,  83,
   20,  84,  21,  85,  22,  86,  23,  87,
   24,  88,  25,  89,  26,  90,  27,  91,
   28,  92,  29,  93,  30,  94,  31,  95, // socket 0
   32,  96,  33,  97,  34,  98,  35,  99,
   36, 100,  37, 101,  38, 102,  39, 103,
   40, 104,  41, 105,  42, 106,  43, 107,
   44, 108,  45, 109,  46, 110,  47, 111,
   48, 112,  49, 113,  50, 114,  51, 115,
   52, 116,  53, 117,  54, 118,  55, 119,
   56, 120,  57, 121,  58, 122,  59, 123,
   60, 124,  61, 125,  62, 126,  63, 127 // socket 1	// amd
      */ 
    /*
    0,   1,   2,   3,   4,   5,   6,   7, 	// intel
    8,   9,  10,  11,  12,  13,  14,  15,
   16,  17,  18,  19,  20,  21,  22,  23,
   24,  25,  26,  27,  28,  29,  30,  31,
   64,  65,  66,  67,  68,  69,  70,  71,
   72,  73,  74,  75,  76,  77,  78,  79, 
   80,  81,  82,  83,  84,  85,  86,  87, 
   88,  89,  90,  91,  92,  93,  94,  95, // socket 0
   32,  33,  34,  35,  36,  37,  38,  39, 
   40,  41,  42,  43,  44,  45,  46,  47, 
   48,  49,  50,  51,  52,  53,  54,  55, 
   56,  57,  58,  59,  60,  61,  62,  63,
   96,  97,  98,  99, 100, 101, 102, 103,
  104, 105, 106, 107, 108, 109, 110, 111,
  112, 113, 114, 115, 116, 117, 118, 119,
  120, 121, 122, 123, 124, 125, 126, 127 // socket 1
      */

  /*
  0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
  11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
  21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
  31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
  41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
  51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
  61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
  71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
  81, 82, 83, 84, 85, 86, 87, 88, 89, 90,
  91, 92, 93, 94, 95, 96, 97, 98, 99, 100,
  101, 102, 103, 104, 105, 106, 107, 108, 109, 110,
  111, 112, 113, 114, 115, 116, 117, 118, 119, 120,
  121, 122, 123, 124, 125, 126, 127, 128, 129, 130,
  131, 132, 133, 134, 135, 136, 137, 138, 139, 140,
  141, 142, 143, 144, 145, 146, 147, 148, 149, 150,
  151, 152, 153, 154, 155, 156, 157, 158, 159
  */
	  
  0, 2, 4, 6, 8, 10, 12, 14,
  16, 18, 20, 22, 24, 26, 28, 30,
  32, 34, 36, 38, 40, 42, 44, 46, 
  48, 50, 52, 54, 56, 58, 60, 62,
  1, 3, 5, 7 ,9, 11, 13, 15,
  17, 19, 21, 23, 25, 27, 29, 31,
  33, 35, 37, 39, 41, 43, 45, 47, 
  49, 51, 53, 55, 57, 59, 61, 63
  
};


static int core_alloc_map_numa[] = {
  0, 2, 4, 6, 8, 10, 12, 14, 16, 18,
  1, 3, 5, 7 ,9, 11, 13, 15, 17, 19,
  20, 22, 24, 26, 28, 30, 32, 34, 36, 38,
  21, 23, 25, 27, 29, 31, 33, 35, 37, 39,  
};


//constexpr static size_t MAX_CORE_NUM = 128;
constexpr static size_t MAX_CORE_NUM = 64;

inline void PinToCore(size_t thread_id) {
  cpu_set_t cpu_set;
  CPU_ZERO(&cpu_set);

  size_t core_id = thread_id % MAX_CORE_NUM;

  if(hyperthreading == true) {
    CPU_SET(core_alloc_map_hyper[core_id], &cpu_set);
  } else {
    CPU_SET(core_alloc_map_numa[core_id], &cpu_set);
  }

  int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
  if(ret != 0) {
    fprintf(stderr, "PinToCore() returns non-0\n");
    exit(1);
  }

  return;
}

template <typename Fn, typename... Args>
void StartThreads(Index<keytype, keycomp> *tree_p,
                  uint64_t num_threads,
                  Fn &&fn,
                  Args &&...args) {
  std::vector<std::thread> thread_group;

  if(tree_p != nullptr) {
    tree_p->UpdateThreadLocal(num_threads);
  }

  auto fn2 = [tree_p, &fn](uint64_t thread_id, Args ...args) {
    if(tree_p != nullptr) {
      tree_p->AssignGCID(thread_id);
    }

    PinToCore(thread_id);
    fn(thread_id, args...);

    if(tree_p != nullptr) {
      tree_p->UnregisterThread(thread_id);
    }

    return;
  };

  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread{fn2, thread_itr, std::ref(args...)});
  }

  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }

  // Print statistical data before we destruct thread local data
#ifdef BWTREE_COLLECT_STATISTICS
  tree_p->CollectStatisticalCounter(num_threads);
#endif

  if(tree_p != nullptr) {
    tree_p->UpdateThreadLocal(1);
  }

  return;
}

/*
 * GetTxnCount() - Counts transactions and return 
 */
template <bool upsert_hack=true>
int GetTxnCount(const std::vector<int> &ops,
                int index_type) {
  int count = 0;
 
  for(auto op : ops) {
    switch(op) {
      case OP_INSERT:
      case OP_READ:
      case OP_SCAN:
        count++;
        break;
      case OP_UPSERT:
        count++;

        break;
      default:
        fprintf(stderr, "Unknown operation\n");
        exit(1);
        break;
    }
  }

  return count;
}


#endif
