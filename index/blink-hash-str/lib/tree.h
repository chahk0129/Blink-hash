#ifndef BLINK_HASH_TREE_H__
#define BLINK_HASH_TREE_H__

#include "inode.h"
#include "lnode.h"
#include "Epoche.h"
#include "Epoche.cpp"
#include <queue>
#include <mutex>
#include <condition_variable>

namespace BLINK_HASH{

template <typename Key_t, typename Value_t>
class btree_t{
    public:
	inline uint64_t _rdtsc(){
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

	btree_t(){ 
	    root = static_cast<node_t*>(new lnode_hash_t<Key_t, Value_t>());
	    #ifndef FINGERPRINT
	    memset(&EMPTY<Key_t>, 0, sizeof(EMPTY<Key_t>));
	    #endif
		#ifdef ASYNC_ADAPT
		start_convert_workers();
		#endif
	}
	~btree_t(){ 
    #ifdef ASYNC_ADAPT
    stop_convert_workers();
    #endif
	}

	int check_height();

	void insert(Key_t key, Value_t value, ThreadInfo& threadEpocheInfo);
	/* this function is called when root has been split by another threads */
	void insert_key(Key_t key, node_t* value, node_t* prev);

	bool update(Key_t key, Value_t value, ThreadInfo& threadEpocheInfo);

	bool remove(Key_t key, ThreadInfo& threadEpocheInfo);

	Value_t lookup(Key_t key, ThreadInfo& threadEpocheInfo);

	int range_lookup(Key_t min_key, int range, Value_t* buf, ThreadInfo& threadEpocheInfo);

	void convert_all(ThreadInfo& threadEpocheInfo);

	void print_leaf();

	void print_internal();

	void print();

	void sanity_check();

	Value_t find_anyway(Key_t key);

	double utilization();

	void footprint(uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied);

	int height();

	ThreadInfo getThreadInfo();

    private:
	node_t* root;
	Epoche epoche{256};
	#ifdef ASYNC_ADAPT

	
		struct ConvertRequest {
			node_t* node;            // the hash leaf to convert
			uint64_t version_hint;   // snapshot version 
		};

		std::mutex convert_mu;
		std::condition_variable convert_cv;
		std::queue<ConvertRequest> convert_queue;
		std::vector<std::thread> convert_workers;
		std::atomic<bool> convert_shutdown{false};

		static constexpr int CONVERT_WORKERS = 2;  // tunable

		void convert_worker_loop();
		void signal_convert(node_t* node, uint64_t version);
		void start_convert_workers();
		void stop_convert_workers();

	#endif

	bool convert(lnode_t<Key_t, Value_t>* leaf, uint64_t version, ThreadInfo& threadEpocheInfo);

	void batch_insert(Key_t* key, node_t** value, int num, node_t* prev, ThreadInfo& threadEpocheInfo);
	
	inode_t<Key_t>** new_root_for_adjustment(Key_t* key, node_t** value, int num, int& new_num);
};
}
#endif
