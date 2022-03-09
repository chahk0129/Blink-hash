#include "src/papi_util.cpp"
#include "include/options.h"
#include "include/cxxopts.hpp"
#include "include/microbench.h"
#include "include/perf_event.h"

//#include <AMDProfileController.h>
#include <cstring>
#include <cctype>
#include <atomic>
#include <sstream>

thread_local long skiplist_steps = 0;
std::atomic<long> skiplist_total_steps;

//#define DEBUG

// Enable this if you need pre-allocation utilization
//#define BWTREE_CONSOLIDATE_AFTER_INSERT

#ifdef BWTREE_CONSOLIDATE_AFTER_INSERT
#ifdef USE_TBB
#warning "Could not use TBB and BwTree consolidate together"
#endif
#endif

#ifdef BWTREE_COLLECT_STATISTICS
#ifdef USE_TBB
#warning "Could not use TBB and BwTree statistics together"
#endif
#endif

// Whether read operatoin miss will be counted
//#define COUNT_READ_MISS

typedef uint64_t keytype;
typedef std::less<uint64_t> keycomp;

static const uint64_t key_type=0;
static const uint64_t value_type=1; // 0 = random pointers, 1 = pointers to keys

extern bool hyperthreading;

// This is the flag for whether to measure memory bandwidth
static bool memory_bandwidth = false;
// This is the flag for whether to measure CPU profiling
static bool profile = false;
// Whether we only perform insert
static bool insert_only = false;
// Whether measure preformance based on earliest finished thread
static bool earliest = false;
// Whether measure latency
static bool measure_latency = false;
static float sampling_rate = 0;
static float skew_factor = 1.2;

// We could set an upper bound of the number of loaded keys
static int64_t max_init_key = -1;

#include "include/util.h"

/*
 * MemUsage() - Reads memory usage from /proc file system
 */
size_t MemUsage() {
    FILE *fp = fopen("/proc/self/statm", "r");
    if(fp == nullptr) {
	fprintf(stderr, "Could not open /proc/self/statm to read memory usage\n");
	exit(1);
    }

    unsigned long unused;
    unsigned long rss;
    if (fscanf(fp, "%ld %ld %ld %ld %ld %ld %ld", &unused, &rss, &unused, &unused, &unused, &unused, &unused) != 7) {
	perror("");
	exit(1);
    }

    (void)unused;
    fclose(fp);

    return rss * (4096 / 1024); // in KiB (not kB)
}

//==============================================================
// LOAD
//==============================================================
inline void load(int wl, int index_type, kvpair_t<keytype>* init_kv, int& init_num, kvpair_t<keytype>* run_kv, int& run_num, int* ranges){
    for(int i=0; i<init_num; i++){
	init_kv[i].key = i+1;
    }

    std::random_shuffle(init_kv, init_kv+init_num);
    for(int i=0; i<init_num; i++){
	init_kv[i].value = (uint64_t)&init_kv[i].key;
    }

    if(wl == OP_INSERT)
	return;

    memcpy(run_kv, init_kv, sizeof(kvpair_t<keytype>)*run_num);
    if(wl == OP_UPSERT){
	for(int i=0; i<run_num; i++){
	    run_kv[i].value = (uint64_t)&run_kv[i].key;
	}
    }
    else if(wl == OP_SCAN){
	for(int i=0; i<run_num; i++){
	    ranges[i] = rand() % 100;
	}
    }
}

//==============================================================
// EXEC
//==============================================================
inline void exec(int wl, int index_type, int num_thread, kvpair_t<keytype>* init_kv, int init_num, kvpair_t<keytype>* run_kv, int run_num, int* ranges){

    Index<keytype, keycomp> *idx = getInstance<keytype, keycomp>(index_type, key_type);

    std::vector<std::chrono::high_resolution_clock::time_point> local_load_latency[num_thread];
    if(measure_latency){
	for(int i=0; i<num_thread; i++){
	    local_load_latency[i].resize(std::ceil(init_num / num_thread * 2));
	    local_load_latency[i].resize(0);
	}
    }

    //WRITE ONLY TEST-----------------
    int count = init_num;
    if(insert_only)
	fprintf(stderr, "Populating the index with %d keys using %d threads\n", count, num_thread);

    BenchmarkParameters params[num_thread];
    PerfEventBlock perf_block[num_thread];

    bool earliest_finished = false;
    std::vector<uint64_t> inserted_num(num_thread);
    breakdown_t breakdown[num_thread];
    memset(breakdown, 0, sizeof(breakdown_t)*num_thread);

    auto func_earliest = [idx, init_kv, init_num, num_thread, index_type, &earliest_finished, &inserted_num, &local_load_latency, measure_latency, sampling_rate, &params, &perf_block, profile, insert_only, &breakdown] (uint64_t thread_id, bool) {
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	size_t total_num_key = init_num; 
	size_t key_per_thread = total_num_key / num_thread;
	size_t start_index = key_per_thread * thread_id;
	size_t end_index = start_index + key_per_thread;
	if(thread_id == num_thread-1)
	    end_index = init_num;
	#ifdef BREAKDOWN
	breakdown_t time;
	memset(&time, 0, sizeof(breakdown_t));
	#endif

	PerfEventBlock e;
	if(insert_only && profile){
	    params[thread_id].setParam("threads", thread_id+1);
	    e.setParam(1, params[thread_id], (thread_id == 0));
	    e.registerCounters();
	    e.startCounters();
	}

	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);

	int gc_counter = 0;
	for(size_t i = start_index;i < end_index;i++) {
	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();

	    if(measure_latency_)
		local_load_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    #ifdef BWTREE_USE_DELTA_UPDATE
	    idx->insert(init_kv[i].key, init_kv[i].value, ti);
	    #else
	    idx->insert_bwtree_fast(init_kv[i].key, init_kv[i].value);
	    #endif
	    if(measure_latency_)
		local_load_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    gc_counter++;
	    if(gc_counter % 4096 == 0) {
		ti->rcu_quiesce();
	    }

	    if(earliest_finished){
		ti->rcu_quiesce();
		if(insert_only && profile){
		    e.stopCounters();
		    perf_block[thread_id] = e;
		}
		inserted_num[thread_id] = i - start_index;
		#ifdef BREAKDOWN
                idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
                memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
		#endif
		return;
	    }
	} 

	ti->rcu_quiesce();
	earliest_finished = true;
	if(insert_only && profile){
	    e.stopCounters();
	    perf_block[thread_id] = e;
	}
	inserted_num[thread_id] = key_per_thread;
	#ifdef BREAKDOWN
        idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
        memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
        #endif
	return;
    };

    auto func = [idx, &init_kv, init_num, num_thread, index_type, &local_load_latency, measure_latency, sampling_rate, &params, &perf_block, profile, insert_only, &breakdown] (uint64_t thread_id, bool) {
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	size_t total_num_key = init_num; 
	size_t key_per_thread = total_num_key / num_thread;
	size_t start_index = key_per_thread * thread_id;
	size_t end_index = start_index + key_per_thread;
	if(thread_id == num_thread-1)
	    end_index = init_num;
	#ifdef BREAKDOWN
	breakdown_t time;
	memset(&time, 0, sizeof(breakdown_t));
	#endif

	PerfEventBlock e;
        if(insert_only && profile){
	    params[thread_id].setParam("threads", thread_id+1);
            e.setParam(1, params[thread_id], (thread_id == 0));
	    e.registerCounters();
            e.startCounters();
        }

	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);

	int gc_counter = 0;
	for(size_t i = start_index;i < end_index;i++) {
	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();

	    if(measure_latency_)
		local_load_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    #ifdef BWTREE_USE_DELTA_UPDATE
	    idx->insert(init_kv[i].key, init_kv[i].value, ti);
	    #else
	    idx->insert_bwtree_fast(init_kv[i].key, init_kv[i].value);
	    #endif
	    if(measure_latency_)
		local_load_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    gc_counter++;
	    if(gc_counter % 4096 == 0) {
		ti->rcu_quiesce();
	    }
	} 
	ti->rcu_quiesce();
	if(insert_only && profile){
            e.stopCounters();
	    perf_block[thread_id] = e;
	}
	#ifdef BREAKDOWN
        idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
        memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
        #endif
	return;
    };

    uint64_t build_thread = static_cast<uint64_t>(MAX_CORE_NUM/2);
    auto func_build = [idx, init_kv, init_num, build_thread, index_type] (uint64_t thread_id, bool) {
	size_t total_num_key = init_num; 
	size_t key_per_thread = total_num_key / build_thread;
	size_t start_index = key_per_thread * thread_id;
	size_t end_index = start_index + key_per_thread;
	if(thread_id == build_thread-1)
	    end_index = init_num;

	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);
	int gc_counter = 0;
	for(size_t i = start_index;i < end_index;i++) {
	    #ifdef BWTREE_USE_DELTA_UPDATE
	    idx->insert(init_kv[i].key, init_kv[i].value, ti);
	    #else
	    idx->insert_bwtree_fast(init_kv[i].key, init_kv[i].value);
	    #endif

	    gc_counter++;
	    if(gc_counter % 4096 == 0) {
		ti->rcu_quiesce();
	    }
	} 
	ti->rcu_quiesce();
	return;
    };

    if(insert_only && memory_bandwidth){
	std::cout << "Ready to profile" << std::endl;
	getchar();
    }
    double start_time = get_now(); 
    if(insert_only){
	if(earliest)
	    StartThreads(idx, num_thread, func_earliest, false);
	else
	    StartThreads(idx, num_thread, func, false);
    }
    else{
	std::cout << "Building index with " << build_thread << " threads" << std::endl;
	StartThreads(idx, build_thread, func_build, false);
    }
    double end_time = get_now();
    
    if(insert_only && memory_bandwidth){
	std::cout << "End of profile" << std::endl;
	getchar();
    }

    if(earliest && insert_only){
	uint64_t _count = 0;
	for(int i=0; i<num_thread; i++){
	    _count += inserted_num[i];
	}
	std::cout << "Processed " << _count << " / " << count << " (" << (double)_count/count*100 << " \%)" << std::endl;
	count = _count;
    }

    double tput = count / (end_time - start_time) / 1000000; //Mops/sec
    if(wl == OP_INSERT)
	std::cout << "Workload Load " << tput << std::endl;
    else
        std::cout << "Load " << tput << std::endl;

    if(insert_only && measure_latency){
	std::vector<uint64_t> global_latency;
	uint64_t total_latency = 0;
	for(auto& v: local_load_latency){
	    for(auto i=0; i<v.size(); i+=2){
		auto lat = std::chrono::nanoseconds(v[i+1] - v[i]).count();
		total_latency += lat;
		global_latency.push_back(lat);
	    }
	}

	std::sort(global_latency.begin(), global_latency.end());
	auto latency_size = global_latency.size();
	std::cout << "Latency observed (" << latency_size << ") \n"
	          << "\tavg: \t" << total_latency / latency_size << "\n"
		  << "\tmin: \t" << global_latency[0] << "\n"
		  << "\t50%: \t" << global_latency[0.5*latency_size] << "\n"
		  << "\t90%: \t" << global_latency[0.9*latency_size] << "\n"
		  << "\t95%: \t" << global_latency[0.95*latency_size] << "\n"
		  << "\t99%: \t" << global_latency[0.99*latency_size] << "\n"
		  << "\t99.9%: \t" << global_latency[0.999*latency_size] << "\n"
		  << "\t99.99%: \t" << global_latency[0.9999*latency_size] << std::endl;
 
	for(int i=0; i<num_thread; i++){
	    local_load_latency[i].clear();
	    local_load_latency[i].shrink_to_fit();
	}
    }

    if(profile && insert_only){
	for(int i=0; i<num_thread; i++){
	    perf_block[i].printCounters();
	}
    }

    #ifdef BREAKDOWN
    {
	uint64_t time_traversal=0, time_abort=0, time_latch=0, time_node=0, time_split=0, time_consolidation=0;
	for(int i=0; i<num_thread; i++){
            time_traversal += breakdown[i].traversal;
            time_abort += breakdown[i].abort;
            time_latch += breakdown[i].latch;
            time_node += breakdown[i].node;
            time_split += breakdown[i].split;
            time_consolidation += breakdown[i].consolidation;
        }

        auto time_total = time_traversal + time_abort + time_latch + time_node + time_split + time_consolidation;
        std::cout << "------------------- BREAKDOWN -------------------" << std::endl;
        std::cout << "Total:             \t" << time_total << std::endl;
        std::cout << "[Traversal]" << std::endl;
        std::cout << "\tSucess:          \t" << (double)time_traversal/time_total * 100 << " \%" << std::endl;
        std::cout << "\tAbort :          \t" << (double)time_abort/time_total * 100 << " \%" << std::endl;
        std::cout << "[Synchronization]" << std::endl;
        std::cout << "\tLatching:        \t" << (double)time_latch/time_total * 100 << " \%" << std::endl;
        std::cout << "[Operations]" << std::endl;
        std::cout << "\tLeaf Operations: \t" << (double)time_node/time_total * 100 << " \%" << std::endl;
        std::cout << "\tSMOs:            \t" << (double)time_split/time_total * 100 << " \%" << std::endl;
        std::cout << "\tConsolidation:   \t" << (double)time_consolidation/time_total * 100 << " \%" << std::endl;
        std::cout << "-------------------------------------------------" << std::endl;

	memset(breakdown, 0, sizeof(breakdown_t)*num_thread);
    }
    #endif

#ifdef DEBUG
    std::vector<std::thread> debugthreads;
    auto debug_func = [idx, init_kv, init_num](size_t start, size_t end){
	std::vector<uint64_t> v;
	v.reserve(10);
	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);
	for(int i=start; i<end; i++){
	    auto ret = idx->find(init_kv[i].key, &v, ti);
	    if(v[0] != init_kv[i].value){
		std::cout << "found wrong value" << std::endl;
		exit(0);
	    }
	}
    };
    std::cout << "Debug running" << std::endl;
    size_t chunk_size = init_num / build_thread;
    for(int i=0; i<build_thread; i++){
	if(i != build_thread-1)
	    debugthreads.emplace_back(std::thread(debug_func, chunk_size*i, chunk_size*(i+1)));
	else
	    debugthreads.emplace_back(std::thread(debug_func, chunk_size*i, init_num));
    }
    for(auto& t: debugthreads) t.join();
#endif

    // If the workload only executes load phase then we return here
    if(insert_only == true) {
	idx->getMemory();
	idx->find_depth();
	delete idx;
	return;
    }

    #ifdef OPTIMAL
    idx->convert();
    #endif
    //READ/UPDATE/SCAN TEST----------------
    //int txn_num = GetTxnCount(ops, index_type);
    uint64_t sum = 0;
    uint64_t s = 0;

    std::vector<std::chrono::high_resolution_clock::time_point> local_run_latency[num_thread];
    if(measure_latency){
	for(int i=0; i<num_thread; i++){
	    local_run_latency[i].resize(std::ceil(init_num / num_thread * 2));
	    local_run_latency[i].resize(0);
	}
    }

    fprintf(stderr, "# of Txn: %d\n", run_num);

    // Only execute consolidation if BwTree delta chain is used
    #ifdef BWTREE_CONSOLIDATE_AFTER_INSERT
    fprintf(stderr, "Starting consolidating delta chain on each level\n");
    idx->AfterLoadCallback();
    #endif

    earliest_finished = false;
    std::vector<uint64_t> op_num(num_thread);
    auto func_upsert_earliest = [num_thread, idx, run_kv, run_num, &earliest_finished, &op_num, &local_run_latency, measure_latency, sampling_rate, &params, &perf_block, profile, &breakdown](uint64_t thread_id, bool) {
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	size_t total_num_op = run_num;
	size_t op_per_thread = total_num_op / num_thread;
	size_t start_index = op_per_thread * thread_id;
	size_t end_index = start_index + op_per_thread;
	if(thread_id == num_thread-1)
	    end_index = run_num;
	#ifdef BREAKDOWN
	breakdown_t time;
	memset(&time, 0, sizeof(breakdown_t));
	#endif

	PerfEventBlock e;
	if(profile){
	    params[thread_id].setParam("threads", thread_id+1);
	    e.setParam(1, params[thread_id], (thread_id == 0));
	    e.registerCounters();
	    e.startCounters();
	}

	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);

	int counter = 0;
	for(size_t i = start_index;i < end_index;i++) {
	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    idx->upsert(run_kv[i].key, run_kv[i].value, ti);

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    counter++;
	    if(counter % 4096 == 0) {
		ti->rcu_quiesce();
	    }

	    if(earliest_finished){
		ti->rcu_quiesce();
		if(profile){
		    e.stopCounters();
		    perf_block[thread_id] = e;
		}
		op_num[thread_id] = i - start_index;
		#ifdef BREAKDOWN
		idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
		memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
        	#endif
		return;
	    }
	}

	// Perform GC after all operations
	ti->rcu_quiesce();
	earliest_finished = true;
	if(profile){
	    e.stopCounters();
	    perf_block[thread_id] = e;
	}
	op_num[thread_id] = op_per_thread;
	#ifdef BREAKDOWN
        idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
        memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
        #endif
	return;
    };

    auto func_upsert = [num_thread, idx, run_kv, run_num, &local_run_latency, measure_latency, sampling_rate, &params, &perf_block, profile, &breakdown](uint64_t thread_id, bool) {
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	size_t total_num_op = run_num;
	size_t op_per_thread = total_num_op / num_thread;
	size_t start_index = op_per_thread * thread_id;
	size_t end_index = start_index + op_per_thread;
	if(thread_id == num_thread-1)
	    end_index = run_num;
	#ifdef BREAKDOWN
	breakdown_t time;
	memset(&time, 0, sizeof(breakdown_t));
	#endif

	PerfEventBlock e;
	if(profile){
	    params[thread_id].setParam("threads", thread_id+1);
	    e.setParam(1, params[thread_id], (thread_id == 0));
	    e.registerCounters();
	    e.startCounters();
	}

	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);

	int counter = 0;
	for(size_t i = start_index;i < end_index;i++) {
	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    idx->upsert(run_kv[i].key, run_kv[i].value, ti);

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    counter++;
	    if(counter % 4096 == 0) {
		ti->rcu_quiesce();
	    }
	}

	// Perform GC after all operations
	ti->rcu_quiesce();
	if(profile){
	    e.stopCounters();
	    perf_block[thread_id] = e;
	}
	#ifdef BREAKDOWN
        idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
        memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
        #endif
	return;
    };

    auto func_read_earliest = [num_thread, idx, init_kv, run_num, &earliest_finished, &op_num, &local_run_latency, measure_latency, sampling_rate, &params, &perf_block, profile, &breakdown](uint64_t thread_id, bool) {
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	size_t total_num_op = run_num;
	size_t op_per_thread = total_num_op / num_thread;
	size_t start_index = op_per_thread * thread_id;
	size_t end_index = start_index + op_per_thread;
	if(thread_id == num_thread-1)
	    end_index = run_num;
	#ifdef BREAKDOWN
	breakdown_t time;
	memset(&time, 0, sizeof(breakdown_t));
	#endif

	PerfEventBlock e;
	if(profile){
	    params[thread_id].setParam("threads", thread_id+1);
	    e.setParam(1, params[thread_id], (thread_id == 0));
	    e.registerCounters();
	    e.startCounters();
	}

	std::vector<uint64_t> v;
	v.reserve(10);

	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);

	int counter = 0;
	for(size_t i = start_index;i < end_index;i++) {
	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    v.clear();
	    idx->find(init_kv[i].key, &v, ti);

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    counter++;
	    if(counter % 4096 == 0) {
		ti->rcu_quiesce();
	    }

	    if(earliest_finished){
		ti->rcu_quiesce();
		if(profile){
		    e.stopCounters();
		    perf_block[thread_id] = e;
		}
		op_num[thread_id] = i - start_index;
		#ifdef BREAKDOWN
		idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
		memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
        	#endif
		return;
	    }
	}

	// Perform GC after all operations
	ti->rcu_quiesce();
	earliest_finished = true;
	if(profile){
	    e.stopCounters();
	    perf_block[thread_id] = e;
	}
	op_num[thread_id] = op_per_thread;
	#ifdef BREAKDOWN
        idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
        memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
        #endif
	return;
    };

    auto func_read = [num_thread, idx, init_kv, run_num, &local_run_latency, measure_latency, sampling_rate, &params, &perf_block, profile, &breakdown](uint64_t thread_id, bool) {
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	size_t total_num_op = run_num;
	size_t op_per_thread = total_num_op / num_thread;
	size_t start_index = op_per_thread * thread_id;
	size_t end_index = start_index + op_per_thread;
	if(thread_id == num_thread-1)
	    end_index = run_num;

	#ifdef BREAKDOWN
	breakdown_t time;
	memset(&time, 0, sizeof(breakdown_t));
	#endif

	PerfEventBlock e;
	if(profile){
	    params[thread_id].setParam("threads", thread_id+1);
	    e.setParam(1, params[thread_id], (thread_id == 0));
	    e.registerCounters();
	    e.startCounters();
	}

	std::vector<uint64_t> v;
	v.reserve(10);

	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);

	int counter = 0;
	for(size_t i = start_index;i < end_index;i++) {
	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    v.clear();
	    idx->find(init_kv[i].key, &v, ti);

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    counter++;
	    if(counter % 4096 == 0) {
		ti->rcu_quiesce();
	    }
	}

	// Perform GC after all operations
	ti->rcu_quiesce();
	if(profile){
	    e.stopCounters();
	    perf_block[thread_id] = e;
	}
	#ifdef BREAKDOWN
        idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
        memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
        #endif
	return;
    };

    auto func_scan_earliest = [num_thread, idx, run_kv, run_num, ranges, &earliest_finished, &op_num, &local_run_latency, measure_latency, sampling_rate, &params, &perf_block, profile, &breakdown](uint64_t thread_id, bool) {
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	size_t total_num_op = run_num;
	size_t op_per_thread = total_num_op / num_thread;
	size_t start_index = op_per_thread * thread_id;
	size_t end_index = start_index + op_per_thread;
	if(thread_id == num_thread-1)
	    end_index = run_num;
	#ifdef BREAKDOWN
	breakdown_t time;
	memset(&time, 0, sizeof(breakdown_t));
	#endif

	PerfEventBlock e;
	if(profile){
	    params[thread_id].setParam("threads", thread_id+1);
	    e.setParam(1, params[thread_id], (thread_id == 0));
	    e.registerCounters();
	    e.startCounters();
	}

	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);

	int counter = 0;
	for(size_t i = start_index;i < end_index;i++) {
	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    idx->scan(run_kv[i].key, ranges[i], ti);

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    counter++;
	    if(counter % 4096 == 0) {
		ti->rcu_quiesce();
	    }

	    if(earliest_finished){
		ti->rcu_quiesce();
		if(profile){
		    e.stopCounters();
		    perf_block[thread_id] = e;
		}
		op_num[thread_id] = i - start_index;
		#ifdef BREAKDOWN
		idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
		memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
        	#endif
		return;
	    }
	}

	// Perform GC after all operations
	ti->rcu_quiesce();
	earliest_finished = true;
	if(profile){
	    e.stopCounters();
	    perf_block[thread_id] = e;
	}
	#ifdef BREAKDOWN
        idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
        memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
        #endif
	op_num[thread_id] = op_per_thread;
	return;
    };

    auto func_scan = [num_thread, idx, run_kv, run_num, ranges, &local_run_latency, measure_latency, sampling_rate, &params, &perf_block, profile, &breakdown](uint64_t thread_id, bool) {
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	size_t total_num_op = run_num;
	size_t op_per_thread = total_num_op / num_thread;
	size_t start_index = op_per_thread * thread_id;
	size_t end_index = start_index + op_per_thread;
	if(thread_id == num_thread-1)
	    end_index = run_num;
	#ifdef BREAKDOWN
	breakdown_t time;
	memset(&time, 0, sizeof(breakdown_t));
	#endif

	PerfEventBlock e;
	if(profile){
	    params[thread_id].setParam("threads", thread_id+1);
	    e.setParam(1, params[thread_id], (thread_id == 0));
	    e.registerCounters();
	    e.startCounters();
	}

	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);

	int counter = 0;
	for(size_t i = start_index;i < end_index;i++) {
	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    idx->scan(run_kv[i].key, ranges[i], ti);

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    counter++;
	    if(counter % 4096 == 0) {
		ti->rcu_quiesce();
	    }
	}

	// Perform GC after all operations
	ti->rcu_quiesce();
	if(profile){
	    e.stopCounters();
	    perf_block[thread_id] = e;
	}
	#ifdef BREAKDOWN
        idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
        memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
        #endif
	return;
    };


    if(memory_bandwidth){
	std::cerr << "Ready to profile" << std::endl;
	getchar();
    }
    start_time = get_now();  
    if(earliest){
	if(wl == OP_UPSERT)
	    StartThreads(idx, num_thread, func_upsert_earliest, false);
	else if(wl == OP_READ)
	    StartThreads(idx, num_thread, func_read_earliest, false);
	else
	    StartThreads(idx, num_thread, func_scan_earliest, false);
    }
    else{
	if(wl == OP_UPSERT)
	    StartThreads(idx, num_thread, func_upsert, false);
	else if(wl == OP_READ)
	    StartThreads(idx, num_thread, func_read, false);
	else
	    StartThreads(idx, num_thread, func_scan, false);
    }
    end_time = get_now();
    
    if(memory_bandwidth){
	std::cerr << "End of profile" << std::endl;
	getchar();
    }
    
    if(earliest){
	uint64_t _run_num = 0;
	for(int i=0; i<num_thread; i++){
	    _run_num += op_num[i];
	}
	std::cout << "Processed " << _run_num << " / " << run_num << " (" << (double)_run_num/run_num*100 << " \%)" << std::endl;
	run_num = _run_num;
    }

    tput = run_num / (end_time - start_time) / 1000000; //Mops/sec
    std::cout << "Elapsed time: " << (double)(end_time - start_time) << " sec" << std::endl;

    if (wl == OP_UPSERT)
	std::cout << "Workload Update " << tput << std::endl; 
    else if (wl == OP_READ)
	std::cout << "Workload Read " << tput << std::endl; 
    else if (wl == OP_SCAN)
	std::cout << "Workload Scan " << tput << std::endl; 
    else{
	fprintf(stderr, "Unknown workload type: %d\n", wl);
    }


    if(measure_latency){
	std::vector<uint64_t> global_latency;
	uint64_t total_latency = 0;
	for(auto& v: local_run_latency){
	    for(auto i=0; i<v.size(); i+=2){
		auto lat = std::chrono::nanoseconds(v[i+1] - v[i]).count();
		total_latency += lat;
		global_latency.push_back(lat);
	    }
	}

	std::sort(global_latency.begin(), global_latency.end());
	auto latency_size = global_latency.size();
	std::cout << "Latency observed (" << latency_size << ") \n"
	          << "\tavg: \t" << total_latency / latency_size << "\n"
		  << "\tmin: \t" << global_latency[0] << "\n"
		  << "\t50%: \t" << global_latency[0.5*latency_size] << "\n"
		  << "\t90%: \t" << global_latency[0.9*latency_size] << "\n"
		  << "\t95%: \t" << global_latency[0.95*latency_size] << "\n"
		  << "\t99%: \t" << global_latency[0.99*latency_size] << "\n"
		  << "\t99.9%: \t" << global_latency[0.999*latency_size] << "\n"
		  << "\t99.99%: \t" << global_latency[0.9999*latency_size] << std::endl; 

	for(int i=0; i<num_thread; i++){
	    local_run_latency[i].clear();
	    local_run_latency[i].shrink_to_fit();
	}
    }

    if(profile){
	for(int i=0; i<num_thread; i++){
	    perf_block[i].printCounters();
	}
    }


    #ifdef BREAKDOWN
    {
	uint64_t time_traversal=0, time_abort=0, time_latch=0, time_node=0, time_split=0, time_consolidation=0;
	for(int i=0; i<num_thread; i++){
            time_traversal += breakdown[i].traversal;
            time_abort += breakdown[i].abort;
            time_latch += breakdown[i].latch;
            time_node += breakdown[i].node;
            time_split += breakdown[i].split;
            time_consolidation += breakdown[i].consolidation;
        }

        auto time_total = time_traversal + time_abort + time_latch + time_node + time_split + time_consolidation;
        std::cout << "------------------- BREAKDOWN -------------------" << std::endl;
        std::cout << "Total:             \t" << time_total << std::endl;
        std::cout << "[Traversal]" << std::endl;
        std::cout << "\tSucess:          \t" << (double)time_traversal/time_total * 100 << " \%" << std::endl;
        std::cout << "\tAbort :          \t" << (double)time_abort/time_total * 100 << " \%" << std::endl;
        std::cout << "[Synchronization]" << std::endl;
        std::cout << "\tLatching:        \t" << (double)time_latch/time_total * 100 << " \%" << std::endl;
        std::cout << "[Operations]" << std::endl;
        std::cout << "\tLeaf Operations: \t" << (double)time_node/time_total * 100 << " \%" << std::endl;
        std::cout << "\tSMOs:            \t" << (double)time_split/time_total * 100 << " \%" << std::endl;
        std::cout << "\tConsolidation:   \t" << (double)time_consolidation/time_total * 100 << " \%" << std::endl;
        std::cout << "-------------------------------------------------" << std::endl;
	
    }
    #endif

    delete idx;
    return;
}

int main(int argc, char *argv[]) {

    options_t opt;
    try{
	cxxopts::Options options("Index-bench", "Benchmark framework for in-memory concurrent index structures.");
	options.positional_help("INPUT").show_positional_help();

	options.add_options()
	    ("workload", "Workload type (load,update,read,scan)", cxxopts::value<std::string>())
	    ("init_num", "Size of workload to load", cxxopts::value<uint32_t>()->default_value(std::to_string(opt.num)))
	    ("run_num", "Size of workload to run", cxxopts::value<uint32_t>()->default_value(std::to_string(opt.num)))
	    ("index", "Index type (artolc, artrowex, hot, masstree, btreeolc, blink, bwtree)", cxxopts::value<std::string>())
	    ("threads", "Number of threads to run", cxxopts::value<uint32_t>()->default_value(std::to_string(opt.threads)))
	    ("mem", "Measure memory bandwidth", cxxopts::value<bool>()->default_value((opt.mem ? "true" : "false")))
	    ("profile", "Enable CPU profiling", cxxopts::value<bool>()->default_value((opt.profile? "true" : "false")))
	    ("latency", "Sample latency of requests", cxxopts::value<float>()->default_value(std::to_string(opt.sampling_latency)))
	    ("hyper", "Enable hyper threading", cxxopts::value<bool>()->default_value((opt.hyper ? "true" : "false")))
	    ("insert_only", "Skip running transactions", cxxopts::value<bool>()->default_value((opt.insert_only ? "true" : "false")))
	    ("earliest", "Measure performance based on the earliest finished thread", cxxopts::value<bool>()->default_value((opt.earliest ? "true" : "false")))
	    ("help", "Print help")
	    ;

	auto result = options.parse(argc, argv);
	if(result.count("help")){
	    std::cout << options.help() << std::endl;
	    exit(0);
	}

	if(result.count("mem"))
	    opt.mem = result["mem"].as<bool>();

	if(result.count("profile"))
	    opt.profile = result["profile"].as<bool>();

	if(result.count("latency"))
	    opt.sampling_latency = result["latency"].as<float>();

	if(result.count("init_num"))
	    opt.init_num = result["init_num"].as<uint32_t>();
	else{
	    std::cout << "Missing load size" << std::endl;
	    std::cout << options.help() << std::endl;
	    exit(0);
	}

	if(result.count("run_num"))
	    opt.run_num = result["run_num"].as<uint32_t>();

	if(result.count("workload"))
	    opt.workload = result["workload"].as<std::string>();

	if(result.count("index"))
	    opt.index = result["index"].as<std::string>();
	else{
	    std::cout << "Missing index type" << std::endl;
	    std::cout << options.help() << std::endl;
	    exit(0);
	}

	if(result.count("threads"))
	    opt.threads = result["threads"].as<uint32_t>();

	if(result.count("hyper"))
	    opt.hyper = result["hyper"].as<bool>();

	if(result.count("insert_only"))
	    opt.insert_only = result["insert_only"].as<bool>();

	if(result.count("earliest"))
	    opt.earliest = result["earliest"].as<bool>();

    }catch(const cxxopts::OptionException& e){
	std::cout << "Error parsing options: " << e.what() << std::endl;
	exit(0);
    }

    if(opt.threads == 0){
	std::cout << "Number of threads should be larger than 0: " << opt.threads << std::endl;
	exit(0);
    }

    if(opt.run_num < opt.init_num){
	std::cout << "Load size should be larger than run size" << std::endl;
	exit(0);
    }

    std::cerr << opt << std::endl;

    int wl;
    if(opt.workload.compare("load") == 0)
	wl = OP_INSERT;
    else if(opt.workload.compare("update") == 0)
	wl = OP_UPSERT;
    else if(opt.workload.compare("read") == 0)
	wl = OP_READ;
    else if(opt.workload.compare("scan") == 0)
	wl = OP_SCAN;
    else{
	std::cout << "Invalid workload type: " << opt.workload << std::endl;
	exit(0);
    }

    int index_type;
    if(opt.index == "artolc")
	index_type = TYPE_ARTOLC;
    else if(opt.index == "artrowex")
	index_type = TYPE_ARTROWEX;
    else if(opt.index == "hot")
	index_type = TYPE_HOT;
    else if(opt.index == "masstree")
	index_type = TYPE_MASSTREE;
    else if(opt.index == "bwtree")
	index_type = TYPE_BWTREE;
    else if(opt.index == "btreeolc")
	index_type = TYPE_BTREEOLC;
    else if(opt.index == "blink")
	index_type = TYPE_BLINKTREE;
    else if(opt.index == "blinkhash")
	index_type = TYPE_BLINKHASH;
    else{
	std::cout << "Invalid index type: " << opt.index << std::endl;
	exit(0);
    }
	
    if(opt.hyper == true)
	hyperthreading = true;
    else
	hyperthreading = false;

    if(opt.insert_only == true)
	insert_only = true;
    else{
	if(wl == OP_INSERT)
	    insert_only = true;
	else
	    insert_only = false;
    }

    if(opt.mem == true)
	memory_bandwidth = true;
    else
	memory_bandwidth = false;

    if(opt.profile == true)
	profile = true;
    else
	profile = false;

    if(opt.earliest == true)
	earliest = true;
    else
	earliest = false;

    int init_num, run_num;
    if(opt.init_num != 0){
	init_num = opt.init_num;
	run_num = opt.run_num;
    }
    else{
	std::cout << "Workload size is not defined!" << std::endl;
	exit(0);
    }

    sampling_rate = opt.sampling_latency;
    if(sampling_rate != 0.0)
	measure_latency = true;

    int num_thread = opt.threads;

    // If we do not interleave threads on two sockets then this will be printed
    if(hyperthreading == true) {
	fprintf(stderr, "  Hyperthreading enabled\n");
    }

    if(memory_bandwidth == true) {
	if(geteuid() != 0) {
	    fprintf(stderr, "Please run the program as root in order to measure memory bandwidth\n");
	    exit(1);
	}

	fprintf(stderr, "  Measuring memory bandwidth\n");
    }

    if(insert_only == true) {
	fprintf(stderr, "Program will exit after insert operation\n");
    }

    if(measure_latency == true){
	fprintf(stderr, "Measuring latency with sampling rate of %lf\n", sampling_rate);
    }

    kvpair_t<keytype>* init_kv = new kvpair_t<keytype>[init_num];
    kvpair_t<keytype>* run_kv = new kvpair_t<keytype>[run_num];
    int* ranges = new int[run_num];

    memset(&init_kv[0], 0x00, init_num * sizeof(kvpair_t<keytype>));
    memset(&run_kv[0], 0x00, run_num * sizeof(kvpair_t<keytype>));
    memset(&ranges[0], 0x00, run_num * sizeof(int));

    load(wl, index_type, init_kv, init_num, run_kv, run_num, ranges);
    printf("Finished loading workload file (mem = %lu)\n", MemUsage());
    if(index_type != TYPE_NONE) {
	exec(wl, index_type, num_thread, init_kv, init_num, run_kv, run_num, ranges);
	printf("Finished running benchmark (mem = %lu)\n", MemUsage());
    }
    else {
	fprintf(stderr, "Type None is selected - no execution phase\n");
    }
    //exit_cleanup();

    return 0;
}
