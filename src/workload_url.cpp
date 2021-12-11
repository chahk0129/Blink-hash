#include "include/microbench.h"
#include "include/perf_event.h"

#include <cstring>
#include <cctype>
#include <atomic>

//#define DEBUG
#define URL_KEYS
#include "include/options.h"
#include "include/cxxopts.hpp"
#include "include/index.h"

// Used for skiplist
thread_local long skiplist_steps = 0;
std::atomic<long> skiplist_total_steps;

typedef GenericKey<128> keytype;
typedef GenericComparator<128> keycomp;

extern bool hyperthreading;

using KeyEuqalityChecker = GenericEqualityChecker<128>;
using KeyHashFunc = GenericHasher<128>;

static const uint64_t key_type=1;
static const uint64_t value_type=1; // 0 = random pointers, 1 = pointers to keys

#include "include/util.h"

// Whether to exit after insert operation
static bool insert_only = false;
// Wether measure latency
static bool measure_latency = false;
static float sampling_rate = 0;
// Whether measure preformance based on earliest finished thread
static bool earliest = false;
// This is the flag for whather to measure memory bandwidth
static bool memory_bandwidth = false;
// Whether to enable CPU profiling
static bool profile = false;

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
inline void load(std::string input, int wl, int kt, int index_type, kvpair_t<keytype>* init_kv, int init_num, kvpair_t<keytype>* run_kv, int& run_num, int* ranges, int* ops) {
    auto back = input.back();
    if(back != '/')
	input.append("/");
    std::string init_file = input;
    std::string txn_file = input;

    if(kt == URL_KEY){
	init_file.append("load_url_70M.dat");
	if(wl == WORKLOAD_A)
	    txn_file.append("txnsa_url_70M.dat");
	else if(wl == WORKLOAD_B)
	    txn_file.append("txnsb_url_70M.dat");
	else if(wl == WORKLOAD_C)
	    txn_file.append("txnsc_url_70M.dat");
	else if(wl == WORKLOAD_E)
	    txn_file.append("txnse_url_70M.dat");
	else if(wl == WORKLOAD_MIXED)
	    txn_file.append("txns_mixed_url_70M.dat");
	else{
	    if(wl != WORKLOAD_LOAD){
	        std::cout << "Invalid workload type" << std::endl;
	        exit(0);
	    }
	}
    }
    else{
	std::cout << "Invalid key type" << std::endl;
	exit(0);
    }

    std::ifstream infile_load(init_file);
    if(!infile_load.is_open()){
	std::cout << "Failed to open load input: " << init_file << std::endl;
	exit(0);
    }

    void *base_ptr = malloc(8);
    uint64_t base = (uint64_t)(base_ptr);
    free(base_ptr);

    std::string op;
    std::string key_str;
    keytype key;
    int range;

    std::string insert("INSERT");
    std::string read("READ");
    std::string update("UPDATE");
    std::string scan("SCAN");

    for(int i=0; i<init_num; i++){
	infile_load >> op >> key_str;
	if(op.compare(insert) != 0){
	    std::cout << "READING LOAD FILE FAIL!" << std::endl;
	    return;
	}
	key.setFromString(key_str);
	init_kv[i].key = key;
	init_kv[i].value = reinterpret_cast<uint64_t>(&init_kv[i].key);
    }

    uint64_t value = 0;
    kvpair_t<keytype>* init_kv_data = &init_kv[0];


    // For insert only mode we return here
    if(insert_only == true) {
	return;
    }

    std::ifstream infile_txn(txn_file);
    if(!infile_txn.is_open()){
	std::cout << "Failed to open txn input: " << txn_file << std::endl;
	exit(0);
    }
    for(int i=0; i<run_num; i++){
	infile_txn >> op >> key_str;
	key.setFromString(key_str);

	if (op.compare(insert) == 0) {
	    ops[i] = OP_INSERT;
	    run_kv[i].key = key;
	    run_kv[i].value = reinterpret_cast<uint64_t>(&run_kv[i].key);
	    ranges[i] = 1;
	}
	else if (op.compare(read) == 0) {
	    ops[i] = OP_READ;
	    run_kv[i].key = key;
	}
	else if (op.compare(update) == 0) {
	    ops[i] = OP_UPSERT;
	    run_kv[i].key = key;
	    run_kv[i].value = reinterpret_cast<uint64_t>(&run_kv[i].key);
	}
	else if (op.compare(scan) == 0) {
	    infile_txn >> range;
	    ops[i] = OP_SCAN;
	    run_kv[i].key = key;
	    ranges[i] = range;
	}
	else {
	    std::cout << "UNRECOGNIZED CMD!\n";
	    return;
	}
    }

    std::cout << "Finished loading workload file\n";

}

//==============================================================
// EXEC
//==============================================================
inline void exec(int wl, int index_type, int num_thread, kvpair_t<keytype>* init_kv, int init_num, kvpair_t<keytype>* run_kv, int run_num, int* ranges, int* ops){

    Index<keytype, keycomp> *idx = getInstance<keytype, keycomp, KeyEuqalityChecker, KeyHashFunc>(index_type, key_type);

    std::vector<std::chrono::high_resolution_clock::time_point> local_load_latency[num_thread];
    if(measure_latency){
	for(int i=0; i<num_thread; i++){
	    local_load_latency[i].resize(std::ceil(init_num / num_thread * 2));
	    local_load_latency[i].resize(0);
	}
    }

    // WRITE ONLY TEST--------------
    bool earliest_finished = false;

    BenchmarkParameters params[num_thread];
    PerfEventBlock perf_block[num_thread];

    std::vector<uint64_t> inserted_num(num_thread);
    breakdown_t breakdown[num_thread];
    memset(breakdown, 0, sizeof(breakdown_t)*num_thread);

    auto func_earliest = [idx, init_kv, init_num, num_thread, &earliest_finished, &inserted_num, &local_load_latency, measure_latency, sampling_rate, &params, &perf_block, profile, insert_only, &breakdown](uint64_t thread_id, bool) {
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	size_t total_num_key = init_num;
	size_t key_per_thread = total_num_key / num_thread;
	size_t start_index = key_per_thread * thread_id;
	size_t end_index = start_index + key_per_thread;
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

	int counter = 0;
	for(size_t i = start_index;i < end_index;i++) {
	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();

	    if(measure_latency_)
		local_load_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    idx->insert(init_kv[i].key, init_kv[i].value, ti);

	    if(measure_latency_)
		local_load_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    counter++;
	    if(counter % 4096 == 0) {
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

    auto func = [idx, init_kv, init_num, num_thread, &local_load_latency, measure_latency, sampling_rate, &params, &perf_block, profile, insert_only, &breakdown](uint64_t thread_id, bool) {
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	size_t total_num_key = init_num;
	size_t key_per_thread = total_num_key / num_thread;
	size_t start_index = key_per_thread * thread_id;
	size_t end_index = start_index + key_per_thread;
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

	int counter = 0;
	for(size_t i = start_index;i < end_index;i++) {
	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();

	    if(measure_latency_)
		local_load_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    idx->insert(init_kv[i].key, init_kv[i].value, ti);

	    if(measure_latency_)
		local_load_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    counter++;
	    if(counter % 4096 == 0) {
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


    if(memory_bandwidth && insert_only){
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
    if(memory_bandwidth && insert_only){
	std::cout << "End of profile" << std::endl;
	getchar();
    }

    if(earliest && insert_only){
	uint64_t _count = 0;
	for(int i=0; i<num_thread; i++){
	    _count += inserted_num[i];
	}
	std::cout << "Processed " << _count << " / " << init_num << " (" << (double)_count/init_num*100 << " \%)" << std::endl;
	init_num = _count;
    }

    double tput = init_num / (end_time - start_time) / 1000000; //Mops/sec
    if(wl == WORKLOAD_LOAD)
	std::cout << "Workload Load " << tput << std::endl;
    else
	std::cout << "Load " << tput << std::endl;

    if(measure_latency){
	std::vector<uint64_t> global_latency;
	for(auto& v: local_load_latency){
	    for(auto i=0; i<v.size(); i+=2){
		global_latency.push_back(std::chrono::nanoseconds(v[i+1] - v[i]).count());
	    }
	}

	std::sort(global_latency.begin(), global_latency.end());
	auto latency_size = global_latency.size();
	std::cout << "Latency observed (" << latency_size << ") \n"
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
	std::cout << "Total: 		 \t" << time_total << std::endl;
	std::cout << "[Traversal]" << std::endl;
	std::cout << "\tSucess: 	 \t" << (double)time_traversal/time_total * 100 << " \%" << std::endl;
	std::cout << "\tAbort : 	 \t" << (double)time_abort/time_total * 100 << " \%" << std::endl;
	std::cout << "[Synchronization]" << std::endl;
	std::cout << "\tLatching: 	 \t" << (double)time_latch/time_total * 100 << " \%" << std::endl;
	std::cout << "[Operations]" << std::endl;
	std::cout << "\tNode Operations: \t" << (double)time_node/time_total * 100 << " \%" << std::endl;
	std::cout << "\tSMOs:		 \t" << (double)time_split/time_total * 100 << " \%" << std::endl;
	std::cout << "\tConsolidation:	 \t" << (double)time_consolidation/time_total * 100 << " \%" << std::endl;
	std::cout << "-------------------------------------------------" << std::endl;
    }
    memset(breakdown, 0, sizeof(breakdown_t)*num_thread);
    #endif


#ifdef DEBUG
    std::vector<std::thread> debugthreads;
    auto debug_func = [&idx, &init_kv, init_num, num_thread](size_t start, size_t end){
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
    size_t chunk_size = init_num / num_thread;
    for(int i=0; i<num_thread; i++){
	if(i != num_thread-1)
	    debugthreads.emplace_back(std::thread(debug_func, chunk_size*i, chunk_size*(i+1)));
	else
	    debugthreads.emplace_back(std::thread(debug_func, chunk_size*i, init_num));
    }
    for(auto& t: debugthreads) t.join();
#endif

    if(insert_only == true) {
	idx->getMemory();
	idx->find_depth();
	delete idx;
	return;
    }

#ifdef BWTREE_CONSOLIDATE_AFTER_INSERT
    fprintf(stderr, "Starting consolidating delta chain on each level\n");
    idx->AfterLoadCallback();
#endif
    //READ/UPDATE/SCAN TEST----------------

    std::vector<std::chrono::high_resolution_clock::time_point> local_run_latency[num_thread];
    if(measure_latency){
	for(int i=0; i<num_thread; i++){
	    local_run_latency[i].resize(std::ceil(init_num / num_thread * 2));
	    local_run_latency[i].resize(0);
	}
    }

    int txn_num = run_num; //GetTxnCount(ops, index_type);
    uint64_t sum = 0;

    fprintf(stderr, "# of Txn: %d\n", txn_num);

    earliest_finished = false;
    std::vector<uint64_t> op_num(num_thread);
    auto func2_earliest = [num_thread, idx, run_kv, &earliest_finished, &op_num, run_num, ranges, ops, &local_run_latency, measure_latency, sampling_rate, &params, &perf_block, profile, &breakdown](uint64_t thread_id, bool) {
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	size_t total_num_op = run_num; 
	size_t op_per_thread = total_num_op / num_thread;
	size_t start_index = op_per_thread * thread_id;
	size_t end_index = start_index + op_per_thread;
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

	    int op = ops[i];

	    if (op == OP_INSERT) { //INSERT
		idx->insert(run_kv[i].key, run_kv[i].value, ti);
	    }
	    else if (op == OP_READ) { //READ
		v.clear();
		idx->find(run_kv[i].key, &v, ti);
	    }
	    else if (op == OP_UPSERT) { //UPDATE
		idx->upsert(run_kv[i].key, run_kv[i].value, ti);
	    }
	    else if (op == OP_SCAN) { //SCAN
		idx->scan(run_kv[i].key, ranges[i], ti);
	    }

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


    auto func2 = [num_thread, idx, run_kv, run_num, ranges, ops, &local_run_latency, measure_latency, sampling_rate, &params, &perf_block, profile, &breakdown](uint64_t thread_id, bool) {
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	size_t total_num_op = run_num; 
	size_t op_per_thread = total_num_op / num_thread;
	size_t start_index = op_per_thread * thread_id;
	size_t end_index = start_index + op_per_thread;
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

	    int op = ops[i];

	    if (op == OP_INSERT) { //INSERT
		idx->insert(run_kv[i].key, run_kv[i].value, ti);
	    }
	    else if (op == OP_READ) { //READ
		v.clear();
		idx->find(run_kv[i].key, &v, ti);
	    }
	    else if (op == OP_UPSERT) { //UPDATE
		idx->upsert(run_kv[i].key, run_kv[i].value, ti);
	    }
	    else if (op == OP_SCAN) { //SCAN
		idx->scan(run_kv[i].key, ranges[i], ti);
	    }

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    counter++;
	    if(counter % 4096 == 0) {
		ti->rcu_quiesce();
	    }
	}

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
    if(earliest)
	StartThreads(idx, num_thread, func2_earliest, false);
    else
	StartThreads(idx, num_thread, func2, false);
    end_time = get_now();
    if(memory_bandwidth){
	std::cerr << "End of profile" << std::endl;
	getchar();
    }

    if(earliest){
	uint64_t _count = 0;
	for(int i=0; i<num_thread; i++){
	    _count += op_num[i];
	}
	std::cout << "Processed " << _count << " / " << txn_num << " (" << (double)_count/txn_num*100 << " \%)" << std::endl;
	txn_num = _count;
    }

    tput = txn_num / (end_time - start_time) / 1000000; //Mops/sec
    std::cout << "Elapsed time: " << (double)(end_time - start_time) << " sec" << std::endl;

    if (wl == WORKLOAD_A) 
	std::cout << "Workload A " << tput << std::endl;
    else if (wl == WORKLOAD_B) 
	std::cout << "Workload B " << tput << std::endl;
    else if (wl == WORKLOAD_C) 
	std::cout << "Workload C " << tput << std::endl;
    else if (wl == WORKLOAD_E) 
	std::cout << "Workload E " << tput << std::endl;
    else if (wl == WORKLOAD_MIXED) 
	std::cout << "Workload MIXED " << tput << std::endl;
    else{ 
	std::cout << "Unknown worklaod type " << tput << std::endl; 
	exit(0);
    }

    if(measure_latency){
	std::vector<uint64_t> global_latency;
	for(auto& v: local_run_latency){
	    for(auto i=0; i<v.size(); i+=2){
		global_latency.push_back(std::chrono::nanoseconds(v[i+1] - v[i]).count());
	    }
	}

	std::sort(global_latency.begin(), global_latency.end());
	auto latency_size = global_latency.size();
	std::cout << "Latency observed (" << latency_size << ") \n"
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
	std::cout << "Total: 		 \t" << time_total << std::endl;
	std::cout << "[Traversal]" << std::endl;
	std::cout << "\tSucess: 	 \t" << (double)time_traversal/time_total * 100 << " \%" << std::endl;
	std::cout << "\tAbort : 	 \t" << (double)time_abort/time_total * 100 << " \%" << std::endl;
	std::cout << "[Synchronization]" << std::endl;
	std::cout << "\tLatching: 	 \t" << (double)time_latch/time_total * 100 << " \%" << std::endl;
	std::cout << "[Operations]" << std::endl;
	std::cout << "\tNode Operations: \t" << (double)time_node/time_total * 100 << " \%" << std::endl;
	std::cout << "\tSMOs:		 \t" << (double)time_split/time_total * 100 << " \%" << std::endl;
	std::cout << "\tConsolidation:	 \t" << (double)time_consolidation/time_total * 100 << " \%" << std::endl;
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
	    ("input", "Absolute path to workload directory", cxxopts::value<std::string>())
	    ("workload", "Workload type (a,b,c,e)", cxxopts::value<std::string>())
	    ("key_type", "Key type (url)", cxxopts::value<std::string>())
	    ("index", "Index type (artolc, artrowex, hot, masstree, blink, bwtree)", cxxopts::value<std::string>())
	    ("mem", "Measure memory bandwidth", cxxopts::value<bool>()->default_value((opt.mem ? "true" : "false")))
	    ("profile", "Enable CPU profiling", cxxopts::value<bool>()->default_value((opt.profile ? "true" : "false")))
	    ("threads", "Number of threads to run", cxxopts::value<uint32_t>()->default_value(std::to_string(opt.threads)))
	    ("latency", "Sample latency of requests", cxxopts::value<float>()->default_value(std::to_string(opt.sampling_latency)))
	    ("hyper", "Enable hyper threading", cxxopts::value<bool>()->default_value((opt.hyper ? "true" : "false")))
	    ("insert_only", "Skip running transactions", cxxopts::value<bool>()->default_value((opt.insert_only ? "true" : "false")))
	    ("earliest", "Measure performance based on the earliest finished thread", cxxopts::value<bool>()->default_value((opt.earliest ? "true" : "false")))
	    ("help", "Print help")
	    ;

	options.parse_positional({"input"});

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

	if(result.count("input"))
	    opt.input = result["input"].as<std::string>();
	else{
	    std::cout << "Missing input path" << std::endl;
	    std::cout << options.help() << std::endl;
	    exit(0);
	}

	if(result.count("workload"))
	    opt.workload = result["workload"].as<std::string>();
	else{
	    std::cout << "Missing workload type" << std::endl;
	    std::cout << options.help() << std::endl;
	    exit(0);
	}

	if(result.count("index"))
	    opt.index = result["index"].as<std::string>();
	else{
	    std::cout << "Missing index type" << std::endl;
	    std::cout << options.help() << std::endl;
	    exit(0);
	}

	if(result.count("threads"))
	    opt.threads = result["threads"].as<uint32_t>();

	if(result.count("key_type"))
	    opt.key_type = result["key_type"].as<std::string>();

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

    std::cerr << opt << std::endl;

    int wl;
    if(opt.workload.compare("load") == 0)
	wl = WORKLOAD_LOAD;
    else if(opt.workload.compare("a") == 0)
	wl = WORKLOAD_A;
    else if(opt.workload.compare("b") == 0)
	wl = WORKLOAD_B;
    else if(opt.workload.compare("c") == 0)
	wl = WORKLOAD_C;
    else if(opt.workload.compare("e") == 0)
	wl = WORKLOAD_E;
    else if(opt.workload.compare("mixed") == 0)
	wl = WORKLOAD_MIXED;
    else{
	std::cout << "Invalid workload type: " << opt.workload << std::endl;
	exit(0);
    }

    int kt;
    if(opt.key_type.compare("url") == 0)
	kt = URL_KEY;
    else{
	std::cout << "Invalid key type: " << opt.key_type << std::endl;
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
    else if(opt.index == "blink")
	index_type = TYPE_BLINKTREE;
    else if(opt.index == "btreeolc")
	index_type = TYPE_BTREEOLC;
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
	if(wl == WORKLOAD_LOAD)
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

    std::string input = opt.input;
    sampling_rate = opt.sampling_latency;
    if(sampling_rate != 0.0)
	measure_latency = true;

    int num_thread = opt.threads;
    int init_num = 70000000;
    int run_num = 70000000;
    kvpair_t<keytype>* init_kv = new kvpair_t<keytype>[init_num];
    kvpair_t<keytype>* run_kv = new kvpair_t<keytype>[run_num];
    int* ranges = new int[run_num];
    int* ops = new int[run_num]; //INSERT = 0, READ = 1, UPDATE = 2

    load(input, wl, kt, index_type, init_kv, init_num, run_kv, run_num, ranges, ops);
    fprintf(stderr, "Finish loading (Mem = %lu)\n", MemUsage());

    exec(wl, index_type, num_thread, init_kv, init_num, run_kv, run_num, ranges, ops);
    fprintf(stderr, "Finished execution (Mem = %lu)\n", MemUsage());

    return 0;
}
