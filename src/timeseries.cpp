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
// Fuzzy insertion latency
static uint64_t fuzzy = 0;
// CPU frequency
static uint64_t frequency = 2600; 
// Random insertion rate
static float random_rate = 0;

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


inline void run(int index_type, int wl, int num_thread, int num){
    Index<keytype, keycomp>* idx = getInstance<keytype, keycomp>(index_type, key_type);
    std::vector<std::chrono::high_resolution_clock::time_point> local_load_latency[num_thread];
    if(measure_latency){
	for(int i=0; i<num_thread; i++){
	    local_load_latency[i].resize(std::ceil(num / num_thread * 2));
	    local_load_latency[i].resize(0);
	}
    }

    BenchmarkParameters params[num_thread];
    PerfEventBlock perf_block[num_thread];

    bool earliest_finished = false;
    std::vector<uint64_t> inserted_num(num_thread);
    breakdown_t breakdown[num_thread];
    std::vector<kvpair_t<keytype>> keys[num_thread];

    std::vector<std::pair<kvpair_t<keytype>, std::pair<int, uint64_t>>> load_ops;
    load_ops.reserve(num);
    int sensor_id = 0;
    for(int i=0; i<num; i++){
	auto r = (rand() % 100) / 100.0;
	if(r < random_rate){
	    auto kv = new kvpair_t<keytype>;
	    kv->key = ((Rdtsc() << 16) | sensor_id++ << 6);
	    kv->value = reinterpret_cast<uint64_t>(&kv->key);
	    load_ops.push_back(std::make_pair(*kv, std::make_pair(OP_RANDOMINSERT, 0)));
	    if(sensor_id == 1024) sensor_id = 0;
	}
	else{
	    auto kv = new kvpair_t<keytype>;
	    uint64_t latency = 0;
	    if(fuzzy)
		latency = rand() % (fuzzy * frequency);
	    load_ops.push_back(std::make_pair(*kv, std::make_pair(OP_INSERT, latency)));
	}
    }
    std::random_shuffle(load_ops.begin(), load_ops.end());

    size_t chunk = num / num_thread;
    for(int i=0; i<num_thread; i++)
	keys[i].reserve(chunk);

    auto load_earliest = [idx, num, num_thread, &earliest_finished, &inserted_num, &local_load_latency, &breakdown, &params, &perf_block, profile, &load_ops](uint64_t thread_id, bool){
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);
	size_t chunk = num / num_thread;
	size_t start = chunk * thread_id;
	size_t end = chunk * (thread_id + 1);
	if(end > num) end = num;
	kvpair_t<keytype>* kv = new kvpair_t<keytype>[chunk];
	#ifdef BREAKDOWN
	breakdown_t time;
	memset(&time, 0, sizeof(breakdown_t));
	#endif
	int gc_counter = 0;
	int sensor_id = 0;

	PerfEventBlock e;
	if(insert_only && profile){
	    params[thread_id].setParam("threads", thread_id+1);
	    e.setParam(1, params[thread_id], (thread_id == 0));
	    e.registerCounters();
	    e.startCounters();
	}

	int j = 0;
	for(auto i=start; i<end; i++, j++){
	    auto op = load_ops[i].second.first;
	    auto lat = load_ops[i].second.second;
	    if(op == OP_INSERT){
		kv[j].key = (((Rdtsc() - lat) << 16) | sensor_id++ << 6) | thread_id;
		kv[j].value = reinterpret_cast<uint64_t>(&kv[j].key);
	    }
	    else{
		kv[j].key = load_ops[i].first.key;
		kv[j].value = reinterpret_cast<uint64_t>(&kv[j].key);
	    }

	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();
	    
	    if(measure_latency_)
		local_load_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    idx->insert(kv[j].key, kv[j].value, ti);

	    if(measure_latency_)
		local_load_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    gc_counter++;
	    if(gc_counter % 4096 == 0) {
		ti->rcu_quiesce();
	    }

	    if(sensor_id == 1024)
		sensor_id = 0;

	    if(earliest_finished){
		ti->rcu_quiesce();
		if(insert_only && profile){
		    e.stopCounters();
		    perf_block[thread_id] = e;
		}
		inserted_num[thread_id] = j;
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
	inserted_num[thread_id] = chunk;
	#ifdef BREAKDOWN
	idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
	memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
	#endif
    };

    auto load = [idx, num, num_thread, &local_load_latency, &keys, &params, &perf_block, profile, &breakdown,  &load_ops](uint64_t thread_id, bool){
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);
	size_t chunk = num / num_thread;
	size_t start = chunk * thread_id;
	size_t end = chunk * (thread_id + 1);
	if(end > num) end = num;
	kvpair_t<keytype>* kv = new kvpair_t<keytype>[chunk];
	#ifdef BREAKDOWN
	breakdown_t time;
	memset(&time, 0, sizeof(breakdown_t));
	#endif
	int gc_counter = 0;
	int sensor_id = 0;

	PerfEventBlock e;
        if(insert_only && profile){
            params[thread_id].setParam("threads", thread_id+1);
            e.setParam(1, params[thread_id], (thread_id == 0));
            e.registerCounters();
            e.startCounters();
        }

	int j = 0;
	for(auto i=start; i<end; i++, j++){
	    auto op = load_ops[i].second.first;
	    auto lat = load_ops[i].second.second;
	    if(op == OP_INSERT){
		kv[j].key = (((Rdtsc() - lat) << 16) | sensor_id++ << 6) | thread_id;
		kv[j].value = reinterpret_cast<uint64_t>(&kv[j].key);
	    }
	    else{
		kv[j].key = load_ops[i].first.key;
		kv[j].value = reinterpret_cast<uint64_t>(&kv[j].key);
	    }

	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();
	    
	    if(measure_latency_)
		local_load_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    idx->insert(kv[j].key, kv[j].value, ti);

	    if(measure_latency_)
		local_load_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    gc_counter++;
	    if(gc_counter % 4096 == 0) {
		ti->rcu_quiesce();
	    }

	    keys[thread_id].push_back(kv[j]);
	    if(sensor_id == 1024)
		sensor_id = 0;
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
    };

    if(insert_only && (memory_bandwidth || profile)){
	std::cout << "Ready to profile" << std::endl;
	getchar();
    }
    double start_time = get_now(); 
    if(!insert_only)
	StartThreads(idx, num_thread, load, false);
    else
	StartThreads(idx, num_thread, load_earliest, false);
    double end_time = get_now();
    
    if(insert_only && (memory_bandwidth || profile)){
	std::cout << "End of profile" << std::endl;
	getchar();
    }

    if(insert_only && earliest){
	uint64_t _num = 0;
	for(int i=0; i<num_thread; i++){
	    _num += inserted_num[i];
	}
	std::cout << "Processed " << _num << " / " << num << " (" << (double)_num/num*100 << " \%)" << std::endl;
	num = _num;
    }
    double tput = num / (end_time - start_time) / 1000000; //Mops/sec
    std::cout << "Load " << tput << std::endl;

    if(measure_latency){
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
	std::cout << "Total: 		 \t" << time_total << std::endl;
	std::cout << "[Traversal]" << std::endl;
	std::cout << "\tSucess: 	 \t" << (double)time_traversal/time_total * 100 << " \%" << std::endl;
	std::cout << "\tAbort : 	 \t" << (double)time_abort/time_total * 100 << " \%" << std::endl;
	std::cout << "[Synchronization]" << std::endl;
	std::cout << "\tLatching: 	 \t" << (double)time_latch/time_total * 100 << " \%" << std::endl;
	std::cout << "[Operations]" << std::endl;
	std::cout << "\tLeaf Operations: \t" << (double)time_node/time_total * 100 << " \%" << std::endl;
	std::cout << "\tSMOs:		 \t" << (double)time_split/time_total * 100 << " \%" << std::endl;
	std::cout << "\tConsolidation:	 \t" << (double)time_consolidation/time_total * 100 << " \%" << std::endl;
	std::cout << "-------------------------------------------------" << std::endl;
    }
    #endif
    if(insert_only == true) {
	idx->getMemory();
	idx->find_depth();
	delete idx;
	return;
    }


    std::vector<std::pair<kvpair_t<keytype>, std::pair<int, uint64_t>>> ops;
    std::unordered_map<keytype, bool> hashmap;
    hashmap.reserve(num);
    ops.reserve(num);
    for(int i=0; i<num_thread; i++){
	for(auto& v: keys[i]){
	    hashmap.insert(std::make_pair(v.key, true));
	}
    }
	    
    for(int i=0; i<num_thread; i++){
	for(auto& v: keys[i]){
	    int r = rand() % 100;
	    if(r < 50){
		auto random = (rand() % 100) / 100.0;
		if(random < random_rate){
		    uint32_t sensor_id = 0;
		    auto key = v.key | ((sensor_id++ << 6) | i);
		    while(hashmap.find(key) != hashmap.end()){
			key = v.key | ((sensor_id++ << 6) | i);
		    }
		    hashmap.insert(std::make_pair(key, true));
		    auto kv = new kvpair_t<keytype>;
		    kv->key = key;
		    kv->value = reinterpret_cast<uint64_t>(&kv->key);
		    ops.push_back(std::make_pair(*kv, std::make_pair(OP_RANDOMINSERT, 0))); // RANDOM INSERT
		}
		else{
		    uint64_t latency = 0;
		    if(fuzzy)
			latency = rand() % (fuzzy * frequency);
		    ops.push_back(std::make_pair(v, std::make_pair(OP_INSERT, latency))); // INSERT
		}
	    }
	    else if(r < 80){
		uint64_t range = rand() % 5 + 5;
		ops.push_back(std::make_pair(v, std::make_pair(OP_SCAN, range))); // SHORT SCAN
	    }
	    else if(r < 90){
		uint64_t range = rand() % 90 + 10;
		ops.push_back(std::make_pair(v, std::make_pair(OP_SCAN, range))); // LONG SCAN
	    }
	    else
		ops.push_back(std::make_pair(v, std::make_pair(OP_READ, 0))); // READ
	}
    }

    std::sort(ops.begin(), ops.end(), [](auto& a, auto& b){
	    return a.first.key < b.first.key;
	    });
    std::random_shuffle(ops.begin(), ops.end());

    std::vector<std::chrono::high_resolution_clock::time_point> local_run_latency[num_thread];
    if(measure_latency){
	for(int i=0; i<num_thread; i++){
	    local_run_latency[i].resize(std::ceil(num / num_thread * 2));
	    local_run_latency[i].resize(0);
	}
    }

    earliest_finished = false;
    std::vector<uint64_t> run_num(num_thread);
    auto read_earliest = [idx, num, num_thread, &earliest_finished, &run_num, &local_run_latency, ops, &params, &perf_block, profile, &breakdown](uint64_t thread_id, bool){
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);
	size_t chunk = num / num_thread;
	size_t start = chunk * thread_id;
	size_t end = chunk * (thread_id + 1);
	if(end > num)
	    end = num;

	#ifdef BREAKDOWN
	breakdown_t time;
	memset(&time, 0, sizeof(breakdown_t));
	#endif
	int gc_counter = 0;

	PerfEventBlock e;
        if(profile){
            params[thread_id].setParam("threads", thread_id+1);
            e.setParam(1, params[thread_id], (thread_id == 0));
            e.registerCounters();
            e.startCounters();
        }

	std::vector<uint64_t> v;
	v.reserve(5);
	for(auto i=start; i<end; i++){
	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();
	    
	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    #ifdef BWTREE_USE_MAPPING_TABLE
	    idx->find(ops[i].first.key, &v, ti);
	    #else
	    idx->find_bwtree_fast(ops[i].first.key, &v);
	    #endif
	    v.clear();

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    gc_counter++;
	    if(gc_counter % 4096 == 0) {
		ti->rcu_quiesce();
	    }

	    if(earliest_finished){
		ti->rcu_quiesce();
		if(profile){
                    e.stopCounters();
                    perf_block[thread_id] = e;
                }
		run_num[thread_id] = i - start;
		#ifdef BREAKDOWN
		idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
		memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
		#endif
		return;
	    }
	}

	ti->rcu_quiesce();
	if(profile){
	    e.stopCounters();
	    perf_block[thread_id] = e;
	}
	earliest_finished = true;
	run_num[thread_id] = chunk;
	#ifdef BREAKDOWN
	idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
	memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
	#endif
    };

    auto read = [idx, num, num_thread, &local_run_latency, ops, &params, &perf_block, profile, &breakdown](uint64_t thread_id, bool){
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);
	size_t chunk = num / num_thread;
	size_t start = chunk * thread_id;
	size_t end = chunk * (thread_id + 1);
	if(end > num)
	    end = num;

	#ifdef BREAKDOWN
	breakdown_t time;
	memset(&time, 0, sizeof(breakdown_t));
	#endif
	int gc_counter = 0;
	PerfEventBlock e;
        if(profile){
            params[thread_id].setParam("threads", thread_id+1);
            e.setParam(1, params[thread_id], (thread_id == 0));
            e.registerCounters();
            e.startCounters();
        }

	std::vector<uint64_t> v;
	v.reserve(5);
	for(auto i=start; i<end; i++){
	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();
	    
	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    #ifdef BWTREE_USE_MAPPING_TABLE
	    idx->find(ops[i].first.key, &v, ti);
	    #else
	    idx->find_bwtree_fast(ops[i].first.key, &v);
	    #endif
	    v.clear();

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    gc_counter++;
	    if(gc_counter % 4096 == 0) {
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
    };

    auto scan_range = new int[num];
    for(int i=0; i<num; i++){
	scan_range[i] = rand() % 100;
    }
    auto scan_earliest = [idx, num, num_thread, &earliest_finished, &run_num, &local_run_latency, ops, &scan_range, &params, &perf_block, profile, &breakdown](uint64_t thread_id, bool){
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);
	size_t chunk = num / num_thread;
	size_t start = chunk * thread_id;
	size_t end = chunk * (thread_id + 1);
	if(end > num)
	    end = num;

	#ifdef BREAKDOWN
	breakdown_t time;
	memset(&time, 0, sizeof(breakdown_t));
	#endif
	int gc_counter = 0;
	PerfEventBlock e;
        if(profile){
            params[thread_id].setParam("threads", thread_id+1);
            e.setParam(1, params[thread_id], (thread_id == 0));
            e.registerCounters();
            e.startCounters();
        }

	for(auto i=start; i<end; i++){
	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();
	    
	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    idx->scan(ops[i].first.key, scan_range[i], ti);

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    gc_counter++;
	    if(gc_counter % 4096 == 0) {
		ti->rcu_quiesce();
	    }

	    if(earliest_finished){
		ti->rcu_quiesce();
		if(profile){
		    e.stopCounters();
		    perf_block[thread_id] = e;
		}
		run_num[thread_id] = i - start;
		#ifdef BREAKDOWN
		idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
		memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
		#endif
		return;
	    }
	}

	ti->rcu_quiesce();
	if(profile){
            e.stopCounters();
            perf_block[thread_id] = e;
        }
	earliest_finished = true;
	run_num[thread_id] = chunk;
	#ifdef BREAKDOWN
	idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
	memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
	#endif
    };

    auto scan = [idx, num, num_thread, &local_run_latency, ops, &scan_range, &params, &perf_block, profile, &breakdown](uint64_t thread_id, bool){
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);
	size_t chunk = num / num_thread;
	size_t start = chunk * thread_id;
	size_t end = chunk * (thread_id + 1);
	if(end > num)
	    end = num;

	#ifdef BREAKDOWN
	breakdown_t time;
	memset(&time, 0, sizeof(breakdown_t));
	#endif
	int gc_counter = 0;
	PerfEventBlock e;
        if(profile){
            params[thread_id].setParam("threads", thread_id+1);
            e.setParam(1, params[thread_id], (thread_id == 0));
            e.registerCounters();
            e.startCounters();
        }

	for(auto i=start; i<end; i++){
	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();
	    
	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    idx->scan(ops[i].first.key, scan_range[i], ti);

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    gc_counter++;
	    if(gc_counter % 4096 == 0) {
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
    };

    auto mix_earliest = [idx, num, num_thread, &earliest_finished, &run_num, &local_run_latency, ops, &params, &perf_block, profile, &breakdown](uint64_t thread_id, bool){
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);
	size_t chunk = num / num_thread;
	size_t start = chunk * thread_id;
	size_t end = chunk * (thread_id + 1);
	if(end > num)
	    end = num;

	#ifdef BREAKDOWN
	breakdown_t time;
	memset(&time, 0, sizeof(breakdown_t));
	#endif
	int gc_counter = 0;
	int sensor_id = 0;
	std::vector<uint64_t> v;
	v.reserve(5);
	PerfEventBlock e;
        if(profile){
            params[thread_id].setParam("threads", thread_id+1);
            e.setParam(1, params[thread_id], (thread_id == 0));
            e.registerCounters();
            e.startCounters();
        }
	for(auto i=start; i<end; i++){
	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();
	    
	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    auto op = ops[i].second.first;
	    if(op == OP_INSERT){
		auto kv = new kvpair_t<keytype>;
		kv->key = (((Rdtsc() - ops[i].second.second) << 16) | sensor_id++ << 6) | thread_id;
		kv->value = reinterpret_cast<uint64_t>(&kv->key);
		if(sensor_id == 1024) sensor_id = 0;
		idx->insert(kv->key, kv->value, ti);
	    }
	    else if(op == OP_RANDOMINSERT){
		auto kv = new kvpair_t<keytype>;
		kv->key = ops[i].first.key;
		kv->value = reinterpret_cast<uint64_t>(&kv->key);
		idx->insert(kv->key, kv->value, ti);
	    }
	    else if(op == OP_READ){
		#ifdef BWTREE_USE_MAPPING_TABLE
		idx->find(ops[i].first.key, &v, ti);
		#else
		idx->find_bwtree_fast(ops[i].first.key, &v);
		#endif
		v.clear();
	    }
	    else{ // SCAN
		idx->scan(ops[i].first.key, ops[i].second.second, ti);
	    }

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    gc_counter++;
	    if(gc_counter % 4096 == 0) {
		ti->rcu_quiesce();
	    }

	    if(earliest_finished){
		ti->rcu_quiesce();
		if(profile){
		    e.stopCounters();
		    perf_block[thread_id] = e;
		}
		run_num[thread_id] = i - start;
		#ifdef BREAKDOWN
		idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
		memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
		#endif
		return;
	    }
	}

	ti->rcu_quiesce();
	if(profile){
            e.stopCounters();
            perf_block[thread_id] = e;
        }
	earliest_finished = true;
	run_num[thread_id] = chunk;
	#ifdef BREAKDOWN
	idx->get_breakdown(time.traversal, time.abort, time.latch, time.node, time.split, time.consolidation);
	memcpy(&breakdown[thread_id], &time, sizeof(breakdown_t));
	#endif
    };

    auto mix = [idx, num, num_thread, &local_run_latency, ops, &params, &perf_block, profile, &breakdown](uint64_t thread_id, bool){
	auto random_bool = std::bind(std::bernoulli_distribution(sampling_rate), std::knuth_b());
	threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);
	size_t chunk = num / num_thread;
	size_t start = chunk * thread_id;
	size_t end = chunk * (thread_id + 1);
	if(end > num)
	    end = num;

	#ifdef BREAKDOWN
	breakdown_t time;
	memset(&time, 0, sizeof(breakdown_t));
	#endif
	int gc_counter = 0;
	int sensor_id = 0;
	std::vector<uint64_t> v;
	v.reserve(5);
	PerfEventBlock e;
	if(profile){
            params[thread_id].setParam("threads", thread_id+1);
            e.setParam(1, params[thread_id], (thread_id == 0));
            e.registerCounters();
            e.startCounters();
        }
	for(auto i=start; i<end; i++){
	    bool measure_latency_ = false;
	    if(measure_latency)
		measure_latency_ = random_bool();
	    
	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    auto op = ops[i].second.first;
	    if(op == OP_INSERT){
		auto kv = new kvpair_t<keytype>;
		kv->key = (((Rdtsc() - ops[i].second.second) << 16) | sensor_id++ << 6) | thread_id;
		kv->value = reinterpret_cast<uint64_t>(&kv->key);
		if(sensor_id == 1024) sensor_id = 0;
		idx->insert(kv->key, kv->value, ti);
	    }
	    else if(op == OP_RANDOMINSERT){
		auto kv = new kvpair_t<keytype>;
		kv->key = ops[i].first.key;
		kv->value = reinterpret_cast<uint64_t>(&kv->key);
		idx->insert(kv->key, kv->value, ti);
	    }
	    else if(op == OP_READ){
		idx->find(ops[i].first.key, &v, ti);
		v.clear();
	    }
	    else{ // SCAN
		idx->scan(ops[i].first.key, ops[i].second.second, ti);
	    }

	    if(measure_latency_)
		local_run_latency[thread_id].push_back(std::chrono::high_resolution_clock::now());

	    gc_counter++;
	    if(gc_counter % 4096 == 0) {
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
    };

    if(memory_bandwidth){
	std::cout << "Ready to profile" << std::endl;
	getchar();
    }
    start_time = get_now(); 
    if (wl == WORKLOAD_C)
	if(earliest)
	    StartThreads(idx, num_thread, read_earliest, false);
	else
	    StartThreads(idx, num_thread, read, false);
    else if (wl == WORKLOAD_E)
	if(earliest)
	    StartThreads(idx, num_thread, scan_earliest, false);
	else
	    StartThreads(idx, num_thread, scan, false);
    else if (wl == WORKLOAD_MIXED)
	if(earliest)
	    StartThreads(idx, num_thread, mix_earliest, false);
	else
	    StartThreads(idx, num_thread, mix, false);
    else{
	fprintf(stderr, "Unknown workload type: %d\n", wl);
	exit(1);
    }
    end_time = get_now(); 

    if(memory_bandwidth){
	std::cerr << "End of profile" << std::endl;
	getchar();
    }
    

    if(earliest){
	uint64_t _num = 0;
	for(int i=0; i<num_thread; i++){
	    _num += run_num[i];
	}
	std::cout << "Processed " << _num << " / " << num << " (" << (double)_num/num*100 << " \%)" << std::endl;
	num = _num;
    }

    tput = num / (end_time - start_time) / 1000000; //Mops/sec
    std::cout << "Elapsed time: " << (double)(end_time - start_time) << " sec" << std::endl;

    if (wl == WORKLOAD_C)
	std::cout << "Read-only " << tput << std::endl; 
    else if (wl == WORKLOAD_E)
	std::cout << "Scan-only " << tput << std::endl; 
    else if (wl == WORKLOAD_MIXED)
	std::cout << "Mixed " << tput << std::endl;
    else{
	fprintf(stderr, "Unknown workload type: %d\n", wl);
	exit(1);
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
	std::cout << "Total: 		 \t" << time_total << std::endl;
	std::cout << "[Traversal]" << std::endl;
	std::cout << "\tSucess: 	 \t" << (double)time_traversal/time_total * 100 << " \%" << std::endl;
	std::cout << "\tAbort : 	 \t" << (double)time_abort/time_total * 100 << " \%" << std::endl;
	std::cout << "[Synchronization]" << std::endl;
	std::cout << "\tLatching: 	 \t" << (double)time_latch/time_total * 100 << " \%" << std::endl;
	std::cout << "[Operations]" << std::endl;
	std::cout << "\tLeaf Operations: \t" << (double)time_node/time_total * 100 << " \%" << std::endl;
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
	    ("workload", "Workload type (load,read,scan,mixed)", cxxopts::value<std::string>())
	    ("num", "Size of workload to run", cxxopts::value<uint32_t>()->default_value(std::to_string(opt.num)))
	    ("index", "Index type (artolc, artrowex, hot, masstree, cuckoo, btreeolc, blink, blinkhash, bwtree)", cxxopts::value<std::string>())
	    ("threads", "Number of threads to run", cxxopts::value<uint32_t>()->default_value(std::to_string(opt.threads)))
	    ("mem", "Measure memory bandwidth", cxxopts::value<bool>()->default_value((opt.mem ? "true" : "false")))
	    ("profile", "Enable CPU profiling", cxxopts::value<bool>()->default_value((opt.profile? "true" : "false")))
	    ("latency", "Sample latency of requests", cxxopts::value<float>()->default_value(std::to_string(opt.sampling_latency)))
	    ("hyper", "Enable hyper threading", cxxopts::value<bool>()->default_value((opt.hyper ? "true" : "false")))
	    ("insert_only", "Skip running transactions", cxxopts::value<bool>()->default_value((opt.insert_only ? "true" : "false")))
	    ("earliest", "Measure performance based on the earliest finished thread", cxxopts::value<bool>()->default_value((opt.earliest ? "true" : "false")))
	    ("fuzzy", "Fuzzy insertion latency in (usec)", cxxopts::value<uint64_t>()->default_value(std::to_string(opt.fuzzy)))
	    ("random", "Amount of random insertion", cxxopts::value<float>()->default_value(std::to_string(opt.random)))
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

	if(result.count("fuzzy"))
	    opt.fuzzy = result["fuzzy"].as<uint64_t>();

	if(result.count("random"))
	    opt.random = result["random"].as<float>();

	if(result.count("num"))
	    opt.num = result["num"].as<uint32_t>();
	else{
	    std::cout << "Missing size of workload" << std::endl;
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
    else if(opt.workload.compare("read") == 0)
	wl = WORKLOAD_C;
    else if(opt.workload.compare("scan") == 0)
	wl = WORKLOAD_E;
    else if(opt.workload.compare("mixed") == 0)
	wl = WORKLOAD_MIXED;
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
    else if(opt.index == "cuckoo")
	index_type = TYPE_CUCKOOHASH;
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

    int num;
    if(opt.num != 0)
	num = opt.num;
    else{
	std::cout << "Workload size is not defined!" << std::endl;
	exit(0);
    }

    sampling_rate = opt.sampling_latency;
    if(sampling_rate != 0.0)
	measure_latency = true;

    if(opt.fuzzy < 0){
	std::cout << "Fuzzy insertion latency (usec) should be equal to or larger than 0" << std::endl;
	exit(0);
    }
    fuzzy = opt.fuzzy;

    if(opt.random < 0 || opt.random > 1){
	std::cout << "Random insertion rate should be between 0.0 and 1.0" << std::endl;
	exit(0);
    }
    random_rate = opt.random;


    int num_thread = opt.threads;

    /* Bw-tree option flags */
    #ifdef BWTREE_CONSOLIDATE_AFTER_INSERT
    fprintf(stderr, "  BwTree will considate after insert phase\n");
    #endif

    #ifdef BWTREE_COLLECT_STATISTICS
    fprintf(stderr, "  BwTree will collect statistics\n");
    #endif

    fprintf(stderr, "Leaf delta chain threshold: %d; Inner delta chain threshold: %d\n",
	    LEAF_DELTA_CHAIN_LENGTH_THRESHOLD,
	    INNER_DELTA_CHAIN_LENGTH_THRESHOLD);

    #ifndef BWTREE_USE_MAPPING_TABLE
    fprintf(stderr, "  BwTree does not use mapping table\n");
    if(wl != WORKLOAD_C) {
	fprintf(stderr, "Could only use workload C\n");
	exit(1);
    }

    if(index_type != TYPE_BWTREE) {
	fprintf(stderr, "Could only use BwTree\n");
	exit(1);
    }
    #endif

    #ifndef BWTREE_USE_CAS
    fprintf(stderr, "  BwTree does not use CAS\n");
    #endif

    #ifndef BWTREE_USE_DELTA_UPDATE
    fprintf(stderr, "  BwTree does not use delta update\n");
    if(index_type != TYPE_BWTREE) {
	fprintf(stderr, "Could only use BwTree\n");
    }
    #endif

    #ifdef USE_OLD_EPOCH
    fprintf(stderr, "  BwTree uses old epoch\n");
    #endif

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

    run(index_type, wl, num_thread, num);
    return 0;
}
