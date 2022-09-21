#include "src/papi_util.cpp"
#include "include/options.h"
#include "include/cxxopts.hpp"
#include "include/microbench.h"
#include "include/perf_event.h"

#include "adms/inversions.h"
#include "adms/scramble.h"
#include "adms/genzipf.h"

#include <cstring>
#include <cctype>
#include <atomic>
#include <sstream>
#include <set>
#include <unordered_set>

typedef uint64_t keytype;
typedef uint64_t valuetype;


typedef std::less<uint64_t> keycomp;
static const uint64_t key_type=0;
static const uint64_t value_type=1; // 0 = random pointers, 1 = pointers to keys

static float random_rate = 0;
static bool insert_only = true;
static uint64_t fuzzy = 0;
static bool measure_latency = false;
static float sampling_rate = 0;
extern bool hyperthreading;
static bool profile = false;
static bool earliest = false;
static int bench_type = 0;

static size_t frame_width = 1;
static size_t inversions = 2;
static size_t stride = 2;
static bool data_exists = false;

#include "include/util.h"
inline void run_adms_bwtree(int index_type, int wl, int num_thread, int num){
    Index<keytype, keycomp>* idx = getInstance<keytype, keycomp>(index_type, key_type);
    std::vector<uint64_t> inserted_num(num_thread);
    auto keys_per_thread = num / num_thread;
    size_t over_allocate_zipfian = 256;
    static const unsigned k = 11;
    float zipf_alpha = 0.0;
    std::vector<keytype> data;
    data.reserve(num);

    std::string dataset = "/remote_dataset/latency/adms.txt";

    if(data_exists){
	std::ifstream ifs;
	ifs.open(dataset);
	if(!ifs.is_open()){
	    std::cerr << "Dataset " << dataset << " open failed! " << std::endl;
	    exit(0);
	}
	while(!ifs.eof()){
	    uint64_t key;
	    ifs >> key;
	    data.push_back(key);
	}
	ifs.close();
    }
    else{
	for(int t=0; t<num_thread; t++){
	    std::unordered_set<keytype> values;

	    while(values.size() < keys_per_thread){
		auto new_val = zipf(zipf_alpha, over_allocate_zipfian * keys_per_thread / (1u << k));
		new_val = (new_val * (1u << k)) + (rand() & ((1u << k) - 1u));
		values.insert(new_val);
	    }

	    for(auto it=values.begin(); it!=values.end(); it++)
		data.push_back(*it);
	    //std::copy(values.begin(), values.end(), (data.data() + t*keys_per_thread));

	    if(data.size() == 0)
		std::cout << "data is zero1" << std::endl;
	    //std::sort(data.data() + t* keys_per_thread, data.data() + (t+ 1) * keys_per_thread);
	    std::sort(&data[t* keys_per_thread], &data[(t+ 1) * keys_per_thread]);
	    if(data.size() == 0)
		std::cout << "data is zero2" << std::endl;
	    scramble(data.data() + t* keys_per_thread, keys_per_thread, sizeof(uint64_t),
		    frame_width, inversions, stride);

	    if(data.size() == 0)
		std::cout << "data is zero3" << std::endl;
	}
	std::cout << "data generated" << std::endl;
	/*
	std::ofstream ofs;
	ofs.open(dataset);
	if(!ofs.is_open()){
	    std::cerr << "Dataset " << dataset << " open failed for write! " << std::endl;
	    exit(0);
	}

	for(auto it=data.begin(); it!=data.end(); it++){
	    ofs << *it << std::endl;
	}
	ofs.close();
	*/
    }

    /*
    for(int t=0; t<num_thread; t++){
	std::cout << "Thread " << t << std::endl;
	for(int i=t*keys_per_thread; i<(t+1)*keys_per_thread; i++){
	    std::cout << "    " << data[i] << std::endl;
	}
    }
    */

    bool earliest_finished = false;
    auto load_func = [idx, &data, num, num_thread, &earliest_finished, &inserted_num](uint64_t thread_id, bool){
        threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);
        size_t chunk = num / num_thread;
        size_t start = chunk * thread_id;
        size_t end = chunk * (thread_id + 1);
        if(end > num) end = num;
        kvpair_t<keytype>* kv = new kvpair_t<keytype>[chunk];
        int gc_counter = 0;
        int sensor_id = 0;

        int j = 0;
        for(auto i=start; i<end; i++, j++){
	    kv[j].key = (keytype)data[i];
	    kv[j].value = reinterpret_cast<uint64_t>(&kv[j].key);

            idx->insert(kv[j].key, kv[j].value, ti);
	    gc_counter++;
            if(gc_counter % 4096 == 0) {
                ti->rcu_quiesce();
            }

            if(earliest_finished){
                ti->rcu_quiesce();
                inserted_num[thread_id] = j;
                return;
            }
	}

        ti->rcu_quiesce();
        earliest_finished = true;
        inserted_num[thread_id] = chunk;
    };

    double start_time = get_now();
    StartThreads(idx, num_thread, load_func, false);
    double end_time = get_now();

    uint64_t _num = 0;
    for(int i=0; i<num_thread; i++){
        _num += inserted_num[i];
    }
    std::cout << "Processed " << _num << " / " << num << " (" << (double)_num/num*100 << " \%)" << std::endl;
    num = _num;
    std::cout << (double)(end_time - start_time) << " sec" << std::endl;
    double tput = _num / (end_time - start_time) / 1000000; //Mops/sec
    std::cout << "Load " << tput << std::endl;
}

 

inline void run_adms_art(int index_type, int wl, int num_thread, int num){
    Index<keytype, keycomp>* idx = getInstance<keytype, keycomp>(index_type, key_type);
    std::vector<uint64_t> inserted_num(num_thread);
    bool earliest_finished = false;
    auto keys_per_thread = num / num_thread;
    std::vector<uint64_t> keys;
    keys.reserve(num);
    //auto keys = new uint64_t[num];

    for (int i = 0; i<num; i++)
        keys.push_back(i + 1);
        //keys[i] = i + 1;

    for (int t = 0; t < num_thread; t++) {
        scramble(keys.data() + t * keys_per_thread,
        //scramble(keys + t * keys_per_thread,
                keys_per_thread, sizeof(uint64_t),
                frame_width, inversions, stride);
    }

    auto inversion_factor = num * frame_width;
    /*
    for(int t=0; t<num_thread; t++){
	std::cout << "Thread " << t << std::endl;
	for(int i=t*keys_per_thread; i<(t+1)*keys_per_thread; i++){
	    std::cout << "    " << keys[i] << std::endl;
	}
    }*/

    std::cout << "Inversion factor: " << inversion_factor << std::endl;

    auto load_func = [idx, &keys, num, num_thread, &earliest_finished, &inserted_num](uint64_t thread_id, bool){
        threadinfo *ti = threadinfo::make(threadinfo::TI_MAIN, -1);
        size_t chunk = num / num_thread;
        size_t start = chunk * thread_id;
        size_t end = chunk * (thread_id + 1);
        if(end > num) end = num;
        kvpair_t<keytype>* kv = new kvpair_t<keytype>[chunk];
        int gc_counter = 0;

        int j = 0;
        for(auto i=start; i<end; i++, j++){
	    kv[j].key = keys[i];
	    kv[j].value = reinterpret_cast<uint64_t>(&kv[j].key);

            idx->insert(kv[j].key, kv[j].value, ti);
            gc_counter++;
            if(gc_counter % 4096 == 0) {
                ti->rcu_quiesce();
            }

            if(earliest_finished){
                ti->rcu_quiesce();
                inserted_num[thread_id] = j;
                return;
            }
        }

        ti->rcu_quiesce();
        earliest_finished = true;
        inserted_num[thread_id] = chunk;
    };

    double start_time = get_now();
    StartThreads(idx, num_thread, load_func, false);
    double end_time = get_now();

    uint64_t _num = 0;
    for(int i=0; i<num_thread; i++){
        _num += inserted_num[i];
    }
    std::cout << "Processed " << _num << " / " << num << " (" << (double)_num/num*100 << " \%)" << std::endl;
    num = _num;
    std::cout << (double)(end_time - start_time) << " sec" << std::endl;
    double tput = num / (end_time - start_time) / 1000000; //Mops/sec
    std::cout << "Load " << tput << std::endl;
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

    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine generator(seed);
    std::poisson_distribution<uint64_t> distribution((double)fuzzy);
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
                latency = std::round(distribution(generator) * fuzzy);
            load_ops.push_back(std::make_pair(*kv, std::make_pair(OP_INSERT, latency)));
        }
    }
    std::random_shuffle(load_ops.begin(), load_ops.end());

    size_t chunk = num / num_thread;
    for(int i=0; i<num_thread; i++)
        keys[i].reserve(chunk);

    auto load_func = [idx, num, num_thread, &earliest_finished, &inserted_num, &local_load_latency, &breakdown, &params, &perf_block, profile,         &load_ops](uint64_t thread_id, bool){
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



    double start_time = get_now();
    StartThreads(idx, num_thread, load_func, false);
    double end_time = get_now();

    uint64_t _num = 0;
    for(int i=0; i<num_thread; i++){
	_num += inserted_num[i];
    }
    std::cout << "Processed " << _num << " / " << num << " (" << (double)_num/num*100 << " \%)" << std::endl;
    num = _num;
    std::cout << (double)(end_time - start_time) << " sec" << std::endl;
    double tput = num / (end_time - start_time) / 1000000; //Mops/sec
    std::cout << "Load " << tput << std::endl;
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
            ("type", "Benchmark type (0: rdtsc, 1: adms_art, 2: adms_bwtree", cxxopts::value<uint32_t>()->default_value(std::to_string(opt.bench_type)))
            ("hyper", "Enable hyper threading", cxxopts::value<bool>()->default_value((opt.hyper ? "true" : "false")))
            ("help", "Print help")
            ;

        auto result = options.parse(argc, argv);
        if(result.count("help")){
            std::cout << options.help() << std::endl;
            exit(0);
        }

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

	if(result.count("type"))
	    opt.bench_type = result["type"].as<uint32_t>();

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
    else if(opt.index == "bepsilon")
        index_type = TYPE_BEPSILONTREE;
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


    int num;
    if(opt.num != 0)
        num = opt.num;

    int num_thread = opt.threads;

    int bench_type = opt.bench_type;

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

    if(bench_type == 0){
	std::cout << "Running RDTSC" << std::endl;
	run(index_type, wl, num_thread, num);
    }
    else if(bench_type == 1){
	std::cout << "Running ADMS ART" << std::endl;
	run_adms_art(index_type, wl, num_thread, num);
    }
    else{
	std::cout << "Running ADMS Bwtree" << std::endl;
	run_adms_bwtree(index_type, wl, num_thread, num);
    }
    return 0;
}
