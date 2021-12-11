#include "include/microbench.h"

static constexpr int PAPI_CACHE_EVENT_COUNT = 6;
//static constexpr int PAPI_CACHE_EVENT_COUNT = 7;
static constexpr int PAPI_INST_EVENT_COUNT = 4;
static constexpr int PAPI_EVENT_COUNTS = 4;
static constexpr int PAPI_THREAD_NUM = 128;
static int PAPI_EVENTSET[PAPI_THREAD_NUM];
static uint64_t PAPI_OVERFLOW[PAPI_THREAD_NUM];

// This is only for low level initialization - do not call this
// if only counters are read or started
void InitPAPI() {
    int retval;

    retval = PAPI_library_init(PAPI_VER_CURRENT);
    if(retval != PAPI_VER_CURRENT && retval > 0) {
	fprintf(stderr,"PAPI library version mismatch!\n");
	exit(1); 
    }

    if (retval < 0) {
	fprintf(stderr, "PAPI failed to start (1): %s\n", PAPI_strerror(retval));
	exit(1);
    }

    retval = PAPI_is_initialized();
    if (retval != PAPI_LOW_LEVEL_INITED) {
	fprintf(stderr, "PAPI failed to start (2): %s\n", PAPI_strerror(retval));
	exit(1);
    }

    return;
}

void InitInstMonitor() {
    //InitPAPI();
    return;
}

void StartInstMonitor() {
    int events[PAPI_INST_EVENT_COUNT] = \
					/*{PAPI_LD_INS, PAPI_SR_INS, */{PAPI_BR_INS, PAPI_TOT_CYC, PAPI_TOT_INS, PAPI_BR_MSP};
    long long counters[PAPI_INST_EVENT_COUNT];
    int retval;

    if ((retval = PAPI_start_counters(events, PAPI_INST_EVENT_COUNT)) != PAPI_OK) {
	fprintf(stderr, "PAPI failed to start counters: %s\n", PAPI_strerror(retval));
	exit(1);
    }

    return;
}

void EndInstMonitor() {
    long long counters[PAPI_INST_EVENT_COUNT];
    int retval;

    if ((retval = PAPI_stop_counters(counters, PAPI_INST_EVENT_COUNT)) != PAPI_OK) {
	fprintf(stderr, "PAPI failed to stop counters: %s\n", PAPI_strerror(retval));
	exit(1);
    }

    //std::cout << "Total load = " << counters[0] << "\n";
    //std::cout << "Total store = " << counters[1] << "\n";
    std::cout << "Total branch = " << counters[0] << "\n";
    std::cout << "Total cycle = " << counters[1] << "\n";
    std::cout << "Total instruction = " << counters[2] << "\n";
    std::cout << "Total branch misprediction = " << counters[3] << "\n";

    return;
}

/*
 * StartCacheMonitor() - Uses PAPI Library to start monitoring L1, L2, L3 cache misses  
 */
void StartCacheMonitor() {
    int events[PAPI_CACHE_EVENT_COUNT] = {PAPI_L1_TCH, PAPI_L1_TCM, PAPI_L2_TCH, PAPI_L2_TCM, PAPI_L3_TCH, PAPI_L3_TCM};//, PAPI_LD_INS, PAPI_SR_INS};
    long long counters[PAPI_CACHE_EVENT_COUNT];
    int retval;
    if ((retval = PAPI_start_counters(events, PAPI_CACHE_EVENT_COUNT)) != PAPI_OK) {
	fprintf(stderr, "PAPI failed to start counters: %s\n", PAPI_strerror(retval));
	exit(1);
    }

    return;
}

/*
 * EndCacheMonitor() - Ends and prints PAPI result on cache misses 
 */
void EndCacheMonitor() {
    // We use this array to receive counter values
    long long counters[PAPI_CACHE_EVENT_COUNT];
    int retval;
    if ((retval = PAPI_stop_counters(counters, PAPI_CACHE_EVENT_COUNT)) != PAPI_OK) {
	fprintf(stderr, "PAPI failed to stop counters: %s\n", PAPI_strerror(retval));
	exit(1);
    }

    std::cout << "L1 hits = " << counters[0] << "\n";
    std::cout << "L1 miss = " << counters[1] << "\n";
    std::cout << "L2 hits = " << counters[2] << "\n";
    std::cout << "L2 miss = " << counters[3] << "\n";
    std::cout << "L3 hits = " << counters[4] << "\n";
    std::cout << "L3 miss = " << counters[5] << "\n";
//    std::cout << "Prefetch data instruction miss = " << counters[6] << "\n";

    return;
}

void InitCacheMonitor() {
    InitPAPI();
    return;
}

void OverflowHandler(int eventset, void* addr, long long overflow_vector, void* ctx){
    PAPI_OVERFLOW[eventset]++;
    //fprintf(stderr, "eventset overflow!!\n");
}
/*
 * EndPAPIMonitor() - Ends and prints PAPI result on CPU stats 
 */
void EndPAPIThreadMonitor(uint64_t tid, uint64_t* _counters) {
    // We use this array to receive counter values
    long long counters[PAPI_EVENT_COUNTS];
    int events[PAPI_EVENT_COUNTS] = {PAPI_BR_TKN, PAPI_BR_MSP, PAPI_TOT_INS, PAPI_TOT_CYC};
    int retval;
    if ((retval = PAPI_stop(PAPI_EVENTSET[tid], counters)) != PAPI_OK) {
    //if ((retval = PAPI_stop_counters(counters, PAPI_EVENT_COUNTS)) != PAPI_OK) {
	fprintf(stderr, "PAPI failed to stop counters: %s\n", PAPI_strerror(retval));
	exit(1);
    }

    for(int i=0; i<PAPI_EVENT_COUNTS; i++){
	if((retval = PAPI_overflow(PAPI_EVENTSET[tid], events[i], 0, 0, OverflowHandler)) != PAPI_OK){
	    fprintf(stderr, "PAPI failed to handler overflow at the end of monitor: %s\n", PAPI_strerror(retval));
	    exit(1);
	}
    }

    memcpy(_counters, counters, sizeof(long long)*PAPI_EVENT_COUNTS);
    /*
    std::cout << "Total number of branch prediction:  " << counters[0] << "\n";
    std::cout << "Number of mispredicted branches:    " << counters[1] << "\n";
    std::cout << "Total number of instructions:       " << counters[2] << "\n";
    std::cout << "Total CPU cycles:                   " << counters[3] << "\n";
    */

    if ((retval = PAPI_unregister_thread()) != PAPI_OK){
	fprintf(stderr, "PAPI failed to unregister thread: %s\n", PAPI_strerror(retval));
	exit(1);
    }

    return;
}


void StartPAPIThreadMonitor(uint64_t tid){
    int events[PAPI_EVENT_COUNTS] = {PAPI_BR_TKN, PAPI_BR_MSP, PAPI_TOT_INS, PAPI_TOT_CYC};
    int retval;

    if ((retval = PAPI_register_thread()) != PAPI_OK){
	fprintf(stderr, "PAPI failed to register thread: %s\n", PAPI_strerror(retval));
	exit(0);
    }

    if ((retval = PAPI_create_eventset(&PAPI_EVENTSET[tid])) != PAPI_OK){
	fprintf(stderr, "PAPI failed to create eventset: %s\n", PAPI_strerror(retval));
	exit(1);
    }

    for(int i=0; i<PAPI_EVENT_COUNTS; i++){
	if((retval = PAPI_add_event(PAPI_EVENTSET[tid], events[i])) != PAPI_OK){
	    fprintf(stderr, "PAPI failed to add events: %s\n", PAPI_strerror(retval));
	    exit(1);
	}
	if((retval = PAPI_overflow(PAPI_EVENTSET[tid], events[i], INT_MAX, 0, OverflowHandler)) != PAPI_OK){
	    fprintf(stderr, "PAPI failed to handle overflow: %s\n", PAPI_strerror(retval));
	    exit(1);
	}
    }

    if((retval = PAPI_start(PAPI_EVENTSET[tid])) != PAPI_OK){
	fprintf(stderr, "PAPI failed to start events: %s\n", PAPI_strerror(retval));
	exit(1);
    }
}


void InitPAPIMonitor() {
    int retval;
    memset(PAPI_EVENTSET, PAPI_NULL, sizeof(int)*PAPI_THREAD_NUM);
    memset(PAPI_OVERFLOW, 0, sizeof(uint64_t)*PAPI_THREAD_NUM);

    InitPAPI();

    if((retval = PAPI_thread_init(pthread_self)) != PAPI_OK){
	fprintf(stderr, "PAPI failed to init thread: %s\n", PAPI_strerror(retval));
	exit(1);
    }

    return;
}
