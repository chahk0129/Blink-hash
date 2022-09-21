#ifndef OPTIONS_H__
#define OPTIONS_H__
#include <string>
#include <iostream>
struct options_t{
    std::string input = "";
    std::string workload = "load";
    std::string key_type = "";
    std::string index = "";

    uint32_t threads = 1;
    uint32_t num = 100;
    float skew = 0.0;
    bool hyper = true;
    bool insert_only = false;
    float sampling_latency = 0.0;
    bool earliest = false;
    bool mem = false;
    bool profile = false;
    uint64_t fuzzy = 0;
    float random = 0.0;

    uint32_t init_num = 10000000;
    uint32_t run_num = 10000000;

    uint32_t bench_type = 0;

};

std::ostream& operator<<(std::ostream& os, const options_t& opt){
    os << "Benchmark Options:\n"
       << "\tInput path: " << opt.input << "\n"
       << "\tWorkload type: " << opt.workload << "\n"
       << "\tWorkload size: " << opt.num << " M records\n"
       << "\tKey type: " << opt.key_type << "\n"
       << "\tIndex type: " << opt.index << "\n"
       << "\t# Threads: " << opt.threads << "\n"
       << "\tSkew factor: " << opt.skew << "\n"
       << "\tMeasure perf with earliest finished thread: " << opt.earliest << "\n"
       << "\tMeasure memory bandwidth: " << opt.mem << "\n"
       << "\tEnable CPU profiling: " << opt.profile << "\n"
       << "\tSampling latency: " << opt.sampling_latency;
    return os;
}


#endif
