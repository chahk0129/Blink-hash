#include <cstdlib>
#include <iostream>
#include <fstream>
#include <experimental/filesystem>
#include <vector>
#include <string>
#include <cstring>

std::string integer = "int";
std::string rdtsc = "rdtsc";
std::string email = "str";
std::string url = "url";


enum key_type_t{
    INTEGER=0,
    RDTSC,
    EMAIL,
    URL,
    INVALID_KEY
};

std::string* workload
enum workload_type_t{
    WorkloadA=0,
    WorkloadB,
    WorkloadC,
    WorkloadE,
    Load,
    WorkloadMixed
};

std::string* results;
enum result_type_t{
    Throughput=0,
    Latency,
    Bandwidth
};

enum index_type_t{
    artolc=0,
    artrowex,
    hot,
    masstree,
    bwtree,
    blink
};

size_t index_type_num;
std::string* index;
std::string _artolc = "artolc";
std::string _artrowex = "artrowex";
std::string _hot = "hot";
std::string _masstree = "masstree";
std::string _bwtree = "bwtree";
std::string _blink = "blink";

size_t workload_type_num;
std::string* workload;

std::vector<double>* load_data;
std::vector<double>* run_data;
std::vector<double>** load_latency;
std::vector<double>** run_latency;

std::string* _t;
size_t total_thread_type_num;

static constexpr const int total_latency_type_num = 7;
std::string _min = "min";
std::string _50 = "50%";
std::string _90 = "90%";
std::string _95 = "95%";
std::string _99 = "99%";
std::string _99_9 = "99.9%";
std::string _99_99 = "99.99%";

enum latency_t{
    __min=0,
    __50,
    __90,
    __95,
    __99,
    __99_9,
    __99_99,
    INVALID_LATENCY
};

static constexpr size_t num_data = 50000000;

void initialize_indexinfo(size_t index_num){
    index_type_num = index_num;
    index = new std::string[index_num];
    static_assert(index_num == 6);
    index[0] = "artolc";
    index[1] = "artrowex";
    index[2] = "hot";
    index[3] = "masstree";
    index[4] = "bwtree";
    index[5] = "blink";
}

void initialize_workloadinfo(size_t workload_num){
    workload_type_num = workload_num;
    workloads = new std::string[workload_num];
    static_assert(workload_num == 6);
    workload[0] = "Load";
    workload[1] = "Workload A";
    workload[2] = "Workload B";
    workload[3] = "Workload C";
    workload[4] = "Workload E";
    workload[5] = "Workload Mixed";
}


void initialize_threadinfo(size_t core_num){
    size_t cnt = 1;
    size_t core_counts = core_num;
    while(core_counts > 0){
	core_counts /= 2;
	cnt++;
    }
    total_thread_type_num = cnt;
    _t = new std::string[total_thread_type_num];
    core_counts = core_num;
    for(int i=0; i<total_thread_type_num; i++){
	_t[i] = std::to_string(core_counts);
	core_counts /= 2;
	std::cout << i << ": " << _t[i] << std::endl;
    }
}

void write_json(std::string output, key_type_t key_type, workload_type_t workload_type, result_type_t result_type, index_type_t index_type, thread_num_t thread_num, latency_t latency){
    std::ofstream ofs;
    ofs.open(output);
    if(!ofs.is_open()){
	std::cout << "output open failed[line " << __LINE__ << "]: " << output << std::endl;
	exit(0);
    }

    ofs << "{" << std::endl;
    ofs << "  \"config\": {" << std::endl;
    ofs << "    \"name\": \"";
    if(result_type == Throughput){
	ofs << "throughput\"," << std::endl;;
	ofs << "    \"latency\": null," << std::endl;
	ofs << "    \"latency_val\": 0," << std::endl;
	ofs << "    \"unit\": \"Mops/sec\"," << std::endl;
    else if(result_type == Latency){
	ofs << "latency\",";
    else{
	ofs << "bandwidth\",";

    if(result_type == Throughput){
    }
    else if(result_type == Bandwidth){
	ofs << "    \"latency\": null," << std::endl;
	ofs << "    \"latency_val\": 0," << std::endl;
	ofs << "    \"unit\": MB/s," << std::endl;
    }
    else{
	ofs << "    \"latency\": true," << std::endl;
	if(latency == __min)
	    ofs << "    \"latency_val\": min," << std::endl;
	else if(latency == __50)
	    ofs << "    \"latency_val\": 50," << std::endl;
	else if(latency == __90)
	    ofs << "    \"latency_val\": 90," << std::endl;
	else if(latency == __95)
	    ofs << "    \"latency_val\": 95," << std::endl;
	else if(latency == __99)
	    ofs << "    \"latency_val\": 99," << std::endl;
	else if(latency == __99_9)
	    ofs << "    \"latency_val\": 99.9," << std::endl;
	else if(latency == __99_99)
	    ofs << "    \"latency_val\": 99.99," << std::endl;
	ofs << "    \"unit\": nsec," << std::endl;
    }

    ofs << "    \"loc\": " << thread_num << "," << std::endl;
    ofs << "    \"stream_cnt\": \"1000\"" << std::endl;
    ofs << "  }," << std::endl;

    ofs << "  \"env\": {" << std::endl;
    ofs << "    \"num_data\": " << num_data << "," << std::endl;

    std::string wk_type;
    if(workload_type == WorkloadA)
	wk_type = "a";
    else if(workload_type == WorkloadB)
	wk_type = "b";
    else if(workload_type == WorkloadB)
	wk_type = "b";
    else if(workload_type == WorkloadC)
	wk_type = "c";
    else if(workload_type == WorkloadE)
	wk_type = "e";
    else
	wk_type = "load";

    ofs << "    \"workload_type\": \"" << wk_type << "\"," << std::endl;

    std::string k_type;
    int key_size = 0;
    if(key_type == INTEGER){
	k_type = integer;
	key_size = 8;
    }
    else if(key_type == RDTSC){
	k_type = rdtsc;
	key_size = 8;
    }
    else if(key_type == EMAIL){
	k_type = email;
	key_size = 32;
    }
    else{
	k_type = url;
	key_size = 128;
    }
    ofs << "    \"key_type\": \"" << k_type << "\"," << std::endl;
    ofs << "    \"key_size\": " << key_size << "," << std::endl;
    ofs << "    \"cpu_num\": " << cpu_num << std::endl;
    ofs << "  }," << std::endl;

    std::string pcm_;
    if(result_type != Latency)
	pcm_ = "null";
    else
	pcm_ = "true";
    ofs << "  \"pcm\": " << pcm_ << "," << std::endl;
    ofs << "  \"metrics\": null,"  << std::endl;

    std::string index_;
    if(index_type == artolc)
	index_ = _artolc;
    else if(index_type == artrowex)
	index_ = _artrowex;
    else if(index_type == hot)
	index_ = _hot;
    else if(index_type == masstree)
	index_ = _masstree;
    else if(index_type == bwtree)
	index_ = _bwtree;
    else
	index_ = _blink;
    ofs << "  \"index\": \"" << index_ << "\"," << std::endl;

    ofs << "  \"results\": [" << std::endl;
    if(result_type != Latency){
	if(workload_type != Load){
	    for(auto it=run_data[thread_num].begin(); it!=run_data[thread_num].end(); it++){
		ofs << "    {" << std::endl;
		ofs << "      \"value\": " << *it << std::endl;
		if(it != run_data[thread_num].end()-1)
		    ofs << "    }," << std::endl;
		else
		    ofs << "    }" << std::endl;
	    }
	}
	else{
	    for(auto it=load_data[thread_num].begin(); it!=load_data[thread_num].end(); it++){
		ofs << "    {" << std::endl;
		ofs << "      \"value\": " << *it << std::endl;
		if(it != load_data[thread_num].end()-1)
		    ofs << "    }," << std::endl;
		else
		    ofs << "    }" << std::endl;
	    }
	}
    }
    else{
	if(workload_type != Load){
	    for(auto it=run_latency[thread_num][latency].begin(); it!=run_latency[thread_num][latency].end(); it++){
		ofs << "    {" << std::endl;
		ofs << "      \"value\": " << *it << std::endl;
		if(it != run_latency[thread_num][latency].end()-1)
		    ofs << "    }," << std::endl;
		else
		    ofs << "    }" << std::endl;
	    }
	}
	else{
	    for(auto it=load_latency[thread_num][latency].begin(); it!=load_latency[thread_num][latency].end(); it++){
		ofs << "    {" << std::endl;
		ofs << "      \"value\": " << *it << std::endl;
		if(it != load_latency[thread_num][latency].end()-1)
		    ofs << "    }," << std::endl;
		else
		    ofs << "    }" << std::endl;
	    }
	}
    }

    ofs << "  ]" << std::endl;
    ofs << "}" << std::endl;
    ofs.close();
}

void write_output(key_type_t key_type, workload_type_t workload_type, result_type_t result_type, index_type_t index_type, thread_num_t thread_num, latency_t latency){
    std::string load_output;
    std::string output = "output/parsed/";
    if(result_type == Throughput)
	output.append("throughput/");
    else if(result_type == Latency)
	output.append("latency/");
    else if(result_type == Bandwidth)
	output.append("bandiwdth/");
    else{
	std::cout << "Unkown result type [line " << __LINE__ << "]: " << result_type << std::endl;
	exit(0);
    }

    if(key_type == INTEGER)
	output.append("int/");
    else if(key_type == RDTSC)
	output.append("rdtsc/");
    else if(key_type == EMAIL)
	output.append("str/");
    else if(key_type == URL)
	output.append("url/");
    else{
	std::cout << "Unkown key type [line " << __LINE__ << ": " << key_type << std::endl;
	exit(0);
    }

    if(key_type != RDTSC){
	load_output = output;
	load_output.append("load-");
	if(workload_type == WorkloadA)
	    output.append("a-");
	else if(workload_type == WorkloadB)
	    output.append("b-");
	else if(workload_type == WorkloadC)
	    output.append("c-");
	else if(workload_type == WorkloadE)
	    output.append("e-");
	else{
	    std::cout << "Unkown workload type[line " << __LINE__ << ": " << workload_type << std::endl;
	    exit(0);
	}

	if(index_type == artolc){
	    output.append("artolc-");
	    load_output.append("artolc-");
	}
	else if(index_type == artrowex){
	    output.append("artrowex-");
	    load_output.append("artrowex-");
	}
	else if(index_type == hot){
	    output.append("hot-");
	    load_output.append("hot-");
	}
	else if(index_type == masstree){
	    output.append("masstree-");
	    load_output.append("masstree-");
	}
	else if(index_type == bwtree){
	    output.append("bwtree-");
	    load_output.append("bwtree-");
	}
	else if(index_type == blink){
	    output.append("blink-");
	    load_output.append("blink-");
	}
	else{
	    std::cout << "Unknown index_type [line " << __LINE__ << ": " << index_type << std::endl;
	    exit(0);
	}


	if(thread_num == t0){
	    output.append(_t0);
	    load_output.append(_t0);
	}
	else if(thread_num == t1){
	    output.append(_t1);
	    load_output.append(_t1);
	}
	else if(thread_num == t2){
	    output.append(_t2);
	    load_output.append(_t2);
	}
	else if(thread_num == t3){
	    output.append(_t3);
	    load_output.append(_t3);
	}
	else if(thread_num == t4){
	    output.append(_t4);
	    load_output.append(_t4);
	}
	else if(thread_num == t5){
	    output.append(_t5);
	    load_output.append(_t5);
	}
	else if(thread_num == t6){
	    output.append(_t6);
	    load_output.append(_t6);
	}
	else if(thread_num == t7){
	    output.append(_t7);
	    load_output.append(_t7);
	}
	else if(thread_num == t8){
	    output.append(_t8);
	    load_output.append(_t8);
	}
	else{
	    std::cout << "Unkown thread num [line " << __LINE__ << "]: " << static_cast<int>(thread_num) << std::endl;
	    exit(0);
	}

	if(result_type != Latency){
	    output.append(".json");
	    load_output.append(".json");
	    write_json(output, key_type, workload_type, result_type, index_type, thread_num, INVALID_LATENCY);
	    write_json(load_output, key_type, Load, result_type, index_type, thread_num, INVALID_LATENCY);
	}
	else{
	    output.append("-");
	    load_output.append("-");
	    if(latency == __min){
		output.append(_min);
		load_output.append(_min);
	    }
	    else if(latency == __50){
		output.append("50");
		load_output.append("50");
	    }
	    else if(latency == __90){
		output.append("90");
		load_output.append("90");
	    }
	    else if(latency == __95){
		output.append("95");
		load_output.append("95");
	    }
	    else if(latency == __99){
		output.append("99");
		load_output.append("99");
	    }
	    else if(latency == __99_9){
		output.append("99_9");
		load_output.append("99_9");
	    }
	    else{
		output.append("99_99");
		load_output.append("99_99");
	    }
	    output.append(".json");
	    load_output.append(".json");
	    write_json(output, key_type, workload_type, result_type, index_type, thread_num, latency);
	    write_json(load_output, key_type, Load, result_type, index_type, thread_num, latency);
	}
    }
    else{
	output.append("load-");

	if(index_type == artolc)
	    output.append("artolc-");
	else if(index_type == artrowex)
	    output.append("artrowex-");
	else if(index_type == hot)
	    output.append("hot-");
	else if(index_type == masstree)
	    output.append("masstree-");
	else if(index_type == bwtree)
	    output.append("bwtree-");
	else if(index_type == blink)
	    output.append("blink-");
	else{
	    std::cout << "Unknown index_type [line " << __LINE__ << "]: " << index_type << std::endl;
	    exit(0);
	}

	if(thread_num == t0)
	    output.append(_t0);
	else if(thread_num == t1)
	    output.append(_t1);
	else if(thread_num == t2)
	    output.append(_t2);
	else if(thread_num == t3)
	    output.append(_t3);
	else if(thread_num == t4)
	    output.append(_t4);
	else if(thread_num == t5)
	    output.append(_t5);
	else if(thread_num == t6)
	    output.append(_t6);
	else if(thread_num == t7)
	    output.append(_t7);
	else if(thread_num == t8)
	    output.append(_t8);
	else{
	    std::cout << "Unkown thread num [line " << __LINE__ << "]" << std::endl;
	    exit(0);
	}

	if(result_type != Latency){
	    output.append(".json");
	    write_json(output, key_type, Load, result_type, index_type, thread_num, INVALID_LATENCY);
	}
	else{
	    output.append("-");
	    if(latency == __min)
		output.append(_min);
	    else if(latency == __50)
		output.append(_50);
	    else if(latency == __90)
		output.append(_90);
	    else if(latency == __95)
		output.append(_95);
	    else if(latency == __99)
		output.append(_99);
	    else if(latency == __99_9)
		output.append(_99_9);
	    else
		output.append(_99_99);
	    write_json(output, key_type, Load, result_type, index_type, thread_num, latency);
	}
    }
}
	

void parse_throughput(std::string path, key_type_t key_type){
    result_type_t result_type = Throughput;
    for(const auto& file: std::experimental::filesystem::directory_iterator(path)){
	auto status = std::experimental::filesystem::status(file.path());
	if(status.type() == std::experimental::filesystem::file_type::directory){
	    std::string subpath = file.path();
	    if(subpath.find(integer) != std::string::npos)
		parse_throughput(subpath, INTEGER);
	    if(subpath.find(rdtsc) != std::string::npos)
		parse_throughput(subpath, RDTSC);
	    if(subpath.find(email) != std::string::npos)
		parse_throughput(subpath, EMAIL);
	    if(subpath.find(url) != std::string::npos)
		parse_throughput(subpath, URL);
	    continue;
	}

	if(key_type == INVALID_KEY)
	    return;


	std::ifstream ifs;
	ifs.open(file.path());
	if(!ifs.is_open()){
	    std::cout << "file open failed [line " << __LINE__ << "]: " << file.path() << std::endl;
	    exit(0);
	}

	std::cout << __func__ << ": " << file.path() << std::endl;
	index_type_t index_type;
	workload_type_t workload_type;
	thread_num_t thread_num;

	std::string _path = file.path();
	if(_path.find(_artolc) != std::string::npos)
	    index_type = artolc;
	else if(_path.find(_artrowex) != std::string::npos)
	    index_type = artrowex;
	else if(_path.find(_hot) != std::string::npos)
	    index_type = hot;
	else if(_path.find(_masstree) != std::string::npos)
	    index_type = masstree;
	else if(_path.find(_bwtree) != std::string::npos)
	    index_type = bwtree;
	else if(_path.find(_blink) != std::string::npos)
	    index_type = blink;
	else{
	    std::cout << "Read wrong file!! [line " << __LINE__ << "]: " << _path << std::endl;
	    exit(0);
	}

	while(!ifs.eof()){
	    std::string temp;
	    double tput = 0;
	    ifs >> temp;

	    if(temp.find("thread") == std::string::npos)
		continue;

	    std::string p;
	    ifs >> p;
	    if(p.find(_t0) != std::string::npos)
		thread_num = t0;
	    else if(p.find(_t1) != std::string::npos)
		thread_num = t1;
	    else if(p.find(_t2) != std::string::npos)
		thread_num = t2;
	    else if(p.find(_t3) != std::string::npos)
		thread_num = t3;
	    else if(p.find(_t4) != std::string::npos)
		thread_num = t4;
	    else if(p.find(_t5) != std::string::npos)
		thread_num = t5;
	    else if(p.find(_t6) != std::string::npos)
		thread_num = t6;
	    else if(p.find(_t7) != std::string::npos)
		thread_num = t7;
	    else if(p.find(_t8) != std::string::npos)
		thread_num = t8;
	    else{
		continue;
		std::cout << "Unkown thread num [line " << __LINE__ << "]" << std::endl;
		exit(0);
	    }

	    while(temp.find(load) == std::string::npos){
		ifs >> temp;
	    }
	    if(temp.find(load) != std::string::npos){
		ifs >> tput;
		load_data[thread_num].push_back(tput);
	    }
	    else{
		std::cout << "Wrogn parse (read word) [line " << __LINE__ << "]: " << temp << std::endl;
		exit(0);
	    }

	    if(key_type == RDTSC)
		continue;

	    ifs >> temp;
	    ifs >> temp;
	    ifs >> tput;
	    run_data[thread_num].push_back(tput);
	    if(temp.find(w_a) != std::string::npos)
		workload_type = WorkloadA;
	    else if(temp.find(w_b) != std::string::npos)
		workload_type = WorkloadB;
	    else if(temp.find(w_c) != std::string::npos)
		workload_type = WorkloadC;
	    else if(temp.find(w_e) != std::string::npos)
		workload_type = WorkloadE;
	    else{
		std::cout << "Unknown workload type [line " << __LINE__ << "]" << std::endl;
		exit(0);
	    }
	}

	ifs.close();
	for(int it=total_thread_type_num-1; it>=0; it--){
	    write_output(key_type, workload_type, result_type, index_type, static_cast<thread_num_t>(it), INVALID_LATENCY);
	    load_data[it].clear();
	    run_data[it].clear();
	}
	std::cout << __func__ << ": " << file.path() << " ... done!" << std::endl;
    }
}
		
void parse_latency(std::string path, key_type_t key_type){
    result_type_t result_type = Latency;
    for(const auto& file: std::experimental::filesystem::directory_iterator(path)){
	auto status = std::experimental::filesystem::status(file.path());
	if(status.type() == std::experimental::filesystem::file_type::directory){
	    std::string subpath = file.path();
	    if(subpath.find(integer) != std::string::npos)
		parse_latency(subpath, INTEGER);
	    if(subpath.find(rdtsc) != std::string::npos)
		parse_latency(subpath, RDTSC);
	    if(subpath.find(email) != std::string::npos)
		parse_latency(subpath, EMAIL);
	    if(subpath.find(url) != std::string::npos)
		parse_latency(subpath, URL);
	    continue;
	}

	if(key_type == INVALID_KEY)
	    return;

	index_type_t index_type;
	workload_type_t workload_type;
	thread_num_t thread_num;

	std::string _path = file.path();
	if(_path.find(_artolc) != std::string::npos)
	    index_type = artolc;
	else if(_path.find(_artrowex) != std::string::npos)
	    index_type = artrowex;
	else if(_path.find(_hot) != std::string::npos)
	    index_type = hot;
	else if(_path.find(_masstree) != std::string::npos)
	    index_type = masstree;
	else if(_path.find(_bwtree) != std::string::npos)
	    index_type = bwtree;
	else if(_path.find(_blink) != std::string::npos)
	    index_type = blink;
	else{
	    std::cout << "Read wrong file!![line " << __LINE__ << "]: " << _path << std::endl;
	    exit(0);
	}

	std::ifstream ifs;
	ifs.open(file.path());
	if(!ifs.is_open()){
	    std::cout << "file open failed [line " << __LINE__ << "]: " << file.path() << std::endl;
	    exit(0);
	}
	std::cout << __func__ << ": parsing " << file.path() << std::endl;


	std::string temp = "";
	while(!ifs.eof()){
	    if(temp.find("thread") == std::string::npos){
		ifs >> temp;
		continue;
	    }

	    std::string p;
	    ifs >> p;
	    if(p.find(_t0) != std::string::npos)
		thread_num = t0;
	    else if(p.find(_t1) != std::string::npos)
		thread_num = t1;
	    else if(p.find(_t2) != std::string::npos)
		thread_num = t2;
	    else if(p.find(_t3) != std::string::npos)
		thread_num = t3;
	    else if(p.find(_t4) != std::string::npos)
		thread_num = t4;
	    else if(p.find(_t5) != std::string::npos)
		thread_num = t5;
	    else if(p.find(_t6) != std::string::npos)
		thread_num = t6;
	    else if(p.find(_t7) != std::string::npos)
		thread_num = t7;
	    else if(p.find(_t8) != std::string::npos)
		thread_num = t8;
	    else{
		continue;
		std::cout << "Unkown thread num [line " << __LINE__ << "]" << std::endl;
		exit(0);
	    }

	    while(temp.find(_min) == std::string::npos){
		ifs >> temp;
	    }

	    double latency_val;
	    for(int it=__min; it<=__99_99; it++){
		ifs >> latency_val;
		load_latency[thread_num][it].push_back(latency_val);
		std::cout << "pushing " << latency_val << std::endl;
		ifs >> temp;
	    }

	    ifs >> temp;
	    
	    if(temp.find(w_a) != std::string::npos)
		workload_type = WorkloadA;
	    else if(temp.find(w_b) != std::string::npos)
		workload_type = WorkloadB;
	    else if(temp.find(w_c) != std::string::npos)
		workload_type = WorkloadC;
	    else if(temp.find(w_c) != std::string::npos)
		workload_type = WorkloadE;
	    else if(temp.find(w_rdtsc) != std::string::npos)
		workload_type = Load;
	    else{
		std::cout << "Unknown workload[line " << __LINE__ << "]: " << temp << std::endl;
		exit(0);
	    }

	    while(temp.find(_min) == std::string::npos){
		ifs >> temp;
	    }

	    for(int it=__min; it<=__99_99; it++){
		ifs >> latency_val;
		run_latency[thread_num][it].push_back(latency_val);
		ifs >> temp;
	    }
	}
	ifs.close();
	for(int i=total_thread_type_num-1; i>=0; i--){
	    for(int it=__min; it<=__99_99; it++){
		write_output(key_type, workload_type, result_type, index_type, static_cast<thread_num_t>(i), static_cast<latency_t>(it));
		load_latency[i][it].clear();
		run_latency[i][it].clear();
	    }
	}
	std::cout << __func__ << ": parsing " << file.path() << " ... done!!" << std::endl;
    }
}

void parse_bandwidth(std::string path, key_type_t key_type){
    result_type_t result_type = Bandwidth;
    workload_type_t workload_type;
    for(const auto& file: std::experimental::filesystem::directory_iterator(path)){
	auto status = std::experimental::filesystem::status(file.path());
	if(status.type() == std::experimental::filesystem::file_type::directory){
	    std::string subpath = file.path();
	    if(subpath.find(integer) != std::string::npos)
		parse_bandwidth(subpath, INTEGER);
	    if(subpath.find(rdtsc) != std::string::npos)
		parse_bandwidth(subpath, RDTSC);
	    if(subpath.find(email) != std::string::npos)
		parse_bandwidth(subpath, EMAIL);
	    if(subpath.find(url) != std::string::npos)
		parse_bandwidth(subpath, URL);
	    continue;
	}

	if(file.path().empty())
	    return;

	if(key_type == INVALID_KEY)
	    return;

	index_type_t index_type;
	std::string _path = file.path();
	if(_path.find(_artolc) != std::string::npos)
	    index_type = artolc;
	else if(_path.find(_artrowex) != std::string::npos)
	    index_type = artrowex;
	else if(_path.find(_hot) != std::string::npos)
	    index_type = hot;
	else if(_path.find(_masstree) != std::string::npos)
	    index_type = masstree;
	else if(_path.find(_bwtree) != std::string::npos)
	    index_type = bwtree;
	else if(_path.find(_blink) != std::string::npos)
	    index_type = blink;
	else{
	    std::cout << "Read wrong file [line " << __LINE__ << "]: " << _path << std::endl;
	    exit(0);
	}

	std::ifstream ifs;
	ifs.open(file.path());
	if(!ifs.is_open()){
	    std::cout << "file open failed [line " << __LINE__ << "]: " << file.path() << std::endl;
	    exit(0);
	}

	while(!ifs.eof()){
	    std::string temp;
	    double tput = 0;
	    ifs >> temp;

	    if(temp.find("thread") == std::string::npos)
		continue;

	    std::string p;
	    ifs >> p;
	    thread_num_t thread_num;
	    if(p.find(_t0) != std::string::npos)
		thread_num = t0;
	    else if(p.find(_t1) != std::string::npos)
		thread_num = t1;
	    else if(p.find(_t2) != std::string::npos)
		thread_num = t2;
	    else if(p.find(_t3) != std::string::npos)
		thread_num = t3;
	    else if(p.find(_t4) != std::string::npos)
		thread_num = t4;
	    else if(p.find(_t5) != std::string::npos)
		thread_num = t5;
	    else if(p.find(_t6) != std::string::npos)
		thread_num = t6;
	    else if(p.find(_t7) != std::string::npos)
		thread_num = t7;
	    else if(p.find(_t8) != std::string::npos)
		thread_num = t8;
	    else{
		continue;
		std::cout << "Unkown thread num  [line " << __LINE__ << "] " << std::endl;
		exit(0);
	    }

	    ifs >> temp;
	    if(temp.find(load) != std::string::npos){
		ifs >> tput;
		load_data[thread_num].push_back(tput);
	    }
	    else{
		std::cout << "Wrogn parse (read word) [line " << __LINE__ << "]: " << temp << std::endl;
		exit(0);
	    }

	    ifs >> temp;
	    if(temp.find(w_a) != std::string::npos){
		ifs >> tput;
		run_data[thread_num].push_back(tput);
		workload_type = WorkloadA;
	    }
	    else if(temp.find(w_b) != std::string::npos){
		ifs >> tput;
		run_data[thread_num].push_back(tput);
		workload_type = WorkloadB;
	    }
	    else if(temp.find(w_c) != std::string::npos){
		ifs >> tput;
		run_data[thread_num].push_back(tput);
		workload_type = WorkloadC;
	    }
	    else if(temp.find(w_e) != std::string::npos){
		ifs >> tput;
		run_data[thread_num].push_back(tput);
		workload_type = WorkloadE;
	    }
	    else{
		std::cout << "Unkown workload type [line " << __LINE__ << "]" << std::endl;
		exit(0);
	    }
	}

	ifs.close();
    }
}



bool parse_input(std::string path){
    load_data = new std::vector<double>[total_thread_type_num];
    run_data = new std::vector<double>[total_thread_type_num];
    load_latency = new std::vector<double>*[total_thread_type_num];
    run_latency = new std::vector<double>*[total_thread_type_num];
    for(int i=0; i<=total_thread_type_num; i++){
	load_latency[i] = new std::vector<double>[total_latency_type_num];
	run_latency[i] = new std::vector<double>[total_latency_type_num];
    }

    std::string throughput = "throughput";
    std::string latency = "latency";
    std::string footprint = "footprint";
    std::string bandwidth = "bandwidth";

    std::cout << "Parsing files in " << path << std::endl;
    if(path.find(throughput) != std::string::npos)
	parse_throughput(path, INVALID_KEY);
    else if(path.find(latency) != std::string::npos)
	parse_latency(path, INVALID_KEY);
    else if(path.find(bandwidth) != std::string::npos)
	parse_bandwidth(path, INVALID_KEY);
    else if(path.find(footprint) != std::string::npos){
	std::cout << "This parser does not support parsing for footrpint" << std::endl;
	return 0;
    }
    else{
	std::cerr << "invalid path: " << path << std::endl;
	return 0;
    }
    return 1;
}
*/

int main(int argc, char* argv[]){
    /*
    std::string path(argv[1]);
    auto ret = parse_input(path);
    if(ret)
	std::cout << "Finished parsing output" << std::endl;
    else
	std::cout << "Failed parsing output" << std::endl;
	*/
    initialize_threadinfo();
    return 0;
}
