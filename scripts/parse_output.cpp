#include <cstdlib>
#include <iostream>
#include <fstream>
#include <experimental/filesystem>
#include <vector>
#include <string>
#include <cstring>
/*
std::string integer = "int";
std::string rdtsc = "rdtsc";
std::string email = "str";
std::string url = "url";
*/
int key_type_num = 5;
std::string* key_type;
enum key_type_t{
    INTEGER=0,
    RDTSC,
    EMAIL,
    URL,
    INVALID_KEY
};

int workload_type_num = 6;
std::string* workload_type;
/*
enum workload_type_t{
    WorkloadA=0,
    WorkloadB,
    WorkloadC,
    WorkloadE,
    Load,
    WorkloadMixed
};
*/

int result_type_num = 3;
std::string* result_type;
/*
enum result_type_t{
    Throughput=0,
    Latency,
    Bandwidth
};
*/

int index_type_num = 6;
std::string* index_type;
/*
enum index_type_t{
    artolc=0,
    artrowex,
    hot,
    masstree,
    bwtree,
    blink
};

std::string _artolc = "artolc";
std::string _artrowex = "artrowex";
std::string _hot = "hot";
std::string _masstree = "masstree";
std::string _bwtree = "bwtree";
std::string _blink = "blink";

std::string load = "Load";
*/
/*
std::string w_a = "Workload A";
std::string w_b = "Workload B";
std::string w_c = "Workload C";
std::string w_e = "Workload E";
*/
/*
std::string w_a = "A";
std::string w_b = "B";
std::string w_c = "C";
std::string w_e = "E";
std::string w_rdtsc = "Load";
*/

std::vector<double>* load_data;
std::vector<double>* run_data;
std::vector<double>** load_latency;
std::vector<double>** run_latency;

std::string* thread_type;
int thread_type_num = 9;
/*
std::string* _t;
*/
static constexpr size_t max_core = 128;
size_t total_thread_type_num = 9;

static constexpr const int total_latency_type_num = 7;
int latency_type_num = 7;
std::string* latency_type;
/*
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
*/

static constexpr size_t num_data = 50000000;

void initialize_info(){
    key_type = new std::string[key_type_num];
    key_type[0] = "int";
    key_type[1] = "rdtsc";
    key_type[2] = "str";
    key_type[3] = "url";

    workload_type = new std::string[workload_type_num];
    workload_type[0] = "Load";
    workload_type[1] = "MIXED";
    workload_type[2] = "A";
    workload_type[3] = "B";
    workload_type[4] = "C";
    workload_type[5] = "E";

    result_type = new std::string[result_type_num];
    result_type[0] = "throughput";
    result_type[1] = "latency";
    result_type[2] = "bandwidth";

    index_type = new std::string[index_type_num];
    index_type[0] = "artolc";
    index_type[1] = "artrowex";
    index_type[2] = "hot";
    index_type[3] = "masstree";
    index_type[4] = "bwtree";
    index_type[5] = "blink";

    thread_type = new std::string[thread_type_num];
    thread_type[0] = "1";
    thread_type[1] = "2";
    thread_type[2] = "4";
    thread_type[3] = "8";
    thread_type[4] = "16";
    thread_type[5] = "32";
    thread_type[6] = "64";
    thread_type[7] = "96";
    thread_type[8] = "128";

    latency_type = new std::string[latency_type_num];
    latency_type[0] = "min";
    latency_type[1] = "50";
    latency_type[2] = "90";
    latency_type[3] = "95";
    latency_type[4] = "99";
    latency_type[5] = "99.9";
    latency_type[6] = "99.99";
}

void write_json(std::string output, std::string index, std::string workload, std::string key, std::string result, std::string thread, std::string latency){
    std::ofstream ofs;
    ofs.open(output);
    if(!ofs.is_open()){
	std::cout << "output open failed[line " << __LINE__ << "]: " << output << std::endl;
	exit(0);
    }

    ofs << "{" << std::endl;
    ofs << "  \"config\": {" << std::endl;
    ofs << "    \"name\": \"";
    ofs << result << "-";
    ofs << thread << "\"," << std::endl;
    ofs << "    \"threads\": " << thread << "," << std::endl;

    if(result == "throughput"){
	ofs << "    \"latency\": null," << std::endl;
	ofs << "    \"latency_val\": 0," << std::endl;
	ofs << "    \"unit\": \"Mops/sec\"," << std::endl;
    }
    else if(result == "bandwidth"){
	ofs << "    \"latency\": null," << std::endl;
	ofs << "    \"latency_val\": 0," << std::endl;
	ofs << "    \"unit\": \"MB/s\"," << std::endl;
    }
    else{
	ofs << "    \"latency\": true," << std::endl;
	ofs << "    \"latency_val\": " << latency << "," << std::endl;
	ofs << "    \"unit\": \"nsec\"," << std::endl;
    }

    int thread_loc = 0;
    for(int i=0; i<thread_type_num; i++){
	if(thread == thread_type[i]){
	    thread_loc = i;
	    break;
	}
    }
    ofs << "    \"loc\": " << thread_loc << "," << std::endl;
    ofs << "    \"stream_cnt\": \"1000\"" << std::endl;
    ofs << "  }," << std::endl;

    ofs << "  \"env\": {" << std::endl;
    ofs << "    \"num_data\": " << num_data << "," << std::endl;
    ofs << "    \"workload_type\": \"" << workload << "\"," << std::endl;

    int key_size = 0;
    for(int i=0; i<key_type_num; i++){
	if(key == key_type[i]){
	    if(key == "int")
		key_size = 4;
	    else if(key == "email")
		key_size = 32;
	    else
		key_size = 128;
	    break;
	}
    }
    ofs << "    \"key_type\": \"" << key << "\"," << std::endl;
    ofs << "    \"key_size\": " << key_size << "," << std::endl;
    ofs << "    \"cpu_num\": " << max_core << std::endl;
    ofs << "  }," << std::endl;

    ofs << "  \"pcm\": " << "null" << "," << std::endl;
    ofs << "  \"metrics\": null,"  << std::endl;
    ofs << "  \"index\": \"" << index << "\"," << std::endl;

    ofs << "  \"results\": [" << std::endl;
    if(result != "latency"){
	if(workload != "load"){
	    for(auto it=run_data[thread_loc].begin(); it!=run_data[thread_loc].end(); it++){
		ofs << "    {" << std::endl;
		ofs << "      \"value\": " << *it << std::endl;
		if(it != run_data[thread_loc].end()-1)
		    ofs << "    }," << std::endl;
		else
		    ofs << "    }" << std::endl;
	    }
	}
	else{
	    for(auto it=load_data[thread_loc].begin(); it!=load_data[thread_loc].end(); it++){
		ofs << "    {" << std::endl;
		ofs << "      \"value\": " << *it << std::endl;
		if(it != load_data[thread_loc].end()-1)
		    ofs << "    }," << std::endl;
		else
		    ofs << "    }" << std::endl;
	    }
	}
    }
    else{
	int latency_loc = 0;
	for(int i=0; i<latency_type_num; i++){
	    if(latency == latency_type[i]){
		latency_loc = i;
		break;
	    }
	}
	if(workload != "load"){
	    for(auto it=run_latency[thread_loc][latency_loc].begin(); it!=run_latency[thread_loc][latency_loc].end(); it++){
		ofs << "    {" << std::endl;
		ofs << "      \"value\": " << *it << std::endl;
		if(it != run_latency[thread_loc][latency_loc].end()-1)
		    ofs << "    }," << std::endl;
		else
		    ofs << "    }" << std::endl;
	    }
	}
	else{
	    for(auto it=load_latency[thread_loc][latency_loc].begin(); it!=load_latency[thread_loc][latency_loc].end(); it++){
		ofs << "    {" << std::endl;
		ofs << "      \"value\": " << *it << std::endl;
		if(it != load_latency[thread_loc][latency_loc].end()-1)
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

void write_output(std::string key, std::string workload, std::string result, std::string index, std::string thread, std::string latency){
    std::string load_output;
    std::string output = "output/parsed/";
    std::string Load = "load";
    output.append(result).append("/");
    output.append(key).append("/");

    if(key != "rdtsc"){
	load_output = output;
	load_output.append("load-").append(index).append("-");;
	output.append(workload).append("-").append(index).append("-");

	output.append(thread);
	load_output.append(thread);

	if(result != "latency"){
	    output.append(".json");
	    load_output.append(".json");
	    write_json(output, index, workload, key, result, thread, latency); 
	    if(workload == "A")
		write_json(load_output, index, Load, key, result, thread, latency); 
	}
	else{
	    output.append("-").append(latency).append(".json");
	    load_output.append("-").append(latency).append(".json");
	    write_json(output, index, workload, key, result, thread, latency);
	    if(workload == "A")
		write_json(load_output, index, Load, key, result, thread, latency); 
	}
    }
    else{
	output.append("load-").append(index).append("-").append(thread).append(".json");
	if(result != "latency"){
	    write_json(output, index, Load, key, result, thread, latency);
	}
	else{
	    output.append("-").append(latency).append(".json");
	    write_json(output, index, Load, key, result, thread, latency);
	}
    }
}
	

void parse_throughput(std::string path, std::string _key){
    std::string result = "throughput";
    for(const auto& file: std::experimental::filesystem::directory_iterator(path)){
	auto status = std::experimental::filesystem::status(file.path());
	if(status.type() == std::experimental::filesystem::file_type::directory){
	    std::string subpath = file.path();
	    for(int i=0; i<key_type_num; i++){
		if(subpath.find(key_type[i]) != std::string::npos){
		    parse_throughput(subpath, key_type[i]);
		}
	    }
	    continue;
	}


	std::string key = "";
	for(int i=0; i<key_type_num; i++){
	    if(_key == key_type[i]){
		key = key_type[i];
		break;
	    }
	}
	if(key == ""){
	    return;
	}


	std::ifstream ifs;
	ifs.open(file.path());
	if(!ifs.is_open()){
	    std::cout << "file open failed [line " << __LINE__ << "]: " << file.path() << std::endl;
	    exit(0);
	}

	std::cout << __func__ << ": " << file.path() << std::endl;

	std::string _path = file.path();
	std::string index = "";
	std::string workload = "";
	std::string thread = "";
	for(int i=0; i<index_type_num; i++){
	    if(_path.find(index_type[i]) != std::string::npos){
		index = index_type[i];
		break;
	    }
	}
	if(index == ""){
	    std::cout << "Error in reading index type (" << __func__ << ")" << std::endl;
	    exit(0);
	}

	while(!ifs.eof()){
	    std::string temp;
	    double tput = 0;
	    ifs >> temp;

	    if(temp.find("thread") == std::string::npos)
		continue;

	    std::string p;
	    int thread_loc = -1;
	    ifs >> p;
	    for(int i=0; i<thread_type_num; i++){
		if(p == thread_type[i]){
		    thread = thread_type[i];
		    thread_loc = i;
		    break;
		}
	    }
	    if(thread_loc == -1){
		std::cout << "Error in reading thread num (" << __func__ << ")" << std::endl;
		exit(0);
	    }

	    std::string load = "Load";
	    while(temp.find(load) == std::string::npos){
		ifs >> temp;
	    }

	    if(temp.find(load) != std::string::npos){
		ifs >> tput;
		load_data[thread_loc].push_back(tput);
	    }
	    else{
		std::cout << "Wrogn parse (read word) [line " << __LINE__ << "]: " << temp << std::endl;
		exit(0);
	    }

	    if(key == "rdtsc")
		continue;

	    ifs >> temp;
	    ifs >> temp;
	    ifs >> tput;
	    run_data[thread_loc].push_back(tput);
	    for(int i=0; i<workload_type_num; i++){
		if(temp.find(workload_type[i]) != std::string::npos){
		    workload = workload_type[i];
		    break;
		}
	    }
	    if(workload == ""){
		std::cout << "Error in reading workload type (" << __func__ << ")" << std::endl;
		exit(0);
	    }
	}

	ifs.close();
	std::string latency = "";
	for(int i=0; i<thread_type_num; i++){
	    write_output(key, workload, result, index, thread_type[i], latency);
	    load_data[i].clear();
	    run_data[i].clear();
	}
	std::cout << __func__ << ": " << file.path() << " ... done!" << std::endl;
    }
}
		
void parse_latency(std::string path, std::string _key){ 
    std::string result = "latency";
    for(const auto& file: std::experimental::filesystem::directory_iterator(path)){
	auto status = std::experimental::filesystem::status(file.path());
	if(status.type() == std::experimental::filesystem::file_type::directory){
	    std::string subpath = file.path();
	    for(int i=0; i<key_type_num; i++){
		if(subpath.find(key_type[i]) != std::string::npos){
		    parse_latency(subpath, key_type[i]);
		}
	    }
	    continue;
	}


	std::string key = "";
	for(int i=0; i<key_type_num; i++){
	    if(_key == key_type[i]){
		key = key_type[i];
		break;
	    }
	}
	if(key == ""){
	    return;
	}


	std::ifstream ifs;
	ifs.open(file.path());
	if(!ifs.is_open()){
	    std::cout << "file open failed [line " << __LINE__ << "]: " << file.path() << std::endl;
	    exit(0);
	}

	std::cout << __func__ << ": " << file.path() << std::endl;

	std::string _path = file.path();
	std::string index = "";
	std::string workload = "";
	std::string thread = "";
	for(int i=0; i<index_type_num; i++){
	    if(_path.find(index_type[i]) != std::string::npos){
		index = index_type[i];
		break;
	    }
	}
	if(index == ""){
	    std::cout << "Error in reading index type (" << __func__ << ")" << std::endl;
	    exit(0);
	}

	while(!ifs.eof()){
	    std::string temp;
	    double tput = 0;
	    ifs >> temp;

	    if(temp.find("thread") == std::string::npos)
		continue;

	    std::string p;
	    int thread_loc = -1;
	    ifs >> p;
	    for(int i=0; i<thread_type_num; i++){
		if(p == thread_type[i]){
		    thread = thread_type[i];
		    thread_loc = i;
		    break;
		}
	    }
	    if(thread_loc == -1){
		std::cout << "Error in reading thread num (" << __func__ << ")" << std::endl;
		exit(0);
	    }

	    std::string load = "Load";
	    while(temp.find(load) == std::string::npos){
		ifs >> temp;
	    }

	    while(temp.find("min") == std::string::npos){
		ifs >> temp;
	    }
	    
	    double latency_val;
	    for(int i=0; i<latency_type_num; i++){
		ifs >> latency_val;
		load_latency[thread_loc][i].push_back(latency_val);
		ifs >> temp;
	    }
	    ifs >> temp;

	    if(key == "rdtsc")
		continue;

	    for(int i=0; i<workload_type_num; i++){
		if(temp.find(workload_type[i]) != std::string::npos){
		    workload = workload_type[i];
		    break;
		}
	    }
	    if(workload == ""){
		std::cout << "Error in reading workload type (" << __func__ << ")" << std::endl;
		exit(0);
	    }

	    while(temp.find("min") == std::string::npos){
		ifs >> temp;
	    }

	    for(int i=0; i<latency_type_num; i++){
		ifs >> latency_val;
		run_latency[thread_loc][i].push_back(latency_val);
		ifs >> temp;
	    }
	}
	ifs.close();
	for(int i=0; i<thread_type_num; i++){
	    for(int j=0; j<latency_type_num; j++){
		write_output(key, workload, result, index, thread_type[i], latency_type[j]);
		load_latency[i][j].clear();
		run_latency[i][j].clear();
	    }
	}
	std::cout << __func__ << ": " << file.path() << " ... done!" << std::endl;
    }
}


bool parse_input(std::string path){
    load_data = new std::vector<double>[thread_type_num];
    run_data = new std::vector<double>[thread_type_num];
    load_latency = new std::vector<double>*[thread_type_num];
    run_latency = new std::vector<double>*[thread_type_num];
    for(int i=0; i<=thread_type_num; i++){
	load_latency[i] = new std::vector<double>[latency_type_num];
	run_latency[i] = new std::vector<double>[latency_type_num];
    }

    std::string throughput = "throughput";
    std::string latency = "latency";
    std::string footprint = "footprint";
    std::string bandwidth = "bandwidth";

    std::cout << "Parsing files in " << path << std::endl;
    if(path.find(throughput) != std::string::npos)
	parse_throughput(path, "");
    else if(path.find(latency) != std::string::npos)
	parse_latency(path, "");
    else if(path.find(bandwidth) != std::string::npos){
	std::cout << "This parser does not support parsing bandwidth" << std::endl;
	exit(0);
    }
    else if(path.find(footprint) != std::string::npos){
	std::cout << "This parser does not support parsing footrpint" << std::endl;
	exit(0);
    }
    else{
	std::cerr << "invalid path: " << path << std::endl;
	return 0;
    }
    return 1;
}

int main(int argc, char* argv[]){
    std::string path(argv[1]);
    initialize_info();
    auto ret = parse_input(path);
    if(ret)
	std::cout << "Finished parsing output" << std::endl;
    else
	std::cout << "Failed parsing output" << std::endl;
    return 0;
}
