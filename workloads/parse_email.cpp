#include <string>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <vector>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <random>
#include <experimental/filesystem>

uint64_t _100M = 100000000;

void load(std::vector<std::string>& emails, std::string output){
    std::ofstream ofs;
    std::string load_output = output;
    load_output.append("load_email_100M.dat");
    ofs.open(load_output);
    if(!ofs.is_open()){
	std::cerr << __func__ << " output open failed: " << load_output << std::endl;
	exit(0);
    }

    for(uint64_t i=0; i<_100M; i++){
	ofs << "INSERT " << emails[i] << std::endl;
    }

    ofs.close();
}

void workloadA(std::vector<std::string>& emails, std::string output){
    std::ofstream ofs;
    std::string txnsa_output = output;
    txnsa_output.append("txnsa_email_100M.dat");
    ofs.open(txnsa_output);
    if(!ofs.is_open()){
	std::cerr << __func__ << " output open failed: " << txnsa_output << std::endl;
	exit(0);
    }

    for(uint64_t i=0; i<_100M; i++){
	uint64_t random = rand() % 100;
	if(random < 50)
	    ofs << "READ " << emails[i] << std::endl;
	else
	    ofs << "UPDATE " << emails[i] << std::endl;
    }

    ofs.close();
}

void workloadB(std::vector<std::string>& emails, std::string output){
    std::ofstream ofs;
    std::string txnsb_output = output;
    txnsb_output.append("txnsb_email_100M.dat");
    ofs.open(txnsb_output);
    if(!ofs.is_open()){
	std::cerr << __func__ << " output open failed: " << txnsb_output << std::endl;
	exit(0);
    }

    for(uint64_t i=0; i<_100M; i++){
	uint64_t random = rand() % 100;
	if(random < 95)
	    ofs << "READ " << emails[i] << std::endl;
	else
	    ofs << "UPDATE " << emails[i] << std::endl;
    }

    ofs.close();
}

void workloadC(std::vector<std::string>& emails, std::string output){
    std::ofstream ofs;
    std::string txnsc_output = output;
    txnsc_output.append("txnsc_email_100M.dat");
    ofs.open(txnsc_output);
    if(!ofs.is_open()){
	std::cerr << __func__ << " output open failed: " << txnsc_output << std::endl;
	exit(0);
    }

    for(uint64_t i=0; i<_100M; i++){
	ofs << "READ " << emails[i] << std::endl;
    }

    ofs.close();
}

void workloadE(std::vector<std::string>& emails, std::string output){
    std::ofstream ofs;
    std::string txnse_output = output;
    txnse_output.append("txnse_email_100M.dat");
    ofs.open(txnse_output);
    if(!ofs.is_open()){
	std::cerr << __func__ << " output open failed: " << txnse_output << std::endl;
	exit(0);
    }

    uint64_t insert_idx = _100M;
    for(uint64_t i=0; i<_100M; i++){
	uint64_t random = rand() % 100;
	if(random < 100){
	//if(random < 95){
	RETRY:
	    uint64_t scan = rand() % 100;
	    if(scan == 0) goto RETRY;
	    ofs << "SCAN " << emails[i] << " " << scan << std::endl;
	}
	else
	    ofs << "INSERT " << emails[insert_idx++] << std::endl;
    }

    ofs.close();
}

void workloadMixed(std::vector<std::string>& emails, std::string output){
    std::ofstream ofs;
    std::string txnsmixed_output = output;
    txnsmixed_output.append("txns_mixed_email_100M.dat");
    ofs.open(txnsmixed_output);
    if(!ofs.is_open()){
	std::cerr << __func__ << " output open failed: " << txnsmixed_output << std::endl;
	exit(0);
    }

    uint64_t insert_idx = _100M;
    uint64_t read_ratio = 50;
    uint64_t scan_ratio = 20;
    uint64_t insert_ratio = 10;
    uint64_t update_ratio = 100 - read_ratio - scan_ratio - insert_ratio;
    for(uint64_t i=0; i<_100M; i++){
	uint64_t random = rand() % 100;
	if(random < read_ratio)
	    ofs << "READ " << emails[i] << std::endl;
	else if(random < read_ratio + scan_ratio){
	RETRY:
	    uint64_t scan = rand() % 100;
	    if(scan == 0) goto RETRY;
	    ofs << "SCAN " << emails[i] << " " << scan << std::endl;
	}
	else if(random < read_ratio + scan_ratio + update_ratio)
	    ofs << "UPDATE " << emails[i] << std::endl;
	else
	    ofs << "INSERT " << emails[insert_idx++] << std::endl;
    }
    ofs.close();
}


void process_input(std::string input, std::string output){
    std::ifstream ifs;
    ifs.open(input);
    if(!ifs.is_open()){
	std::cerr << "input open failed: " << input << std::endl;
	exit(0);
    }

    std::vector<std::string> emails;
    emails.reserve(60000000);
    uint64_t cnt = 0;
    uint64_t min = 32;
    uint64_t max = 0;
    uint64_t lens = 0;
    while(!ifs.eof()){
	std::string temp;
	ifs >> temp;

	auto len = temp.length();
	//if(len > 31 || len < 16)
	if(len > 32 || len < 16)
	    continue;

	lens += len;
	if(max < len) max = len;
	if(min > len) min = len;

	emails.push_back(temp);
	cnt++;
    }

    std::cout << "collected " << cnt << " emails" << std::endl;
    std::cout << "max " << max << std::endl;
    std::cout << "min " << min << std::endl;
    std::cout << "avg " << (double)lens / cnt << std::endl;

    std::random_shuffle(emails.begin(), emails.end());
    load(emails, output);
    workloadMixed(emails, output);
    workloadA(emails, output);
    workloadB(emails, output);
    workloadC(emails, output);
    workloadE(emails, output);
}


int main(int argc, char* argv[]){
    if(argc != 3){
	std::cout << "Usage: " << argv[0] << " (path for raw_data) (path for output)" << std::endl;
	exit(0);
    }
    std::string path(argv[1]);
    std::string output(argv[2]);
    auto back = output.back();
    if(back != '/')
	output.append("/");
    std::cout << "Input path: " << path << std::endl;
    std::cout << "Output path: " << output << std::endl;
    process_input(path, output);
    return 0;
}

