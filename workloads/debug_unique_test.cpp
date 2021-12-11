#include <iostream>
#include <fstream>
#include <cstdlib>
#include <unistd.h>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <string>
#include <climits>
int main(){
    std::string input = "/mydata/hcha/workload/load_url_50M.dat";
    //std::string input = "/mydata/hcha/workload/load_email_50M.dat";
    std::ifstream ifs;
    ifs.open(input);
    if(!ifs.is_open()){
	std::cout << "input open failed" << std::endl;
	exit(0);
    }

    uint64_t cnt = 0;
    uint64_t size = 0;
    uint64_t max = 0;
    uint64_t min = 1000000000;

    uint64_t invalid = 0;
    uint64_t over_size = 0;
    std::unordered_map<std::string, bool> hash;
    hash.reserve(50000000);
    while(!ifs.eof()){
	std::string op;
	std::string key;

	ifs >> op >> key;
	auto _size = key.length();
	if(_size == 0){
	    std::cout << "op: " << op << std::endl;
	    std::cout << "key: " << key << std::endl;
	    continue;
	}
	if(strlen(key.c_str()) == 0){
	    invalid++;
	    continue;
	}
	if(key.length() >= 128){
	    over_size++;
	    continue;
	}
	cnt++;
	size += _size;
	if(max < _size) max = _size;
	if(min > _size) min = _size;

	auto ret = hash.find(key);
	if(ret != hash.end()){
	    std::cout << "duplicate key exists!!" << std::endl;
	    exit(0);
	}
	hash.insert(std::make_pair(key, true));
    }
    ifs.close();
    std::cout << "Invalid " << invalid << std::endl;
    std::cout << "Total cnt: " << cnt << std::endl;
    std::cout << "Oversize: " << over_size << std::endl;
    std::cout << "Average size: " << size / cnt << std::endl;
    std::cout << "Max size: " << max << std::endl;
    std::cout << "Min size: " << min << std::endl;
    return 0;
}
	


