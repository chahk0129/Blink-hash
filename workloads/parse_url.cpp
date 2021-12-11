#include <cstdlib>
#include <cstring>
#include <string>
#include <unistd.h>
#include <vector>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <random>

int main(int argc, char* argv[]){
    if(argc != 3){
	std::cout << "Usage: " << argv[0] << " (input path) (output path)" << std::endl;
	exit(0);
    }
    std::string input(argv[1]);
    std::string output(argv[2]);
    output.append("/");
    std::ifstream ifs;
    ifs.open(input);
    if(!ifs.is_open()){
	std::cerr << "dataset(" << input << ") open failed" << std::endl;
	exit(0);
    }

    std::vector<std::string> urls;
    uint64_t cnt = 0;
    uint64_t total_size = 0;
    uint64_t max_size = 0;
    uint64_t min_size = UINT64_MAX;
    while(!ifs.eof()){
	std::string temp;
	ifs >> temp;
	auto len = strlen(temp.c_str());
	if((len > 127) || (len < 16)) continue; // exclude string containing null and we need to add \0 to the end of key in the final workload
	cnt++;
	total_size += len;
	if(max_size < len)
	    max_size = len;
	if(min_size > len)
	    min_size = len;
	urls.push_back(temp);
    }

    std::random_shuffle(urls.begin(), urls.end());
    size_t num = 70000000;

    { // Load
	std::string path = output;
	path.append("load_url_70M.dat");
	std::ofstream ofs;
	ofs.open(path);
	if(!ofs.is_open()){
	    std::cerr << path << " open failed for write" << std::endl;
	    exit(0);
	}
	for(int i=0; i<num; i++){
	    ofs << "INSERT ";
	    ofs << urls[i];
	    ofs << "\n";
	}
	ofs.close();
    }

    { // Workload A
	std::string path = output;
	path.append("txnsa_url_70M.dat");
	std::ofstream ofs;
	ofs.open(path);
	if(!ofs.is_open()){
	    std::cerr << path << " open failed for write" << std::endl;
	    exit(0);
	}
	int update_ratio = 50;
	for(int i=0; i<num; i++){
	    int random = rand() % 100;
	    if(random < update_ratio)
		ofs << "UPDATE ";
	    else
		ofs << "READ ";
	    ofs << urls[i];
	    ofs << "\n";
	}
	ofs.close();
    }

    { // Workload B
	std::string path = output;
	path.append("txnsb_url_70M.dat");
	std::ofstream ofs;
	ofs.open(path);
	if(!ofs.is_open()){
	    std::cerr << path << " open failed for write" << std::endl;
	    exit(0);
	}
	int update_ratio = 5;
	for(int i=0; i<num; i++){
	    int random = rand() % 100;
	    if(random < update_ratio)
		ofs << "UPDATE ";
	    else
		ofs << "READ ";
	    ofs << urls[i];
	    ofs << "\n";
	}
	ofs.close();
    }

    { // Workload C
	std::string path = output;
	path.append("txnsc_url_70M.dat");
	std::ofstream ofs;
	ofs.open(path);
	if(!ofs.is_open()){
	    std::cerr << path << " open failed for write" << std::endl;
	    exit(0);
	}
	for(int i=0; i<num; i++){
	    ofs << "READ ";
	    ofs << urls[i];
	    ofs << "\n";
	}
	ofs.close();
    }

    { // Workload E
	std::string path = output;
	path.append("txnse_url_70M.dat");
	std::ofstream ofs;
	ofs.open(path);
	if(!ofs.is_open()){
	    std::cerr << path << " open failed for write" << std::endl;
	    exit(0);
	}
	int insert_ratio = 5;
	uint64_t insert_idx = num;
	for(int i=0; i<num; i++){
	    int random = rand() % 100;
	    if(random < insert_ratio){
		ofs << "INSERT ";
		ofs << urls[insert_idx++];
		ofs << "\n";
	    }
	    else{
		int range = rand() % 100;
		ofs << "SCAN ";
		ofs << urls[i];
		ofs << " " << range;
		ofs << "\n";
	    }
	}
	ofs.close();
    }

    { // Workload Mixed
	std::string path = output;
	path.append("txns_mixed_url_70M.dat");
	std::ofstream ofs;
	ofs.open(path);
	if(!ofs.is_open()){
	    std::cerr << path << " open failed for write" << std::endl;
	    exit(0);
	}
	uint64_t insert_idx = num;
	int read_ratio = 50;
	int scan_ratio = 20;
	int update_ratio = 20;
	int insert_ratio = 100 - read_ratio - scan_ratio - update_ratio;

	for(int i=0; i<num; i++){
	    int random = rand() % 100;
	    if(random < read_ratio){
		ofs << "READ ";
		ofs << urls[i];
		ofs << "\n";
	    }
	    else if(random < read_ratio + scan_ratio){
		int range;
		do{
		    range = rand() % 100;
		}while(range <= 0);
		ofs << "SCAN ";
		ofs << urls[i];
		ofs << " " << range;
		ofs << "\n";
	    }
	    else if(random < read_ratio + scan_ratio + update_ratio){
		ofs << "UPDATE ";
		ofs << urls[i];
		ofs << "\n";
	    }
	    else{
		ofs << "INSERT ";
		ofs << urls[insert_idx++];
		ofs << "\n";
	    }
	}
	ofs.close();
    }


    uint64_t average_size = total_size / cnt;
    std::cout << "Total URL Num: \t" << cnt << std::endl;
    std::cout << "Avegaet Bytes: \t" << average_size << std::endl;
    std::cout << "Maximum Bytes: \t" << max_size << std::endl;
    std::cout << "Minimum Bytes: \t" << min_size << std::endl;
    return 0;
}
