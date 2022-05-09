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
#include <experimental/filesystem>

static uint64_t email_size = 100000000;

bool open_input(std::ifstream& ifs, std::string path){
    ifs.open(path);
    if(!ifs.is_open()){
	return false;
    }
    return true;
}

bool close_input(std::ifstream& ifs){
    ifs.close();
}

void collect_emails(std::ifstream& ifs, std::unordered_map<std::string, bool>& data){
    std::string email;
    while(!ifs.eof()){
	ifs >> email;
	if(data.find(email) != data.end()) // duplicate email
	    continue;

	auto len = strlen(email.c_str());
	if(len == 0) // containing null
	    continue;

	if(len > 31) // we need to add \0 to end of key in the final workload 
	    continue;

	if(email.find(":") != std::string::npos || email.find("$") != std::string::npos || // invalid email
	   email.find("!") != std::string::npos || email.find("#") != std::string::npos || 
	   email.find("%") != std::string::npos || email.find("^") != std::string::npos || 
	   email.find("&") != std::string::npos || email.find("*") != std::string::npos || 
	   email.find("(") != std::string::npos || email.find(")") != std::string::npos || 
	   email.find("+") != std::string::npos || email.find("=") != std::string::npos || 
	   email.find("|") != std::string::npos || email.find("[") != std::string::npos || 
	   email.find("]") != std::string::npos || email.find("{") != std::string::npos || 
	   email.find("}") != std::string::npos || email.find(",") != std::string::npos || 
	   email.find("/") != std::string::npos || email.find("?") != std::string::npos || 
	   email.find("\"") != std::string::npos || email.find("'") != std::string::npos || 
	   email.find("\\") != std::string::npos || email.find(";") != std::string::npos)
	    continue;

	data.insert(std::make_pair(email, true));
    }
}

void iter_input(std::string path, std::unordered_map<std::string, bool>& data){
    for(const auto& file: std::experimental::filesystem::directory_iterator(path)){
	auto status = std::experimental::filesystem::status(file.path());
	if(status.type() == std::experimental::filesystem::file_type::directory){
	    iter_input(file.path(), data);
	    continue;
	}

	std::ifstream ifs;
	if(!open_input(ifs, file.path())){
	    std::cerr << "file open failed: " << file.path() << std::endl;
	}

	collect_emails(ifs, data);
	close_input(ifs);
    }
}

void write_output(std::string path, std::unordered_map<std::string, bool> data){
    uint64_t cnt = 0;
    std::ofstream ofs;
    ofs.open(path);
    if(!ofs.is_open()){
	std::cerr << "output file is not opened! " << path << std::endl;
	exit(0);
    }
    std::cout << "output file path: " << path << std::endl;

    for(auto it=data.begin(); it!=data.end(); it++){
	cnt++;
	ofs << it->first << std::endl;
    }
    ofs.close();
    std::cout << "total " << cnt << " emails are extracted" << std::endl;
}


int main(int argc, char* argv[]){
    if(argc != 2){
	std::cout << "Usage: " << argv[0] << " (input path)" << std::endl;
	exit(0);
    }

    std::string path(argv[1]);
    std::cout << "path: " << path << std::endl;

    std::unordered_map<std::string, bool> data;
    data.reserve(email_size); // 100M
    iter_input(path, data);

    std::string output = path;
    auto back = output.back();
    if(back != '/')
	output.append("/");
    output.append("raw_emails.dat");
    write_output(output, data);

    return 0;
}

