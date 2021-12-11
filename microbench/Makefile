.PHONY: all likwid clean
CXX = g++ -std=c++17 -march=native
CXXFLAGS = -g -O3 
AMDuProf_DIR = /opt/AMDuProf_3.4-502
all: 
	$(CXX) $(CXXFLAGS) -o bin/amduprof_arr amduprof_arr.cpp -lpthread -I../ -I$(AMDuProf_DIR)/include -L$(AMDuProf_DIR)/lib/x64 -lAMDProfileController -lrt
	$(CXX) $(CXXFLAGS) -o bin/amduprof_test amduprof_test.cpp -lpthread -I../ -I$(AMDuProf_DIR)/include -L$(AMDuProf_DIR)/lib/x64 -lAMDProfileController -lrt
	$(CXX) $(CXXFLAGS) -o bin/getchar getchar.cpp -I../
	$(CXX) $(CXXFLAGS) -o bin/test test.cpp -lpthread -I../
	$(CXX) $(CXXFLAGS) -o bin/papi_test papi_test.cpp -lpthread -I../ -lpapi
	$(CXX) $(CXXFLAGS) -o bin/memcpy memcpy.cpp -lpthread -ltcmalloc_minimal
	$(CXX) $(CXXFLAGS) -o bin/malloc_diff malloc.cpp -lpthread -ltcmalloc_minimal
	$(CXX) $(CXXFLAGS) -o bin/malloc_same malloc.cpp -lpthread -ltcmalloc_minimal -DSAME
	$(CXX) $(CXXFLAGS) -o bin/fetch_add fetch_add.cpp -lpthread
	$(CXX) $(CXXFLAGS) -o bin/cas cas_test.cpp -lpthread
	$(CXX) $(CXXFLAGS) -o bin/atomic_load atomic_load.cpp -lpthread
	$(CXX) $(CXXFLAGS) -o bin/array_of_token mem_location_test.cpp -lpthread -DARRAY_OF_TOKEN
	$(CXX) $(CXXFLAGS) -o bin/token_of_array mem_location_test.cpp -lpthread
	$(CXX) $(CXXFLAGS) -o bin/median median.cpp -lpthread
	$(CXX) $(CXXFLAGS) -o bin/node_olc node_test.cpp -lpthread
	$(CXX) $(CXXFLAGS) -o bin/node_mutex node_test.cpp -lpthread -DMUTEX

likwid:
	$(CXX) $(CXXFLAGS) -o bin/likwid_test likwid_test.cpp -lpthread -llikwid -lm
clean: 
	rm -f bin/*
