.PHONY: all clean
CXX = g++ -g -O3 -std=c++17 -march=native
CFLAGS =  -Wno-invalid-offsetof -Wno-deprecated-declarations -Wall -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new
SIMD_FLAGS = -mavx -mavx2 -mbmi2 -mlzcnt -mcx16
LDLIBS = -lpthread -latomic -ltcmalloc_minimal #-ljemalloc 

all: test rdtsc

rdtsc: rdtsc.cpp
	$(CXX) $(CFLAGS) -o bin/rdtsc_linked_old rdtsc.cpp $(LDLIBS)  -DLINKED -DAVX2 -DSAMPLING -DOLD
	$(CXX) $(CFLAGS) -o bin/rdtsc_linked_new rdtsc.cpp $(LDLIBS)  -DLINKED -DAVX2 -DSAMPLING
#	$(CXX) $(CFLAGS) -o bin/rdtsc_baseline rdtsc.cpp $(LDLIBS)
#	$(CXX) $(CFLAGS) -o bin/rdtsc_simd rdtsc.cpp $(LDLIBS) -DAVX2
#	$(CXX) $(CFLAGS) -o bin/rdtsc_simd512 rdtsc.cpp $(LDLIBS) -DAVX512
#	$(CXX) $(CFLAGS) -o bin/rdtsc_sample rdtsc.cpp $(LDLIBS) -DAVX2 -DSAMPLING
#	$(CXX) $(CFLAGS) -o bin/rdtsc_sample512 rdtsc.cpp $(LDLIBS) -DAVX512 -DSAMPLING


test: test.cpp
	$(CXX) $(CFLAGS) -o bin/test_linked_old test.cpp $(LDLIBS)  -DLINKED -DAVX2 -DSAMPLING -DOLD
	$(CXX) $(CFLAGS) -o bin/test_linked_new test.cpp $(LDLIBS)  -DLINKED -DAVX2 -DSAMPLING
#	$(CXX) $(CFLAGS) -o bin/test_baseline test.cpp $(LDLIBS) -DAVX
#	$(CXX) $(CFLAGS) -o bin/test_simd test.cpp $(LDLIBS) -DAVX2
#	$(CXX) $(CFLAGS) -o bin/test_sample test.cpp $(LDLIBS) -DAVX2 -DSAMPLING

range: range.cpp
	$(CXX) $(CFLAGS) -o bin/range range.cpp $(LDLIBS)  -DLINKED -DAVX2 -DSAMPLING
	$(CXX) $(CFLAGS) -o bin/range_breakdown range.cpp $(LDLIBS)  -DLINKED -DAVX2 -DSAMPLING -DRANGE_BREAKDOWN
	$(CXX) $(CFLAGS) -o bin/range_comp range.cpp $(LDLIBS)  -DLINKED -DAVX2 -DSAMPLING -DKEY_COMP
	$(CXX) $(CFLAGS) -o bin/range_comp_breakdown range.cpp $(LDLIBS)  -DLINKED -DAVX2 -DSAMPLING -DKEY_COMP -DRANGE_BREAKDOWN
	
clean:
	rm bin/*
