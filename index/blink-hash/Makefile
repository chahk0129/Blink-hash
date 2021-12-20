.PHONY: all clean
CXX = g++ -g -O3 -std=c++17 -march=native
CFLAGS =  -Wno-invalid-offsetof -Wno-deprecated-declarations -Wall -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new
SIMD_FLAGS = -mavx -mavx2 -mbmi2 -mlzcnt -mcx16
LDLIBS = -lpthread -latomic -ltcmalloc_minimal #-ljemalloc 

all: test rdtsc

rdtsc: rdtsc.cpp
	$(CXX) $(CFLAGS) -o bin/rdtsc_linked rdtsc.cpp $(LDLIBS)  -DLINKED -DAVX2 -DSAMPLING
	$(CXX) $(CFLAGS) -o bin/rdtsc_baseline rdtsc.cpp $(LDLIBS)
	$(CXX) $(CFLAGS) -o bin/rdtsc_simd rdtsc.cpp $(LDLIBS) -DAVX2
	$(CXX) $(CFLAGS) -o bin/rdtsc_sample rdtsc.cpp $(LDLIBS) -DAVX2 -DSAMPLING


test: test.cpp
	$(CXX) $(CFLAGS) -o bin/test_linked test.cpp $(LDLIBS)  -DLINKED -DAVX2 -DSAMPLING
	$(CXX) $(CFLAGS) -o bin/test_baseline test.cpp $(LDLIBS)
	$(CXX) $(CFLAGS) -o bin/test_simd test.cpp $(LDLIBS) -DAVX2
	$(CXX) $(CFLAGS) -o bin/test_sample test.cpp $(LDLIBS) -DAVX2 -DSAMPLING

clean:
	rm bin/*
