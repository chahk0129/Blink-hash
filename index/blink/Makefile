.PHONY: all test clean
CXX = g++ -g -O3 -std=c++17 -march=native
CFLAGS =  -Wno-invalid-offsetof -Wno-deprecated-declarations -Wall -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new
LDLIBS = -lpthread -latomic -ltcmalloc_minimal

all: test

test: test.cpp
	$(CXX) $(CFLAGS) -o bin/blink test.cpp $(LDLIBS)  -DBLINK_TREE
	$(CXX) $(CFLAGS) -o bin/blink2 test.cpp $(LDLIBS) 
#	$(CXX) $(CFLAGS) -o bin/mixed test.cpp $(LDLIBS) -DMIXED
#	$(CXX) $(CFLAGS) -o bin/blink_opt test.cpp $(LDLIBS) -DOPTIMIZED
#	$(CXX) $(CFLAGS) -o bin/mixed_opt test.cpp $(LDLIBS) -DMIXED -DOPTIMIZED
#	$(CXX) $(CFLAGS) -o bin/rdtsc rdtsc.cpp $(LDLIBS) 
#	$(CXX) $(CFLAGS) -o bin/rdtsc_optimized rdtsc.cpp $(LDLIBS) -DOPTIMIZED

clean:
	rm bin/*
