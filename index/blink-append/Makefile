.PHONY: all clean
CXX = g++ -g -O3 -std=c++17 -march=native
CFLAGS =  -Wno-invalid-offsetof -Wno-deprecated-declarations -Wall -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new
LDLIBS = -lpthread -latomic -ltcmalloc_minimal

all: test

test: test.cpp
	$(CXX) $(CFLAGS) -o bin/linear test.cpp $(LDLIBS) 
	$(CXX) $(CFLAGS) -o bin/linear_time test.cpp $(LDLIBS)  -DTIME

clean:
	rm bin/*
