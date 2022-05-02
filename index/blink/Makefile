.PHONY: all test clean
CXX = g++ -g -O3 -std=c++17 -march=native
CFLAGS =  -Wno-invalid-offsetof -Wno-deprecated-declarations -Wall -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new
LDLIBS = -lpthread -latomic -ltcmalloc_minimal

all: test

range: range.cpp
	$(CXX) $(CFLAGS) -o bin/range range.cpp $(LDLIBS)  -DBLINK_TREE

test: test.cpp
	$(CXX) $(CFLAGS) -o bin/blink test.cpp $(LDLIBS)  -DBLINK_TREE
	$(CXX) $(CFLAGS) -o bin/timestamp timestamp.cpp $(LDLIBS)  

clean:
	rm bin/*
