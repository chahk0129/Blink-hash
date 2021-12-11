.PHONY: all clean
CXX = g++ -g -O3 -std=c++17 -march=native
CFLAGS =  -Wno-invalid-offsetof -Wno-deprecated-declarations -Wall -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new
ifdef __x86__
SIMD_FLAGS = -mavx -mavx2 -mbmi2 -mlzcnt -mcx16
else
SIMD_FLAGS = -ftree-vectorize -mfpu=neon
endif
LDLIBS = -lpthread -latomic -ltcmalloc_minimal #-ljemalloc 

all: test rdtsc

rdtsc: rdtsc.cpp
	$(CXX) $(CFLAGS) -o bin/rdtsc_node rdtsc.cpp $(LDLIBS)  -DLINKED
#	$(CXX) $(CFLAGS) -o bin/rdtsc_random_entry_avx2 rdtsc.cpp $(LDLIBS) $(SIMD_FLAGS) -DENTRY_SAMPLING -DSIMD
#	$(CXX) $(CFLAGS) -o bin/rdtsc_random_entry_avx2_exp rdtsc.cpp $(LDLIBS) $(SIMD_FLAGS) -DENTRY_SAMPLING -DSIMD -DEXP
#	$(CXX) $(CFLAGS) -o bin/rdtsc_random_entry_avx2_linked rdtsc.cpp $(LDLIBS) $(SIMD_FLAGS) -DENTRY_SAMPLING -DAVX2 -DLINKED
#	$(CXX) $(CFLAGS) -o bin/rdtsc_random_entry_avx512_linked rdtsc.cpp $(LDLIBS) $(SIMD_FLAGS) -DENTRY_SAMPLING -DAVX512 -DLINKED
#	$(CXX) $(CFLAGS) -o bin/rdtsc_random_entry_simd_linked_exp rdtsc.cpp $(LDLIBS) $(SIMD_FLAGS) -DENTRY_SAMPLING -DSIMD -DLINKED2
#	$(CXX) $(CFLAGS) -o bin/breakdown_rdtsc rdtsc.cpp $(LDLIBS) -DBREAKDOWN 
#	$(CXX) $(CFLAGS) -o bin/breakdown_rdtsc_random_entry_simd rdtsc.cpp $(LDLIBS) -DBREAKDOWN -DAVX2 -DENTRY_SAMPLING
#	$(CXX) $(CFLAGS) -o bin/breakdown_rdtsc_random_entry_simd_linked rdtsc.cpp $(LDLIBS) -DBREAKDOWN -DAVX2 -DENTRY_SAMPLING -DLINKED

test: test.cpp
	$(CXX) $(CFLAGS) -o bin/test_node test.cpp $(LDLIBS)  -DLINKED
#	$(CXX) $(CFLAGS) -o bin/test_random_entry_simd_bulkcopy test.cpp $(LDLIBS)  -DBULK_COPY -DSIMD -DENTRY_SAMPLING
#	$(CXX) $(CFLAGS) -o bin/test_random_entry_simd_bulkcopy_batch test.cpp $(LDLIBS)  -DBULK_COPY -DSIMD -DENTRY_SAMPLING -DBATCH
#	$(CXX) $(CFLAGS) -o bin/test_random_entry test.cpp $(LDLIBS) -DENTRY_SAMPLING
#	$(CXX) $(CFLAGS) -o bin/test_random_entry_avx2 test.cpp $(LDLIBS) $(SIMD_FLAGS) -DENTRY_SAMPLING -DSIMD
#	$(CXX) $(CFLAGS) -o bin/test_random_entry_avx2_exp test.cpp $(LDLIBS) $(SIMD_FLAGS) -DENTRY_SAMPLING -DSIMD -DEXP
#	$(CXX) $(CFLAGS) -o bin/test_random_entry_avx2_linked test.cpp $(LDLIBS) $(SIMD_FLAGS) -DENTRY_SAMPLING -DAVX2 -DLINKED
#	$(CXX) $(CFLAGS) -o bin/test_random_entry_avx512_linked test.cpp $(LDLIBS) $(SIMD_FLAGS) -DENTRY_SAMPLING -DAVX512 -DLINKED
#	$(CXX) $(CFLAGS) -o bin/test_random_entry_simd_linked_exp test.cpp $(LDLIBS) $(SIMD_FLAGS) -DENTRY_SAMPLING -DSIMD -DLINKED2
#	$(CXX) $(CFLAGS) -o bin/test_random_entry_simd_bulkcopy_exp test.cpp $(LDLIBS) $(SIMD_FLAGS) -DENTRY_SAMPLING -DSIMD -DEXP -DBULK_COPY
#	$(CXX) $(CFLAGS) -o bin/breakdown_test test.cpp $(LDLIBS) -DBREAKDOWN
#	$(CXX) $(CFLAGS) -o bin/breakdown_test_random_entry_simd_linked test.cpp $(LDLIBS) -DBREAKDOWN -DAVX2 -DENTRY_SAMPLING -DLINKED
#	$(CXX) $(CFLAGS) -o bin/breakdown_test_random_entry_simd test.cpp $(LDLIBS) -DBREAKDOWN -DAVX2 -DENTRY_SAMPLING

clean:
	rm bin/*
