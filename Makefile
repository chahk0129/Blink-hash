.PHONY: all workload workload_string workload_url clean
CC = gcc
CXX = g++
CFLAGS = -std=c++17 -g -O3 -march=native
DEPSDIR := index/masstree/.deps
DEPCFLAGS = -MD -MF $(DEPSDIR)/$*.d -MP
MEMMGR = -lpapi -latomic -ltcmalloc_minimal 
LDFLAGS = -Wno-invalid-offsetof -Wno-deprecated-declarations -Wall -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new $(DEPCFLAGS) -include index/masstree/config.h -I./
LDFLAGS += -DBWTREE_NODEBUG -DNDEBUG -DSIMD -DAVX2 -mavx -mavx2 -mbmi2 -mlzcnt -mcx16 -DLINKED -DAVX2 -DSAMPLING
MICROFLAGS = -Wno-invalid-offsetof -Wno-deprecated-declarations -Wall -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new
AMDuProfDIR = /opt/AMDuProf_3.4-502
PROFFLAGS = #-I$(AMDuProfDIR)/include -L$(AMDuProfDIR)/lib/x64 -lAMDProfileController -lrt
# By default just use 1 thread. Override this option to allow running the
# benchmark with 20 threads. i.e. THREAD_NUM=20 make run_all_atrloc
THREAD_NUM?=1
TYPE?=bwtree

SNAPPY = /usr/lib/libsnappy.so.1.3.0

all: micro workload workload_string workload_url


ifdef STRING_KEY
	$(info Using string key as key type)
	CFLAGS += -DUSE_GENERIC_KEY
endif

micro.o: src/microbench.cpp include/index.h include/util.h
	$(CXX) $(CFLAGS) -c -o obj/microbench.o src/microbench.cpp $(LDFLAGS) -DUPDATE_LOCK

micro: micro.o bwtree.o artolc.o artrowex.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex.a
	$(CXX) $(CFLAGS) -o bin/microbench obj/microbench.o obj/bwtree.o obj/artolc.o obj/artrowex.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex.a $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb -DUPDATE_LOCK

workload.o: src/workload.cpp include/microbench.h include/index.h include/util.h index/masstree/mtIndexAPI.hh index/BwTree/bwtree.h index/hot/src/wrapper.h src/papi_util.cpp index/blink/tree_optimized.h pcm/pcm-memory.cpp pcm/pcm-numa.cpp
	$(CXX) $(CFLAGS) -c -o obj/workload.o src/workload.cpp $(LDFLAGS) $(PROFFLAGS) -DUPDATE_LOCK
	$(CXX) $(CFLAGS) -c -o obj/workload_breakdown.o src/workload.cpp $(LDFLAGS) $(PROFFLAGS) -DBREAKDOWN -DUPDATE_LOCK

workload: workload.o bwtree.o artolc.o artrowex.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex.a pcm/libPCM.a index/blink/tree_optimized.h
	$(CXX) $(CFLAGS) -o bin/workload obj/workload.o obj/bwtree.o obj/artolc.o obj/artrowex.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex.a pcm/libPCM.a $(MEMMGR) $(LDFLAGS) $(PROFFLAGS) -lpthread -lm -ltbb -DUPDATE_LOCK
	$(CXX) $(CFLAGS) -o bin/breakdown_workload obj/workload_breakdown.o obj/bwtree.o obj/artolc_breakdown.o obj/artrowex_breakdown.o index/masstree/mtIndexAPI.a pcm/libPCM.a index/hot/build/src/libhot-rowex-breakdown.a $(MEMMGR) $(LDFLAGS) $(PROFFLAGS) -lpthread -lm -ltbb -DUPDATE_LOCK -DBREAKDOWN

workload_string.o: src/workload_string.cpp include/microbench.h include/index.h include/util.h index/masstree/mtIndexAPI.hh index/BwTree/bwtree.h index/hot/src/wrapper.h index/blink/tree_optimized.h
	$(CXX) $(CFLAGS) -c -o obj/workload_string.o src/workload_string.cpp $(LDFLAGS) $(PROFFLAGS) -DSTRING_KEY -DUPDATE_LOCK
	$(CXX) $(CFLAGS) -c -o obj/workload_string_breakdown.o src/workload_string.cpp $(LDFLAGS) $(PROFFLAGS) -DSTRING_KEY -DBREAKDOWN

workload_string: workload_string.o bwtree.o artolc.o artrowex.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex.a index/blink/tree_optimized.h
	$(CXX) $(CFLAGS) -o bin/workload_string obj/workload_string.o obj/bwtree.o obj/artolc.o obj/artrowex.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex.a index/blink/tree.h index/blink-hash/tree.h $(MEMMGR) $(LDFLAGS) $(PROFFLAGS) -lpthread -lm -ltbb  -DSTRING_KEY -DUPDATE_LOCK
	$(CXX) $(CFLAGS) -o bin/breakdown_workload_string obj/workload_string_breakdown.o obj/bwtree_breakdown.o obj/artolc_breakdown.o obj/artrowex_breakdown.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex-breakdown.a $(MEMMGR) $(LDFLAGS) $(PROFFLAGS) -lpthread -lm -ltbb  -DSTRING_KEY -DBREAKDOWN

workload_url.o: src/workload_url.cpp include/microbench.h include/index.h include/util.h index/masstree/mtIndexAPI.hh index/BwTree/bwtree.h index/hot/src/wrapper.h index/blink/tree_optimized.h
	$(CXX) $(CFLAGS) -c -o obj/workload_url.o src/workload_url.cpp $(LDFLAGS) $(PROFFLAGS) -DURL_KEYS -DUPDATE_LOCK
	$(CXX) $(CFLAGS) -c -o obj/workload_url_breakdown.o src/workload_url.cpp $(LDFLAGS) $(PROFFLAGS) -DURL_KEYS -DBREAKDOWN

workload_url: workload_url.o bwtree.o artolc.o artrowex.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex.a index/blink/tree_optimized.h
	$(CXX) $(CFLAGS) -o bin/workload_url obj/workload_url.o obj/bwtree.o obj/artolc.o obj/artrowex.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex.a $(MEMMGR) $(LDFLAGS) $(PROFFLAGS) -lpthread -lm -ltbb  -DURL_KEYS -DUPDATE_LOCK
	$(CXX) $(CFLAGS) -o bin/breakdown_workload_url obj/workload_url_breakdown.o obj/bwtree_breakdown.o obj/artolc_breakdown.o obj/artrowex_breakdown.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex-breakdown.a $(MEMMGR) $(LDFLAGS) $(PROFFLAGS) -lpthread -lm -ltbb -DURL_KEYS -DBREAKDOWN

bwtree.o: index/BwTree/bwtree.h index//BwTree/bwtree.cpp
	$(CXX) $(CFLAGS) -c -o obj/bwtree.o index/BwTree/bwtree.cpp
	$(CXX) $(CFLAGS) -c -o obj/bwtree_breakdown.o index/BwTree/bwtree.cpp -DBREAKDOWN

artolc.o: index/ARTOLC/*.cpp index/ARTOLC/*.h
	$(CXX) $(CFLAGS) index/ARTOLC/Tree.cpp -c -o obj/artolc.o $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb
	$(CXX) $(CFLAGS) index/ARTOLC/Tree.cpp -c -o obj/artolc_breakdown.o $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb -DBREAKDOWN

artrowex.o: index/ARTROWEX/*.cpp index/ARTROWEX/*.h
	$(CXX) $(CFLAGS) index/ARTROWEX/Tree.cpp -c -o obj/artrowex.o $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb
	$(CXX) $(CFLAGS) index/ARTROWEX/Tree.cpp -c -o obj/artrowex_breakdown.o $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb -DBREAKDOWN

clean:
	$(RM) bin/* obj/*
