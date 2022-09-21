.PHONY: all micro workload timeseries workload_string clean
CC = gcc
CXX = g++
CFLAGS = -std=c++17 -g -O3 -march=native
DEPSDIR := index/masstree/.deps
DEPCFLAGS = -MD -MF $(DEPSDIR)/$*.d -MP
MEMMGR = -lpapi -latomic -ltcmalloc_minimal 
LDFLAGS = -Wno-invalid-offsetof -Wno-deprecated-declarations -Wall -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new $(DEPCFLAGS) -include index/masstree/config.h -I./
LDFLAGS += -DBWTREE_NODEBUG -DNDEBUG -mavx -mavx2 -mbmi2 -mlzcnt -mcx16 
MICROFLAGS = -Wno-invalid-offsetof -Wno-deprecated-declarations -Wall -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new
# By default just use 1 thread. Override this option to allow running the
# benchmark with 20 threads. i.e. THREAD_NUM=20 make run_all_atrloc
THREAD_NUM?=1
TYPE?=bwtree

SNAPPY = /usr/lib/libsnappy.so.1.3.0

all: micro workload timeseries workload_string


ifdef STRING_KEY
	$(info Using string key as key type)
	CFLAGS += -DUSE_GENERIC_KEY
endif

adms.o: src/adms.cpp adms/inversions.h adms/scramble.h adms/genzipf.h include/microbench.h include/index.h include/util.h index/masstree/mtIndexAPI.hh index/BwTree/bwtree.h index/hot/src/wrapper.h src/papi_util.cpp index/BTreeOLC/BTreeOLC_adjacent_layout.h index/blink/tree_optimized.h index/blink-hash/lib/tree.h pcm/pcm-memory.cpp pcm/pcm-numa.cpp
	$(CXX) $(CFLAGS) -c -o obj/adms.o src/adms.cpp $(LDFLAGS)

adms_adds: adms/inversions.c adms/genzipf.c adms/scramble.c
	$(CXX) $(CFLAGS) -c -o obj/inversions.o adms/inversions.c $(LDFLAGS)
	$(CXX) $(CFLAGS) -c -o obj/genzipf.o adms/genzipf.c $(LDFLAGS)
	$(CXX) $(CFLAGS) -c -o obj/scramble.o adms/scramble.c $(LDFLAGS)

adms: adms.o adms_adds
	$(CXX) $(CFLAGS) -o bin/adms obj/adms.o obj/inversions.o obj/genzipf.o obj/scramble.o obj/artolc.o obj/artrowex.o obj/bwtree.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex.a index/blink-hash/build/lib/libblinkhash.a $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb

micro.o: src/microbench.cpp include/index.h include/util.h
	$(CXX) $(CFLAGS) -c -o obj/microbench.o src/microbench.cpp $(LDFLAGS) 

micro: micro.o bwtree.o artolc.o artrowex.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex.a index/blink-hash/build/lib/libblinkhash.a
	$(CXX) $(CFLAGS) -o bin/microbench obj/microbench.o obj/bwtree.o obj/artolc.o obj/artrowex.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex.a index/blink-hash/build/lib/libblinkhash.a $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb 

workload.o: src/workload.cpp include/microbench.h include/index.h include/util.h index/masstree/mtIndexAPI.hh index/BwTree/bwtree.h index/hot/src/wrapper.h src/papi_util.cpp index/BTreeOLC/BTreeOLC_adjacent_layout.h index/blink/tree_optimized.h index/blink-hash/lib/tree.h pcm/pcm-memory.cpp pcm/pcm-numa.cpp
	$(CXX) $(CFLAGS) -c -o obj/workload.o src/workload.cpp $(LDFLAGS) $(PROFFLAGS)

workload: workload.o bwtree.o artolc.o artrowex.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex.a index/blink-hash/build/lib/libblinkhash.a pcm/libPCM.a
	$(CXX) $(CFLAGS) -o bin/workload obj/workload.o obj/bwtree.o obj/artolc.o obj/artrowex.o index/masstree/mtIndexAPI.a index/blink-hash/build/lib/libblinkhash.a index/hot/build/src/libhot-rowex.a pcm/libPCM.a $(MEMMGR) $(LDFLAGS) $(PROFFLAGS) -lpthread -lm -ltbb

timeseries.o: src/timeseries.cpp include/microbench.h include/index.h include/util.h index/masstree/mtIndexAPI.hh index/BwTree/bwtree.h index/hot/src/wrapper.h src/papi_util.cpp index/BTreeOLC/BTreeOLC_adjacent_layout.h index/blink/tree_optimized.h index/blink-hash/lib/tree.h pcm/pcm-memory.cpp pcm/pcm-numa.cpp
	$(CXX) $(CFLAGS) -c -o obj/timeseries.o src/timeseries.cpp $(LDFLAGS) $(PROFFLAGS)

timeseries_breakdown.o: src/timeseries.cpp include/microbench.h include/index.h include/util.h index/masstree/mtIndexAPI.hh index/BwTree/bwtree.h index/hot/src/wrapper.h src/papi_util.cpp index/BTreeOLC/BTreeOLC_adjacent_layout.h index/blink/tree_optimized.h index/blink-hash/lib/tree.h pcm/pcm-memory.cpp pcm/pcm-numa.cpp
	$(CXX) $(CFLAGS) -c -o obj/timeseries_breakdown.o src/timeseries.cpp $(LDFLAGS) $(PROFFLAGS) -DBREAKDOWN

timeseries: timeseries.o bwtree.o artolc.o artrowex.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex.a pcm/libPCM.a index/blink/tree_optimized.h index/blink-hash/build/lib/libblinkhash.a pcm/libPCM.a
	$(CXX) $(CFLAGS) -o bin/timeseries obj/timeseries.o obj/bwtree.o obj/artolc.o obj/artrowex.o index/masstree/mtIndexAPI.a index/blink-hash/build/lib/libblinkhash.a index/hot/build/src/libhot-rowex.a pcm/libPCM.a $(MEMMGR) $(LDFLAGS) $(PROFFLAGS) -lpthread -lm -ltbb

timeseries_breakdown: timeseries_breakdown.o bwtree_breakdown.o artolc_breakdown.o artrowex_breakdown.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex-breakdown.a pcm/libPCM.a index/blink/tree_optimized.h index/blink-hash/build/lib/libblinkhash.a pcm/libPCM.a
	$(CXX) $(CFLAGS) -o bin/timeseries_breakdown obj/timeseries_breakdown.o obj/bwtree_breakdown.o obj/artolc_breakdown.o obj/artrowex_breakdown.o index/masstree/mtIndexAPI.a index/blink-hash/build/lib/libblinkhash.a index/hot/build/src/libhot-rowex-breakdown.a pcm/libPCM.a $(MEMMGR) $(LDFLAGS) $(PROFFLAGS) -lpthread -lm -ltbb -DBREAKDOWN

workload_string.o: src/workload_string.cpp include/microbench.h include/index.h include/util.h index/masstree/mtIndexAPI.hh index/BwTree/bwtree.h index/hot/src/wrapper.h src/papi_util.cpp index/BTreeOLC/BTreeOLC_adjacent_layout.h index/blink/tree_optimized.h pcm/pcm-numa.cpp
	$(CXX) $(CFLAGS) -c -o obj/workload_string.o src/workload_string.cpp $(LDFLAGS) $(PROFFLAGS) -DSTRING_KEY

workload_string: workload_string.o bwtree.o artolc.o artrowex.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex-str.a index/blink/tree_optimized.h index/blink-hash-str/build/lib/libblinkhash.a
	$(CXX) $(CFLAGS) -o bin/workload_string obj/workload_string.o obj/bwtree.o obj/artolc.o obj/artrowex.o index/masstree/mtIndexAPI.a index/hot/build/src/libhot-rowex-str.a index/blink-hash-str/build/lib/libblinkhash.a index/blink/tree.h $(MEMMGR) $(LDFLAGS) $(PROFFLAGS) -lpthread -lm -ltbb  -DSTRING_KEY 

bwtree.o: index/BwTree/bwtree.h index//BwTree/bwtree.cpp
	$(CXX) $(CFLAGS) -c -o obj/bwtree.o index/BwTree/bwtree.cpp

bwtree_breakdown.o: index/BwTree/bwtree.h index//BwTree/bwtree.cpp
	$(CXX) $(CFLAGS) -c -o obj/bwtree_breakdown.o index/BwTree/bwtree.cpp -DBREAKDOWN

artolc.o: index/ARTOLC/*.cpp index/ARTOLC/*.h
	$(CXX) $(CFLAGS) index/ARTOLC/Tree.cpp -c -o obj/artolc.o $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb

artolc_breakdown.o: index/ARTOLC/*.cpp index/ARTOLC/*.h
	$(CXX) $(CFLAGS) index/ARTOLC/Tree.cpp -c -o obj/artolc_breakdown.o $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb -DBREAKDOWN

artrowex.o: index/ARTROWEX/*.cpp index/ARTROWEX/*.h
	$(CXX) $(CFLAGS) index/ARTROWEX/Tree.cpp -c -o obj/artrowex.o $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb

artrowex_breakdown.o: index/ARTROWEX/*.cpp index/ARTROWEX/*.h
	$(CXX) $(CFLAGS) index/ARTROWEX/Tree.cpp -c -o obj/artrowex_breakdown.o $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb -DBREAKDOWN


clean:
	$(RM) bin/* obj/*
