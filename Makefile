.PHONY: all micro workload timeseries workload_string clean
CC = gcc
CXX = g++
CFLAGS = -std=c++17 -g -O3 -march=native
DEPSDIR := index/masstree/.deps
DEPCFLAGS = -MD -MF $(DEPSDIR)/$*.d -MP
MEMMGR = -lpapi -latomic -ltcmalloc_minimal 
LDFLAGS = -Wno-invalid-offsetof -Wno-deprecated-declarations -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new $(DEPCFLAGS) -include index/masstree/config.h -I./
LDFLAGS += -DBWTREE_NODEBUG -DNDEBUG -mavx -mavx2 -mbmi2 -mlzcnt -mcx16 

INDEX_LIB = obj/artolc.o obj/artrowex.o index/hot/build/src/libhot-rowex.a index/masstree/mtIndexAPI.a obj/bwtree.o index/blink-hash/build/lib/libblinkhash.a index/blink-buffer/build/lib/libblink_buffer.a index/blink-buffer-batch/build/lib/libblink_buffer_batch.a
INDEX_LIB_SHARED = index/hot/build/src/libhot-rowex.a index/masstree/mtIndexAPI.a index/blink-hash/build/lib/libblinkhash.a index/blink-buffer/build/lib/libblink_buffer.a index/blink-buffer-batch/build/lib/libblink_buffer_batch.a
INDEX_LIB_HEADER = index/ARTOLC/Tree.h index/ARTROWEX/Tree.h index/masstree/mtIndexAPI.hh index/BwTree/bwtree.h index/hot/src/wrapper.h index/BTreeOLC/BTreeOLC_adjacent_layout.h index/blink/tree_optimized.h index/blink-hash/lib/tree.h index/blink-buffer/lib/run.h index/blink-buffer-batch/lib/run.h
INDEX_LIB_STRING = obj/artolc.o obj/artrowex.o index/hot/build/src/libhot-rowex-str.a index/masstree/mtIndexAPI.a obj/bwtree.o index/blink-hash-str/build/lib/libblinkhash.a index/blink-buffer/build/lib/libblink_buffer.a index/blink-buffer-batch/build/lib/libblink_buffer_batch.a
INDEX_LIB_SHARED_STRING = index/hot/build/src/libhot-rowex-str.a index/masstree/mtIndexAPI.a index/blink-hash-str/build/lib/libblinkhash.a index/blink-buffer/build/lib/libblink_buffer.a index/blink-buffer-batch/build/lib/libblink_buffer_batch.a
INDEX_LIB_HEADER_STRING = index/ARTOLC/Tree.h index/ARTROWEX/Tree.h index/masstree/mtIndexAPI.hh index/BwTree/bwtree.h index/hot/src/wrapper.h index/BTreeOLC/BTreeOLC_adjacent_layout.h index/blink/tree_optimized.h index/blink-hash-str/lib/tree.h index/blink-buffer/lib/run.h index/blink-buffer-batch/lib/run.h
INDEX_LIB_FLUSH = obj/artolc.o obj/artrowex.o index/hot/build/src/libhot-rowex.a index/masstree/mtIndexAPI.a obj/bwtree.o index/blink-hash/build/lib/libblinkhash.a index/blink-buffer/build/lib/libblink_buffer_flush.a index/blink-buffer-batch/build/lib/libblink_buffer_batch_flush.a
INDEX_LIB_BREAKDOWN = obj/artolc_breakdown.o obj/artrowex_breakdown.o index/hot/build/src/libhot-rowex-breakdown.a index/masstree/mtIndexAPI.a obj/bwtree_breakdown.o index/blink-hash/build/lib/libblinkhash.a index/blink-buffer/build/lib/libblink_buffer.a index/blink-buffer-batch/build/lib/libblink_buffer_batch.a
INDEX_LIB_SHARED_BREAKDOWN = index/hot/build/src/libhot-rowex-breakdown.a index/masstree/mtIndexAPI.a index/blink-hash/build/lib/libblinkhash.a index/blink-buffer/build/lib/libblink_buffer.a index/blink-buffer-batch/build/lib/libblink_buffer_batch.a

BENCH_LIB_HEADER = include/microbench.h include/index.h include/util.h

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

adms.o: src/adms.cpp adms/inversions.h adms/scramble.h adms/genzipf.h $(BENCH_LIB_HEADER) $(INDEX_LIB_HEADER)
	$(CXX) $(CFLAGS) -c -o obj/adms.o src/adms.cpp $(LDFLAGS)

adms_adds: adms/inversions.c adms/genzipf.c adms/scramble.c
	$(CXX) $(CFLAGS) -c -o obj/inversions.o adms/inversions.c $(LDFLAGS)
	$(CXX) $(CFLAGS) -c -o obj/genzipf.o adms/genzipf.c $(LDFLAGS)
	$(CXX) $(CFLAGS) -c -o obj/scramble.o adms/scramble.c $(LDFLAGS)

adms: adms.o adms_adds
	$(CXX) $(CFLAGS) -o bin/adms obj/adms.o obj/inversions.o obj/genzipf.o obj/scramble.o $(INDEX_LIB) $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb

micro.o: src/microbench.cpp $(BENCH_LIB_HEADER)
	$(CXX) $(CFLAGS) -c -o obj/microbench.o src/microbench.cpp $(LDFLAGS) 

micro: micro.o bwtree.o artolc.o artrowex.o $(INDEX_LIB_SHARED) 
	$(CXX) $(CFLAGS) -o bin/microbench obj/microbench.o $(INDEX_LIB) $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb 

workload.o: src/workload.cpp $(BENCH_LIB_HEADER) $(INDEX_LIB_HEADER)
	$(CXX) $(CFLAGS) -c -o obj/workload.o src/workload.cpp $(LDFLAGS) 

workload: workload.o bwtree.o artolc.o artrowex.o $(INDEX_LIB_SHARED)
	$(CXX) $(CFLAGS) -o bin/workload obj/workload.o $(INDEX_LIB) $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb

timeseries.o: src/timeseries.cpp $(BENCH_LIB_HEADER) $(INDEX_LIB_HEADER)
	$(CXX) $(CFLAGS) -c -o obj/timeseries.o src/timeseries.cpp $(LDFLAGS)

timeseries_breakdown.o: src/timeseries.cpp $(BENCH_LIB_HEADER) $(INDEX_LIB_HEADER)
	$(CXX) $(CFLAGS) -c -o obj/timeseries_breakdown.o src/timeseries.cpp $(LDFLAGS) -DBREAKDOWN

timeseries: timeseries.o bwtree.o artolc.o artrowex.o $(INDEX_LIB_SHARED)
	$(CXX) $(CFLAGS) -o bin/timeseries obj/timeseries.o $(INDEX_LIB) $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb
	$(CXX) $(CFLAGS) -o bin/timeseries_flush obj/timeseries.o $(INDEX_LIB_FLUSH) $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb -DFLUSH

timeseries_breakdown: timeseries_breakdown.o bwtree_breakdown.o artolc_breakdown.o artrowex_breakdown.o $(INDEX_LIB_SHARED_BREAKDOWN)
	$(CXX) $(CFLAGS) -o bin/timeseries_breakdown obj/timeseries_breakdown.o $(INDEX_LIB_BREAKDOWN) $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb -DBREAKDOWN

workload_string.o: src/workload_string.cpp $(BENCH_LIB_HEADER) $(INDEX_LIB_HEADER_STRING)
	$(CXX) $(CFLAGS) -c -o obj/workload_string.o src/workload_string.cpp $(LDFLAGS) -DSTRING_KEY

workload_string: workload_string.o bwtree.o artolc.o artrowex.o $(INDEX_LIB_SHARED_STRING)
	$(CXX) $(CFLAGS) -o bin/workload_string obj/workload_string.o $(INDEX_LIB_STRING) $(MEMMGR) $(LDFLAGS) -lpthread -lm -ltbb  -DSTRING_KEY 

bwtree.o: index/BwTree/bwtree.h index/BwTree/bwtree.cpp
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
