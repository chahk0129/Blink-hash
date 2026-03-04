add_executable(test test.cpp)
target_link_libraries(test blinkhash pthread)

add_executable(range range.cpp)
target_link_libraries(range blinkhash pthread)

add_executable(rdtsc rdtsc.cpp)
target_link_libraries(rdtsc blinkhash pthread)
#add_executable(rdtsc_debug rdtsc.cpp)
#target_link_libraries(rdtsc_debug sampling_debug pthread)

add_executable(mixed mixed.cpp)
target_link_libraries(mixed blinkhash pthread)

add_executable(timestamp timestamp.cpp)
target_link_libraries(timestamp blinkhash pthread)

# String key (GenericKey<32>) test — Phase 0 verification
add_executable(test_string test_string.cpp)
target_link_libraries(test_string blinkhash pthread)

# ---- Latency benchmark (sync baseline) ----
add_executable(bench_range_latency_sync bench_range_latency.cpp)
target_link_libraries(bench_range_latency_sync adapt pthread)

# ---- Latency benchmark (async) ----
add_executable(bench_range_latency_async bench_range_latency.cpp)
target_link_libraries(bench_range_latency_async blinkhash_async pthread)

# ---- Throughput benchmark (sync baseline) ----
add_executable(bench_range_throughput_sync bench_range_throughput.cpp)
target_link_libraries(bench_range_throughput_sync adapt pthread)

# ---- Throughput benchmark (async) ----
add_executable(bench_range_throughput_async bench_range_throughput.cpp)
target_link_libraries(bench_range_throughput_async blinkhash_async pthread)

# ---- Conversion counter (async only) ----
add_executable(bench_convert_count bench_convert_count.cpp)
target_link_libraries(bench_convert_count blinkhash_async ${TBB_LIBRARIES})
target_compile_definitions(bench_convert_count PRIVATE ADAPTATION ASYNC_ADAPT)

# ---- Phase 1 WAL tests ----
add_executable(test_wal_record test_wal_record.cpp)
target_link_libraries(test_wal_record blinkhash_wal pthread)

add_executable(test_wal_ring test_wal_ring.cpp)
target_link_libraries(test_wal_ring blinkhash_wal pthread)
# ---- Phase 2 WAL tests ----
add_executable(test_wal_e2e test_wal_e2e.cpp)
target_link_libraries(test_wal_e2e blinkhash_wal pthread)

add_executable(bench_wal_commit_latency bench_wal_commit_latency.cpp)
target_link_libraries(bench_wal_commit_latency blinkhash_wal pthread)
# ---- Phase 3 WAL integration test ----
add_executable(test_wal_hooks test_wal_hooks.cpp)
target_link_libraries(test_wal_hooks blinkhash blinkhash_wal pthread)
# ---- Phase 4 WAL recovery test ----
add_executable(test_wal_recovery test_wal_recovery.cpp)
target_link_libraries(test_wal_recovery blinkhash blinkhash_wal pthread)
# ---- Phase 5 Checkpoint test ----
add_executable(test_wal_checkpoint test_wal_checkpoint.cpp)
target_link_libraries(test_wal_checkpoint blinkhash blinkhash_wal pthread)
# ---- Phase 6 Persistence tests ----
add_executable(test_node_map test_node_map.cpp)
target_link_libraries(test_node_map blinkhash blinkhash_wal pthread)

add_executable(test_buffer_pool test_buffer_pool.cpp)
target_link_libraries(test_buffer_pool blinkhash blinkhash_wal pthread)
## factor analysis
#add_executable(baseline_ timestamp.cpp)
#target_link_libraries(baseline_ baseline pthread)
#add_executable(fingerprint_ timestamp.cpp)
#target_link_libraries(fingerprint_ fingerprint pthread)
#add_executable(simd_ timestamp.cpp)
#target_link_libraries(simd_ simd pthread)
#add_executable(sampling_ timestamp.cpp)
#target_link_libraries(sampling_ sampling pthread)
#add_executable(linked_ timestamp.cpp)
#target_link_libraries(linked_ linked pthread)
#add_executable(adapt_ timestamp.cpp)
#target_link_libraries(adapt_ adapt pthread)
#
#
#
