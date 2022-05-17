add_executable(test test.cpp)

add_executable(range range.cpp)
add_executable(rdtsc rdtsc.cpp)
add_executable(rdtsc_debug rdtsc.cpp)
add_executable(mixed mixed.cpp)

add_executable(timestamp timestamp.cpp)
## factor analysis
add_executable(baseline_ timestamp.cpp)
add_executable(fingerprint_ timestamp.cpp)
add_executable(simd_ timestamp.cpp)
add_executable(sampling_ timestamp.cpp)
add_executable(linked_ timestamp.cpp)
add_executable(adapt_ timestamp.cpp)

target_link_libraries(test blinkhash pthread)
target_link_libraries(range blinkhash pthread)
target_link_libraries(rdtsc blinkhash pthread)
target_link_libraries(mixed blinkhash pthread)

target_link_libraries(rdtsc_debug sampling_debug pthread)

target_link_libraries(baseline_ baseline pthread)
target_link_libraries(fingerprint_ fingerprint pthread)
target_link_libraries(simd_ simd pthread)
target_link_libraries(sampling_ sampling pthread)
target_link_libraries(linked_ linked pthread)
target_link_libraries(adapt_ adapt pthread)
target_link_libraries(timestamp blinkhash pthread)

#add_test(NAME blinkhash_test 
#        COMMAND blinkhash_test)
