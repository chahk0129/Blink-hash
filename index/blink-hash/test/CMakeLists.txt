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
