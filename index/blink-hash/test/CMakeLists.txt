add_executable(test test.cpp)
add_executable(range range.cpp)
add_executable(rdtsc rdtsc.cpp)

target_link_libraries(test blinkhash pthread)
target_link_libraries(range blinkhash pthread)
target_link_libraries(rdtsc blinkhash pthread)

add_test(NAME blinkhash_test 
        COMMAND blinkhash_test)
