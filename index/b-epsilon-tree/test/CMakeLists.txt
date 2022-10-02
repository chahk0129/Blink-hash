add_executable(test_tree test.cpp)
target_link_libraries(test_tree betree tcmalloc_minimal pthread)

add_executable(test_buffer test.cpp)
target_compile_definitions(test_buffer PUBLIC -DBUFFER)
target_link_libraries(test_buffer betree tcmalloc_minimal pthread)

add_executable(rdtsc_tree rdtsc.cpp)
target_link_libraries(rdtsc_tree betree tcmalloc_minimal pthread)

add_executable(rdtsc_buffer rdtsc.cpp)
target_compile_definitions(rdtsc_buffer PUBLIC -DBUFFER)
target_link_libraries(rdtsc_buffer betree tcmalloc_minimal pthread)
