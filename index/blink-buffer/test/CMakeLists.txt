add_executable(tree_test test.cpp)
target_link_libraries(tree_test blink_buffer pthread)

add_executable(tree_timestamp timestamp.cpp)
target_link_libraries(tree_timestamp blink_buffer pthread)

add_executable(run_test run_test.cpp)
target_link_libraries(run_test blink_buffer pthread)

add_executable(run_timestamp run_timestamp.cpp)
target_link_libraries(run_timestamp blink_buffer pthread)

add_executable(run_timestamp_flush run_timestamp.cpp)
target_compile_definitions(run_timestamp_flush PUBLIC -DFLUSH)
target_link_libraries(run_timestamp_flush blink_buffer_flush pthread)
