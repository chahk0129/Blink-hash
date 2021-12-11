# CMake generated Testfile for 
# Source directory: /users/hcha/index_bench/index/libcuckoo/tests/stress-tests
# Build directory: /users/hcha/index_bench/index/libcuckoo/build/tests/stress-tests
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(stress_checked "/users/hcha/index_bench/index/libcuckoo/build/tests/stress-tests/stress_checked")
set_tests_properties(stress_checked PROPERTIES  _BACKTRACE_TRIPLES "/users/hcha/index_bench/index/libcuckoo/tests/stress-tests/CMakeLists.txt;15;add_test;/users/hcha/index_bench/index/libcuckoo/tests/stress-tests/CMakeLists.txt;0;")
add_test(stress_unchecked "/users/hcha/index_bench/index/libcuckoo/build/tests/stress-tests/stress_unchecked")
set_tests_properties(stress_unchecked PROPERTIES  _BACKTRACE_TRIPLES "/users/hcha/index_bench/index/libcuckoo/tests/stress-tests/CMakeLists.txt;16;add_test;/users/hcha/index_bench/index/libcuckoo/tests/stress-tests/CMakeLists.txt;0;")
