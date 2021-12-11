# CMake generated Testfile for 
# Source directory: /users/hcha/index_bench/index/libcuckoo/tests/universal-benchmark
# Build directory: /users/hcha/index_bench/index/libcuckoo/build/tests/universal-benchmark
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(pure_read "/users/hcha/index_bench/index/libcuckoo/build/tests/universal-benchmark/universal_benchmark" "--reads" "100" "--prefill" "75" "--total-ops" "500" "--initial-capacity" "23")
set_tests_properties(pure_read PROPERTIES  _BACKTRACE_TRIPLES "/users/hcha/index_bench/index/libcuckoo/tests/universal-benchmark/CMakeLists.txt;25;add_test;/users/hcha/index_bench/index/libcuckoo/tests/universal-benchmark/CMakeLists.txt;0;")
add_test(pure_insert "/users/hcha/index_bench/index/libcuckoo/build/tests/universal-benchmark/universal_benchmark" "--inserts" "100" "--total-ops" "75" "--initial-capacity" "23")
set_tests_properties(pure_insert PROPERTIES  _BACKTRACE_TRIPLES "/users/hcha/index_bench/index/libcuckoo/tests/universal-benchmark/CMakeLists.txt;27;add_test;/users/hcha/index_bench/index/libcuckoo/tests/universal-benchmark/CMakeLists.txt;0;")
add_test(pure_erase "/users/hcha/index_bench/index/libcuckoo/build/tests/universal-benchmark/universal_benchmark" "--erases" "100" "--prefill" "75" "--total-ops" "75" "--initial-capacity" "23")
set_tests_properties(pure_erase PROPERTIES  _BACKTRACE_TRIPLES "/users/hcha/index_bench/index/libcuckoo/tests/universal-benchmark/CMakeLists.txt;29;add_test;/users/hcha/index_bench/index/libcuckoo/tests/universal-benchmark/CMakeLists.txt;0;")
add_test(pure_update "/users/hcha/index_bench/index/libcuckoo/build/tests/universal-benchmark/universal_benchmark" "--updates" "100" "--prefill" "75" "--total-ops" "500" "--initial-capacity" "23")
set_tests_properties(pure_update PROPERTIES  _BACKTRACE_TRIPLES "/users/hcha/index_bench/index/libcuckoo/tests/universal-benchmark/CMakeLists.txt;31;add_test;/users/hcha/index_bench/index/libcuckoo/tests/universal-benchmark/CMakeLists.txt;0;")
add_test(pure_upsert "/users/hcha/index_bench/index/libcuckoo/build/tests/universal-benchmark/universal_benchmark" "--upserts" "100" "--prefill" "25" "--total-ops" "200" "--initial-capacity" "23")
set_tests_properties(pure_upsert PROPERTIES  _BACKTRACE_TRIPLES "/users/hcha/index_bench/index/libcuckoo/tests/universal-benchmark/CMakeLists.txt;33;add_test;/users/hcha/index_bench/index/libcuckoo/tests/universal-benchmark/CMakeLists.txt;0;")
add_test(insert_expansion "/users/hcha/index_bench/index/libcuckoo/build/tests/universal-benchmark/universal_benchmark" "--inserts" "100" "--initial-capacity" "4" "--total-ops" "13107200")
set_tests_properties(insert_expansion PROPERTIES  _BACKTRACE_TRIPLES "/users/hcha/index_bench/index/libcuckoo/tests/universal-benchmark/CMakeLists.txt;36;add_test;/users/hcha/index_bench/index/libcuckoo/tests/universal-benchmark/CMakeLists.txt;0;")
add_test(read_insert_expansion "/users/hcha/index_bench/index/libcuckoo/build/tests/universal-benchmark/universal_benchmark" "--reads" "80" "--inserts" "20" "--initial-capacity" "10" "--total-ops" "4096000")
set_tests_properties(read_insert_expansion PROPERTIES  _BACKTRACE_TRIPLES "/users/hcha/index_bench/index/libcuckoo/tests/universal-benchmark/CMakeLists.txt;38;add_test;/users/hcha/index_bench/index/libcuckoo/tests/universal-benchmark/CMakeLists.txt;0;")
