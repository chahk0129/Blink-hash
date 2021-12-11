cmake_minimum_required(VERSION 2.8)

PROJECT(HOT_ROOT)
if( NOT CMAKE_BUILD_TYPE )
    message(STATUS "No build type selected, default to Release")
    set(CMAKE_BUILD_TYPE Release)
else()
    message(STATUS "Build type is set to ${CMAKE_BUILD_TYPE}")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -march=native -mrtm -mcx16 -mavx -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations -Wall -Wextra -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new=64 -DNDEBUG")

execute_process(COMMAND cat /proc/cpuinfo COMMAND grep avx2 OUTPUT_VARIABLE ENABLE_AVX2)
execute_process(COMMAND cat /proc/cpuinfo COMMAND grep bmi2 OUTPUT_VARIABLE ENABLE_BMI2)
execute_process(COMMAND cat /proc/cpuinfo COMMAND grep avx512 OUTPUT_VARIABLE ENABLE_AVX512)

if(ENABLE_AVX2)
    message(STATUS "Looking for avx2 instruction - found")
else()
    message(STATUS "Looking for avx2 instruction - not found")
endif()
if(ENABLE_BMI2)
    message(STATUS "Looking for bmi2 instruction - found")
else()
    message(STATUS "Looking for bmi2 instruction - not found")
endif()
if(ENABLE_AVX512)
    set(CMAKE_CXX_FLAGS "-mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd ${CMAKE_CXX_FLAGS}")
    add_definitions(-DUSE_AVX512)
    message(STATUS "Looking for avx512 instruction - found")
else()
    message(STATUS "Looking for avx512 instruction - not found")
endif()

if(ENABLE_AVX2 AND ENABLE_BMI2)
    set(HOT_PROJECT TRUE)
else()
    set(HOT_PROEJCT FALSE)
    message(FALSE_ERROR "Cannot find avx2 & bmi2 --> cannot run HOT")
endif()

set(TC_MALLOC_LIBRARIES tcmalloc tcmalloc_minimal libtcmalloc_minimal.so.4.3.0 )

find_library(TC_MALLOC_LIBRARY
        NAMES ${TC_MALLOC_NAMES}
        PATHS /usr/lib/x86_64-linux-gnu /usr/lib64 /lib /usr/lib /usr/local/lib /opt/local/lib
        NO_DEFAULT_PATH
)

SET(TBB_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/third-party/tbb")
include(${TBB_ROOT}/cmake/TBBBuild.cmake)
tbb_build(TBB_ROOT ${TBB_ROOT} CONFIG_DIR TBB_DIR MAKE_ARGS)
find_package(TBB REQUIRED)

ADD_SUBDIRECTORY(lib)
ADD_SUBDIRECTORY(src)
