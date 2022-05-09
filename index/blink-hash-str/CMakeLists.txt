cmake_minimum_required(VERSION 3.11)
project(Blinkhash)

option(USEAVX2 "USEAVX2" ON)
option(USEAVX512 "USEAVX512" OFF)

set(CMAKE_CXX_FLAGS "-std=c++17 -pthread -Wall -Wextra -march=native -mavx2 -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new=64")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RELEASE)
endif()

if(USE_AVX2)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx -mavx2")
    add_definitions(-DAVX2)
elseif(USE_AVX512)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd")
    add_definitions(-DAVX512)
endif()

set(TC_MALLOC_LIBRARY tcmalloc tcmalloc_minimal libtcmalloc_minimal.so.4.3.0)
find_library(TC_MALLOC_LIBRARY
        NAMES ${TC_MALLOC_NAMES}
        PATHS /usr/lib/x86_64-linux-gnu /usr/lib64 /lib /usr/lib /usr/local/lib /opt/local/lib
        NO_DEFAULT_PATH
)

#find_library(TBB tbb)
set(TBB_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../hot/third-party/tbb")
include(${TBB_ROOT}/cmake/TBBBuild.cmake)
tbb_build(TBB_ROOT ${TBB_ROOT} CONFIG_DIR TBB_DIR MAKE_ARGS)
find_package(TBB REQUIRED)

include_directories(${CMAKE_SOURCE_DIR}/lib)
add_subdirectory(lib)
