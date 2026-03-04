cmake_minimum_required(VERSION 3.11)
project(Blinkhash)

option(USEAVX2 "USEAVX2" ON)

set(CMAKE_CXX_FLAGS "-std=c++17 -pthread -march=native -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new=64")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RELEASE)
endif()

# AVX is x86-only; skip silently on Apple Silicon / ARM
if(USE_AVX2 AND CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx -mavx2")
    add_definitions(-DAVX_128)
endif()

set(TC_MALLOC_LIBRARY tcmalloc tcmalloc_minimal libtcmalloc_minimal.so.4.3.0)
find_library(TC_MALLOC_LIBRARY
        NAMES ${TC_MALLOC_NAMES}
        PATHS /usr/lib/x86_64-linux-gnu /usr/lib64 /lib /usr/lib /usr/local/lib /opt/local/lib
        NO_DEFAULT_PATH
)

# Use system TBB.  On macOS install with: brew install tbb
if(APPLE)
    execute_process(
        COMMAND brew --prefix tbb
        OUTPUT_VARIABLE HOMEBREW_TBB_PREFIX
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
    if(HOMEBREW_TBB_PREFIX)
        list(APPEND CMAKE_PREFIX_PATH "${HOMEBREW_TBB_PREFIX}")
    endif()
endif()
find_package(TBB REQUIRED)

include_directories(${CMAKE_SOURCE_DIR}/lib)
include_directories(${CMAKE_SOURCE_DIR}/wal)
add_subdirectory(lib)
add_subdirectory(wal)
add_subdirectory(test)

