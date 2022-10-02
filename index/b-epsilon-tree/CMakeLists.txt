project(LSM-tree)
set(CMAKE_CXX_FLAGS "-std=c++17 -pthread -Wall -Wextra -march=native -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new=64")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RELEASE)
endif()

set(TC_MALLOC_LIBRARY tcmalloc tcmalloc_minimal libtcmalloc_minimal.so.4.3.0)
find_library(TC_MALLOC_LIBRARY
        NAMES ${TC_MALLOC_NAMES}
        PATHS /usr/lib/x86_64-linux-gnu /usr/lib64 /lib /usr/lib /usr/local/lib /opt/local/lib
        NO_DEFAULT_PATH
)


include_directories(${CMAKE_SOURCE_DIR}/lib)
add_subdirectory(lib)
add_subdirectory(test)
