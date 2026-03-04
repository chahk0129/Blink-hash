# wal/CMakeLists.txt — WAL infrastructure library

file(GLOB WAL_SRC "*.h" "*.cpp")

add_library(blinkhash_wal STATIC ${WAL_SRC})

# WAL headers live in this directory
target_include_directories(blinkhash_wal PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# wal_recovery.cpp includes tree.h → needs lib/ headers
target_include_directories(blinkhash_wal PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../lib)

# Propagate the same feature flags the tree library uses
target_compile_definitions(blinkhash_wal PUBLIC
    ${BLINKHASH_EXTRA_DEFS} -DFINGERPRINT -DSAMPLING -DLINKED -DADAPTATION)

# wal_recovery.cpp → tree.h → Epoche.h → tbb/enumerable_thread_specific.h
find_package(TBB QUIET)
if(TBB_FOUND)
    target_link_libraries(blinkhash_wal TBB::tbb)
else()
    # Fallback: find TBB manually
    find_path(TBB_INCLUDE_DIR tbb/enumerable_thread_specific.h
        HINTS /usr/local/include /opt/homebrew/include /usr/include)
    if(TBB_INCLUDE_DIR)
        target_include_directories(blinkhash_wal PUBLIC ${TBB_INCLUDE_DIR})
    else()
        message(FATAL_ERROR "TBB not found — needed by Epoche.h (via tree.h)")
    endif()
endif()

# Linux: link io_uring if available
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    find_library(URING_LIB uring)
    if(URING_LIB)
        target_link_libraries(blinkhash_wal ${URING_LIB})
        target_compile_definitions(blinkhash_wal PUBLIC HAS_IO_URING)
    endif()
endif()

INSTALL(TARGETS blinkhash_wal
    ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})