file(GLOB Blinkhash_SRC "*.h" "*.cpp")
add_library(blinkhash STATIC ${Blinkhash_SRC})

option(SAMPLING "SAMPLING" ON)
option(LINKED "LINKED" ON)
option(ADJUSTMENT "ADJUSTMENT" ON)

if(SAMPLING)
    add_definitions(-DSAMPLING)
endif()
if(LINKED)
    add_definitions(-DLINKED)
endif()
if(ADJUSTMENT)
    add_definitions(-DADJUSTMENT)
endif()

include_directories("../../../")
target_compile_definitions(blinkhash PUBLIC -DADJUSTMENT)
target_link_libraries(blinkhash tcmalloc_minimal)
INSTALL(TARGETS blinkhash 
        ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})


if(ADJUSTMENT)
    remove_definitions(-DADJUSTMENT)
endif()
add_library(blinkhash_baseline STATIC ${Blinkhash_SRC})
target_link_libraries(blinkhash_baseline tcmalloc_minimal)

INSTALL(TARGETS blinkhash_baseline
	ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})
