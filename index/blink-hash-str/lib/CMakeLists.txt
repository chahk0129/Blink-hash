file(GLOB Blinkhash_SRC "*.h" "*.cpp")

add_library(blinkhash STATIC ${Blinkhash_SRC})
include_directories("../../../")
target_compile_definitions(blinkhash PUBLIC -DAVX2 -DFINGERPRINT -DSAMPLING -DLINKED -DADAPTATION -DCONVERT_LOCK)
target_link_libraries(blinkhash tcmalloc_minimal TBB::tbb)
INSTALL(TARGETS blinkhash 
	ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})
