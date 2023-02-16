file(GLOB Blinkhash_SRC "*.h" "*.cpp")

add_library(blinkhash STATIC ${Blinkhash_SRC})
include_directories("../../../")
target_compile_definitions(blinkhash PUBLIC -DAVX_128 -DFINGERPRINT -DSAMPLING -DLINKED -DADAPTATION)
target_link_libraries(blinkhash TBB::tbb)
INSTALL(TARGETS blinkhash 
	ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})
