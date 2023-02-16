file(GLOB Blinkhash_SRC "*.h" "*.cpp")

#add_library(baseline STATIC ${Blinkhash_SRC})
#target_link_libraries(baseline TBB::tbb)
##target_link_libraries(baseline tcmalloc_minimal TBB::tbb)
#INSTALL(TARGETS baseline
#	ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})
#
#add_library(fingerprint STATIC ${Blinkhash_SRC})
#target_compile_definitions(fingerprint PUBLIC -DFINGERPRINT)
#target_link_libraries(fingerprint TBB::tbb)
##target_link_libraries(fingerprint tcmalloc_minimal TBB::tbb)
#INSTALL(TARGETS fingerprint 
#	ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})
#
#add_library(simd STATIC ${Blinkhash_SRC})
#target_compile_definitions(fingerprint PUBLIC -DAVX_128 -DFINGERPRINT)
#target_link_libraries(simd TBB::tbb)
##target_link_libraries(simd tcmalloc_minimal TBB::tbb)
#INSTALL(TARGETS simd 
#	ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})
#
#add_library(sampling STATIC ${Blinkhash_SRC})
#target_compile_definitions(sampling PUBLIC -DAVX_128 -DFINGERPRINT -DSAMPLING)
#target_link_libraries(sampling tcmalloc_minimal TBB::tbb)
#INSTALL(TARGETS sampling 
#	ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})
#
#add_library(sampling_debug STATIC ${Blinkhash_SRC})
#target_compile_definitions(sampling_debug PUBLIC -DAVX_128 -DFINGERPRINT -DSAMPLING -DDEBUG)
#target_link_libraries(sampling_debug tcmalloc_minimal TBB::tbb)
#INSTALL(TARGETS sampling_debug
#	ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})
#
#add_library(linked STATIC ${Blinkhash_SRC})
#target_compile_definitions(linked PUBLIC -DAVX_128 -DFINGERPRINT -DSAMPLING -DLINKED)
#target_link_libraries(linked tcmalloc_minimal TBB::tbb)
#INSTALL(TARGETS linked
#	ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})

add_library(adapt STATIC ${Blinkhash_SRC})
target_compile_definitions(adapt PUBLIC -DAVX_128 -DFINGERPRINT -DSAMPLING -DLINKED -DADAPTATION)
target_link_libraries(adapt TBB::tbb)
INSTALL(TARGETS adapt 
	ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})



add_library(blinkhash STATIC ${Blinkhash_SRC})
target_compile_definitions(blinkhash PUBLIC -DAVX_128 -DFINGERPRINT -DSAMPLING -DLINKED -DADAPTATION)
target_link_libraries(blinkhash TBB::tbb)
INSTALL(TARGETS blinkhash 
	ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})
