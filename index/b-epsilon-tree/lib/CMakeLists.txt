file(GLOB BETREE_SRC "*.h" "*.cpp")
add_library(betree STATIC ${BETREE_SRC})
target_link_libraries(betree tcmalloc_minimal)
INSTALL(TARGETS betree
	ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})
