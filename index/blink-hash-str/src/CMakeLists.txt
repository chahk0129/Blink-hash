add_library(blinkhash STATIC wrapper.cpp)
target_link_libraries(blinkhash-lib)
target_include_directories(blinkhash
	PUBLIC ${CMAKE_SOURCE_DIR}/src
	PUBLIC ${CMAKE_SOURCE_DIR}/lib
	)

INSTALL(TARGETS blinkhash
	ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR}
       )
include_directories("../../../")

