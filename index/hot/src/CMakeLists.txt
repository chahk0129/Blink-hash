add_library(hot-rowex STATIC wrapper.cpp)
target_link_libraries(hot-rowex hot-rowex-lib content-helpers-lib)
target_include_directories(hot-rowex
	PUBLIC ${CMAKE_SOURCE_DIR}/src
	PUBLIC hot/rowex
	PUBLIC idx/contenthelpers
	)
INSTALL(TARGETS hot-rowex
	ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR}
	PUBLIC_HEADER DESTINATION ${CMAKE_SOURCE_DIR}/include
	)
include_directories("../../../")


add_library(hot-rowex-breakdown STATIC wrapper.cpp)
target_compile_definitions(hot-rowex-breakdown PUBLIC BREAKDOWN=1)
target_link_libraries(hot-rowex-breakdown hot-rowex-lib-breakdown content-helpers-lib)
target_include_directories(hot-rowex-breakdown
	PUBLIC ${CMAKE_SOURCE_DIR}/src
	PUBLIC hot/rowex
	PUBLIC idx/contenthelpers
	)
INSTALL(TARGETS hot-rowex-breakdown
	ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR}
	PUBLIC_HEADER DESTINATION ${CMAKE_SOURCE_DIR}/include
	)
include_directories("../../../")


add_library(hot-rowex-str STATIC wrapper.cpp)
target_compile_definitions(hot-rowex-str PUBLIC STRING_KEY=1)
target_link_libraries(hot-rowex-str hot-rowex-lib content-helpers-lib)
target_include_directories(hot-rowex
	PUBLIC ${CMAKE_SOURCE_DIR}/src
	PUBLIC hot/rowex
	PUBLIC idx/contenthelpers
	)
INSTALL(TARGETS hot-rowex
	ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR}
	PUBLIC_HEADER DESTINATION ${CMAKE_SOURCE_DIR}/include
	)
include_directories("../../../")

