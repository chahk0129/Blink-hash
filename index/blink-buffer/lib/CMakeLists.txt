file(GLOB BlinkBuffer_SRC "*.h" "*.cpp")
  
add_library(blink_buffer STATIC ${BlinkBuffer_SRC})
target_link_libraries(blink_buffer tcmalloc_minimal TBB::tbb)
INSTALL(TARGETS blink_buffer 
        ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})

add_library(blink_buffer_flush STATIC ${BlinkBuffer_SRC})
target_compile_definitions(blink_buffer_flush PUBLIC -DFLUSH)
target_link_libraries(blink_buffer_flush tcmalloc_minimal TBB::tbb)
INSTALL(TARGETS blink_buffer_flush 
        ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})
