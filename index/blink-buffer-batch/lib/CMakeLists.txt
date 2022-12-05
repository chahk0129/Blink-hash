file(GLOB Blink-Buffer-Batch_SRC "*.h" "*.cpp")
  
add_library(blink_buffer_batch STATIC ${Blink-Buffer-Batch_SRC})
target_link_libraries(blink_buffer_batch tcmalloc_minimal TBB::tbb)
INSTALL(TARGETS blink_buffer_batch
        ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})

add_library(blink_buffer_batch_flush STATIC ${Blink-Buffer-Batch_SRC})
target_compile_definitions(blink_buffer_batch_flush PUBLIC -DFLUSH)
target_link_libraries(blink_buffer_batch_flush tcmalloc_minimal TBB::tbb)
INSTALL(TARGETS blink_buffer_batch_flush
        ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR})
