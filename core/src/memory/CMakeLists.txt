aux_source_directory(${CMAKE_CURRENT_LIST_DIR} MEM_POOL_LIST)
set(MEM_TARGET memory)
add_library(${MEM_TARGET} ${MEM_POOL_LIST})

# dependent include
target_include_directories(${MEM_TARGET} PUBLIC /usr/local/include/jemalloc/)
target_link_libraries(${MEM_TARGET} PUBLIC jemalloc cpu_checker)

