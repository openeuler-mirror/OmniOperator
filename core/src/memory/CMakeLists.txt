aux_source_directory(${CMAKE_CURRENT_LIST_DIR} MEM_POOL_LIST)
set(MEM_TARGET memory)
add_library(${MEM_TARGET} ${MEM_POOL_LIST})

# dependent include
target_link_libraries(${MEM_TARGET} PUBLIC cpu_checker)

