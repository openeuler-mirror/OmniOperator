aux_source_directory(${CMAKE_CURRENT_LIST_DIR} JIT_CONTEXT_LIST)
set(JIT_CONTEXT_TARGET jit_context)
add_library(${JIT_CONTEXT_TARGET} ${JIT_CONTEXT_LIST})

target_link_libraries(${JIT_CONTEXT_TARGET} PUBLIC jit)