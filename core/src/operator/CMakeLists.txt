file(GLOB_RECURSE OPERATOR_LIST ${CMAKE_CURRENT_LIST_DIR}/*.cpp)
set(OP_TARGET operator)
# compile .a file
add_library(${OP_TARGET} ${OPERATOR_LIST})

#dependent library
target_link_libraries(${OP_TARGET} PUBLIC jit codegen common omni_vector)
