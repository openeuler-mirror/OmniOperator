file(GLOB_RECURSE OPERATOR_LIST ${CMAKE_CURRENT_LIST_DIR}/*.cpp)
set(OP_TARGET operator)
# compile .a file
add_library(${OP_TARGET} SHARED ${OPERATOR_LIST})

#dependent library
target_link_libraries(${OP_TARGET} PUBLIC codegen expression omni_vector)

install(TARGETS ${OP_TARGET} DESTINATION ${CMAKE_INSTALL_PREFIX})