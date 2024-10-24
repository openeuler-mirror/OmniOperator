file(GLOB_RECURSE OPERATOR_LIST ${CMAKE_CURRENT_LIST_DIR}/*.cpp)

set(OP_TARGET ${OMNI_OPERATOR_SO})
find_package(nlohmann_json 3.7.3 REQUIRED)
# compile .a file
add_library(${OP_TARGET} SHARED ${OPERATOR_LIST})

#dependent library
target_link_libraries(${OP_TARGET} PUBLIC expression ${OMNI_CODEGEN_SO} ${OMNI_VECTOR_SO})
target_include_directories(${OP_TARGET} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/simd)
install(TARGETS ${OP_TARGET} DESTINATION ${CMAKE_INSTALL_PREFIX})