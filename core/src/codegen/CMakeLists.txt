file(GLOB_RECURSE CODE_GEN_LIST ${CMAKE_CURRENT_LIST_DIR}/*.cpp)
set(CODEGEN_TARGET ${OMNI_CODEGEN_SO})
add_library(${CODEGEN_TARGET} SHARED ${CODE_GEN_LIST})

# from command 'llvm-config-15 --cxxflags --ldflags --libs'
target_include_directories(${CODEGEN_TARGET} PUBLIC /usr/lib/llvm-15/include)
target_compile_options(${CODEGEN_TARGET} PRIVATE -D_GNU_SOURCE -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS)
target_link_libraries(${CODEGEN_TARGET} PRIVATE LLVM-15 ${OMNI_VECTOR_SO} util expression udf)
install(TARGETS ${CODEGEN_TARGET} DESTINATION ${CMAKE_INSTALL_PREFIX})

file(GLOB CODEGEN_HEAD_FILES  ${SOURCE_ROOT}/src/codegen/*.h)
file(GLOB CODEGEN_FUNCTIONS_HEAD_FILES  ${SOURCE_ROOT}/src/codegen/functions/*.h)
install(FILES ${CODEGEN_HEAD_FILES} DESTINATION ${CMAKE_INSTALL_PREFIX}/include/codegen)
install(FILES ${CODEGEN_FUNCTIONS_HEAD_FILES} DESTINATION ${CMAKE_INSTALL_PREFIX}/include/codegen/functions)