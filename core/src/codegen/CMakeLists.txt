add_subdirectory(functions)

file(GLOB_RECURSE CODE_GEN_LIST ${CMAKE_CURRENT_LIST_DIR}/*.cpp)
set(CODEGEN_TARGET ${OMNI_CODEGEN_SO})
add_library(${CODEGEN_TARGET} SHARED ${CODE_GEN_LIST})

# from command 'llvm-config-12 --cxxflags --ldflags --libs'
target_include_directories(${CODEGEN_TARGET} PUBLIC /usr/lib/llvm-12/include)
target_compile_options(${CODEGEN_TARGET} PRIVATE -std=c++14 -D_GNU_SOURCE -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS)
target_link_libraries(${CODEGEN_TARGET} PRIVATE LLVM-12 securec ${OMNI_VECTOR_SO} util expression udf)
install(TARGETS ${CODEGEN_TARGET} DESTINATION ${CMAKE_INSTALL_PREFIX})