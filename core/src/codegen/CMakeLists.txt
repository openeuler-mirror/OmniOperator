aux_source_directory(${CMAKE_CURRENT_LIST_DIR} CODE_GEN_LIST)
set(CODEGEN_TARGET codegen)
add_library(${CODEGEN_TARGET} ${CODE_GEN_LIST})

# from command 'llvm-config-12 --cxxflags --ldflags --libs'
target_include_directories(${CODEGEN_TARGET} PUBLIC /usr/lib/llvm-12/include)
target_compile_options(${CODEGEN_TARGET} PRIVATE -std=c++14 -fno-exceptions -D_GNU_SOURCE -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS)
target_link_libraries(${CODEGEN_TARGET} PRIVATE LLVM-12)

# dependent include
target_include_directories(${MEM_TARGET} PUBLIC /usr/local/include/re2/)
target_link_libraries(${MEM_TARGET} PUBLIC re2)
