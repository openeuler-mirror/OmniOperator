aux_source_directory(${CMAKE_CURRENT_LIST_DIR} LLVM_TESTS_LIST)
set(LLVM_TEST_TARGET llvmtest)
add_library(${LLVM_TEST_TARGET} ${LLVM_TESTS_LIST})

# dependent library
target_link_libraries(${LLVM_TEST_TARGET} jit)
target_include_directories(${LLVM_TEST_TARGET} PRIVATE ${PROJECT_SOURCE_DIR}/src/jit/compiler)