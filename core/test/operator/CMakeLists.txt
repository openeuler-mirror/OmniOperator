aux_source_directory(${CMAKE_CURRENT_LIST_DIR} OP_TESTS_LIST)
set(OP_TEST_TARGET optest)
add_library(${OP_TEST_TARGET} ${OP_TESTS_LIST})

# dependent library
target_link_libraries(${OP_TEST_TARGET} jit jni vector operator memory codegen)
target_include_directories(${OP_TEST_TARGET} PRIVATE ${PROJECT_SOURCE_DIR}/src/operator/algorithm)