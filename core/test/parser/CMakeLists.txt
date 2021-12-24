aux_source_directory(${CMAKE_CURRENT_LIST_DIR} PARSER_TESTS_LIST)
set(PARSER_TEST_TARGET parsertest)
add_library(${PARSER_TEST_TARGET} ${PARSER_TESTS_LIST})

# dependent library
target_link_libraries(${PARSER_TEST_TARGET} jit jni omni_vector operator memory codegen util)
target_include_directories(${PARSER_TEST_TARGET} PRIVATE ${PROJECT_SOURCE_DIR}/src/common/parser
        {PROJECT_SOURCE_DIR}/src/common/jsonparser)
