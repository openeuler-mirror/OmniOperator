aux_source_directory(${CMAKE_CURRENT_LIST_DIR} PARSER_TESTS_LIST)
set(PARSER_TEST_TARGET parsertest)
add_library(${PARSER_TEST_TARGET} ${PARSER_TESTS_LIST})

# dependent library
target_link_libraries(${PARSER_TEST_TARGET} memory util ${OMNI_CODEGEN_SO} ${OMNI_OPERATOR_SO} ${OMNI_VECTOR_SO})
target_include_directories(${PARSER_TEST_TARGET} PRIVATE ${SOURCE_ROOT}/src/common/parser
        ${SOURCE_ROOT}/src/common/jsonparser)
