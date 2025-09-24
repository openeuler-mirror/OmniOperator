aux_source_directory(${CMAKE_CURRENT_LIST_DIR} VECTORIZATION__TESTS_LIST)
set(VECTORIZATION_TEST_TARGET vectorizationTest)
add_library(${VECTORIZATION_TEST_TARGET} ${VECTORIZATION__TESTS_LIST})

# dependent library
target_link_libraries(${VECTORIZATION_TEST_TARGET} vectorization memory util type ${OMNI_OPERATOR_SO} ${OMNI_VECTOR_SO})
target_include_directories(${VECTORIZATION_TEST_TARGET} PUBLIC ${SOURCE_ROOT}/test)
target_include_directories(${VECTORIZATION_TEST_TARGET} PUBLIC ${SOURCE_ROOT}/src/util)