aux_source_directory(${CMAKE_CURRENT_LIST_DIR} OP_TESTS_LIST)

# test of aggregator will spend too much time especially link stage
SET(ENABLE_AGG_TEST ON)
if(ENABLE_AGG_TEST)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/aggregator OP_AGG_TEST_TARGET)
endif()

set(OP_TEST_TARGET optest)
add_library(${OP_TEST_TARGET} ${OP_TESTS_LIST} ${OP_AGG_TEST_TARGET})

# dependent library
target_link_libraries(${OP_TEST_TARGET} memory type ${OMNI_CODEGEN_SO} ${OMNI_OPERATOR_SO} ${OMNI_VECTOR_SO})
target_include_directories(${OP_TEST_TARGET} PRIVATE ${SOURCE_ROOT}/src/util ${SOURCE_ROOT}/src/operator/algorithm ${SOURCE_ROOT}/test)
