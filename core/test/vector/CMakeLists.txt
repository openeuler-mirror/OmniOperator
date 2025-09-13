aux_source_directory(${CMAKE_CURRENT_LIST_DIR} VEC_TESTS_LIST)
set(VEC_TEST_TARGET vectest)

add_library(${VEC_TEST_TARGET} ${VEC_TESTS_LIST})

# dependent library
target_link_libraries(${VEC_TEST_TARGET} ${OMNI_VECTOR_SO} memory type)
target_include_directories(${VEC_TEST_TARGET} PRIVATE ${SOURCE_ROOT}/src/vector)
target_include_directories(${VEC_TEST_TARGET} PRIVATE ${SOURCE_ROOT}/src/vector/tracer)
target_include_directories(${VEC_TEST_TARGET} PUBLIC ${SOURCE_ROOT}/test)
