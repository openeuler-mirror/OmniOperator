aux_source_directory(${CMAKE_CURRENT_LIST_DIR} VEC_TESTS_LIST)
set(VEC_TEST_TARGET vectest)

add_library(${VEC_TEST_TARGET} ${VEC_TESTS_LIST})

# dependent library
target_link_libraries(${VEC_TEST_TARGET} omni_vector memory type)
target_include_directories(${VEC_TEST_TARGET} PRIVATE ${PROJECT_SOURCE_DIR}/src/vector)
target_include_directories(${VEC_TEST_TARGET} PRIVATE ${PROJECT_SOURCE_DIR}/src/vector/tracer)
