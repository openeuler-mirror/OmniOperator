aux_source_directory(${CMAKE_CURRENT_LIST_DIR} SIMD_TESTS_LIST)
set(SIMD_TEST_TARGET simdtest)

add_library(${SIMD_TEST_TARGET} ${SIMD_TESTS_LIST})
