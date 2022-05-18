aux_source_directory(${CMAKE_CURRENT_LIST_DIR} UTIL_TESTS_LIST)
set(UTIL_TESTS_TARGET testutil)
add_library(${UTIL_TESTS_TARGET} ${UTIL_TESTS_LIST})
target_link_libraries(${UTIL_TESTS_TARGET} PUBLIC jit)