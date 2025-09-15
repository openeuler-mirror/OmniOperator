aux_source_directory(${CMAKE_CURRENT_LIST_DIR} TYPE_TESTS_LIST)
set(TYPE_TEST_TARGET typetest)

add_library(${TYPE_TEST_TARGET} ${TYPE_TESTS_LIST})

# dependent library
target_link_libraries(${TYPE_TEST_TARGET} type)
