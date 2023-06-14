aux_source_directory(${CMAKE_CURRENT_LIST_DIR} MEMORY_TESTS_LIST)
set(MEMORY_TEST_TARGET memtest)

add_library(${MEMORY_TEST_TARGET} ${MEMORY_TESTS_LIST})

# dependent library
target_link_libraries(${MEMORY_TEST_TARGET} memory benchmark benchmark_main vectest)
target_include_directories(${MEMORY_TEST_TARGET} PRIVATE ${SOURCE_ROOT}/src/memory)
