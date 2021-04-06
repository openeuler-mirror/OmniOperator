set(SOURCE_LINK data operator util)
aux_source_directory(. TESTS_LIST)
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

add_executable(tests ${TESTS_LIST})

target_link_libraries(tests ${GTEST_BOTH_LIBRARIES} ${SOURCE_LINK} pthread harden api operator memory)

gtest_discover_tests(tests)
