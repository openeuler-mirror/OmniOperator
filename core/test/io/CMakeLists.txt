aux_source_directory(${CMAKE_CURRENT_LIST_DIR} IO_TESTS_LIST)
set(IO_TESTS_TARGET ioTest)
add_library(${IO_TESTS_TARGET} ${IO_TESTS_LIST})

target_link_libraries(${IO_TESTS_TARGET} io_core)