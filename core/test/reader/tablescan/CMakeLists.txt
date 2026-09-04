set(MAIN_PATH ${CMAKE_CURRENT_SOURCE_DIR})
configure_file(scan_test.h.in ${CMAKE_CURRENT_SOURCE_DIR}/scan_test.h)

aux_source_directory(${CMAKE_CURRENT_LIST_DIR} SCAN_TESTS_LIST)
if (NOT STRINGVIEW_ENABLE)
    list(REMOVE_ITEM SCAN_TESTS_LIST
        ${CMAKE_CURRENT_LIST_DIR}/orc_string_view_dict_read_test.cpp
        ${CMAKE_CURRENT_LIST_DIR}/orc_string_view_read_test.cpp)
endif ()
set(SCAN_TEST_TARGET tablescantest)
add_library(${SCAN_TEST_TARGET} STATIC ${SCAN_TESTS_LIST})
target_compile_options(${SCAN_TEST_TARGET} PUBLIC )

target_link_libraries(${SCAN_TEST_TARGET} ${OMNI_READER_SO})

target_include_directories(${SCAN_TEST_TARGET} PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(${SCAN_TEST_TARGET} PUBLIC $ENV{JAVA_HOME}/include/linux)
