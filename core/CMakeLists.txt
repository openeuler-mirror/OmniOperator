set(SOURCE_ROOT ${CMAKE_CURRENT_LIST_DIR})

add_subdirectory(src)

# unit test
if(NOT EXCLUDE_TEST)
    enable_testing()
    add_subdirectory(test)
endif()

# configure file
configure_file(
        "${CMAKE_CURRENT_LIST_DIR}/config.h.in"
        "${CMAKE_CURRENT_LIST_DIR}/config.h"
)
