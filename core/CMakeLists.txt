set(SOURCE_ROOT ${CMAKE_CURRENT_LIST_DIR})

add_subdirectory(src)

# unit test
enable_testing()
add_subdirectory(test)

# configure file
configure_file(
        "${CMAKE_CURRENT_LIST_DIR}/config.h.in"
        "${CMAKE_CURRENT_LIST_DIR}/config.h"
)
