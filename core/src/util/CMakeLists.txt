aux_source_directory(${CMAKE_CURRENT_LIST_DIR} UTIL_LIST)
add_library(util ${UTIL_LIST})
target_link_libraries(util PRIVATE securec)
