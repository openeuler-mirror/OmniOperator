aux_source_directory(${CMAKE_CURRENT_LIST_DIR} UTIL_LIST)
add_library(util ${UTIL_LIST} type_util.cpp)
