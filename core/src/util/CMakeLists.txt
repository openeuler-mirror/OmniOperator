aux_source_directory(${CMAKE_CURRENT_LIST_DIR} UTIL_LIST)
add_library(util ${UTIL_LIST})
# dependent include
target_include_directories(util PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(util PUBLIC $ENV{JAVA_HOME}/include/linux)
target_link_libraries(util PRIVATE securec)
