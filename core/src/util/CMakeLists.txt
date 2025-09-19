file(GLOB_RECURSE UTIL_LIST ${CMAKE_CURRENT_LIST_DIR}/*cpp)
add_library(util ${UTIL_LIST})
# dependent include
target_include_directories(util PUBLIC $ENV{JAVA_HOME}/include)
target_include_directories(util PUBLIC $ENV{JAVA_HOME}/include/linux)
