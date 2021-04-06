# set(SOURCE_LINK data operator util memory)
aux_source_directory(. API_LIST)

include_directories($ENV{JAVA_HOME}/include)
include_directories($ENV{JAVA_HOME}/include/linux)
include_directories(/usr/include/llvm-12)
include_directories(/usr/include/llvm-c-12)
add_library(api ${API_LIST})