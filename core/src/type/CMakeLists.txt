file(GLOB_RECURSE TYPE_LIST ${CMAKE_CURRENT_LIST_DIR}/*.cpp)
set(TYPE_TARGET type)
add_library(${TYPE_TARGET} ${TYPE_LIST})

find_package(nlohmann_json 3.7.3 REQUIRED)

add_subdirectory(tz)
add_subdirectory(tzdb)

# dependent include
target_link_libraries(${TYPE_TARGET} PUBLIC nlohmann_json::nlohmann_json)
