aux_source_directory(${CMAKE_CURRENT_LIST_DIR} TYPE_LIST)
set(TYPE_TARGET type)
add_library(${TYPE_TARGET} ${TYPE_LIST})

find_package(nlohmann_json 3.7.3 REQUIRED)

# dependent include
target_link_libraries(${TYPE_TARGET} PUBLIC nlohmann_json::nlohmann_json)
