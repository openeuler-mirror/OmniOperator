aux_source_directory(${CMAKE_CURRENT_LIST_DIR} VECTOR_LIST)
set(VEC_TARGET vector)

find_package(nlohmann_json 3.9.1 REQUIRED)

add_library(${VEC_TARGET} SHARED ${VECTOR_LIST})
target_link_libraries(${VEC_TARGET} PRIVATE -Wl,--whole-archive memory -Wl,--no-whole-archive securec nlohmann_json::nlohmann_json)
target_include_directories(${VEC_TARGET} PUBLIC ${PROJECT_SOURCE_DIR}/src/util)

install(TARGETS ${VEC_TARGET} DESTINATION ${CMAKE_INSTALL_PREFIX})