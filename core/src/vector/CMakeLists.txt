aux_source_directory(${CMAKE_CURRENT_LIST_DIR} VECTOR_LIST)
set(VEC_TARGET vector)

add_library(${VEC_TARGET} SHARED ${VECTOR_LIST})
target_link_libraries(${VEC_TARGET} PRIVATE -Wl,--whole-archive memory -Wl,--no-whole-archive)
target_include_directories(${VEC_TARGET} PUBLIC ${PROJECT_SOURCE_DIR}/src/util)

install(TARGETS ${VEC_TARGET} DESTINATION ${CMAKE_INSTALL_PREFIX})