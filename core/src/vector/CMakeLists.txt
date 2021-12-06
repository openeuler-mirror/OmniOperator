file(GLOB_RECURSE VECTOR_LIST ${CMAKE_CURRENT_LIST_DIR}/*.cpp)
set(VEC_TARGET omni_vector)

find_package(nlohmann_json 3.9.1 REQUIRED)

add_library(${VEC_TARGET} SHARED ${VECTOR_LIST})
target_link_libraries(${VEC_TARGET} PRIVATE -Wl,--whole-archive memory -Wl,--no-whole-archive securec nlohmann_json::nlohmann_json)
target_include_directories(${VEC_TARGET} PUBLIC ${PROJECT_SOURCE_DIR}/src/util)

install(TARGETS ${VEC_TARGET} DESTINATION ${CMAKE_INSTALL_PREFIX})

file(GLOB VECTOR_HEAD_FILES ${PROJECT_SOURCE_DIR}/src/vector/*.h)
install(FILES ${VECTOR_HEAD_FILES} DESTINATION ${CMAKE_INSTALL_PREFIX}/include/vector)
install(FILES ${PROJECT_SOURCE_DIR}/src/memory/chunk.h ${PROJECT_SOURCE_DIR}/src/memory/memory_pool.h ${PROJECT_SOURCE_DIR}/src/memory/memory_statistic.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/memory)
install(FILES ${PROJECT_SOURCE_DIR}/src/vector/loader/vector_loader.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/vector/loader)
install(FILES ${PROJECT_SOURCE_DIR}/src/vector/tracer/vector_tracer.h ${PROJECT_SOURCE_DIR}/src/vector/tracer/vector_leak_detector.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/vector/tracer)
install(FILES ${PROJECT_SOURCE_DIR}/src/util/debug.h ${PROJECT_SOURCE_DIR}/src/util/compiler_util.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/util)
install(FILES ${PROJECT_SOURCE_DIR}/config.h DESTINATION ${CMAKE_INSTALL_PREFIX}/)
install(FILES ${PROJECT_SOURCE_DIR}/thirdparty/huawei_secure_c/include/securec.h ${PROJECT_SOURCE_DIR}/thirdparty/huawei_secure_c/include/securectype.h DESTINATION ${CMAKE_INSTALL_PREFIX}/thirdparty/huawei_secure_c/include/)
file(GLOB VECTOR_TYPE_HEAD_FILES ${PROJECT_SOURCE_DIR}/src/vector/type/*.h)
install(FILES ${VECTOR_TYPE_HEAD_FILES} DESTINATION ${CMAKE_INSTALL_PREFIX}/include/vector/type)
