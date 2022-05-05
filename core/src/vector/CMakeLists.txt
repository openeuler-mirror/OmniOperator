file(GLOB_RECURSE VECTOR_LIST ${CMAKE_CURRENT_LIST_DIR}/*.cpp)
set(VEC_TARGET ${OMNI_VECTOR_SO})

add_library(${VEC_TARGET} SHARED ${VECTOR_LIST})
target_link_libraries(${VEC_TARGET} PRIVATE -Wl,--whole-archive memory type -Wl,--no-whole-archive securec)
target_include_directories(${VEC_TARGET} PUBLIC ${PROJECT_SOURCE_DIR}/src/util)

install(TARGETS ${VEC_TARGET} DESTINATION ${CMAKE_INSTALL_PREFIX})

file(GLOB VECTOR_HEAD_FILES ${PROJECT_SOURCE_DIR}/src/vector/*.h)
install(FILES ${VECTOR_HEAD_FILES} DESTINATION ${CMAKE_INSTALL_PREFIX}/include/vector)
install(FILES ${PROJECT_SOURCE_DIR}/src/memory/chunk.h ${PROJECT_SOURCE_DIR}/src/memory/memory_pool.h ${PROJECT_SOURCE_DIR}/src/memory/memory_statistic.h ${PROJECT_SOURCE_DIR}/src/memory/base_allocator.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/memory)
install(FILES ${PROJECT_SOURCE_DIR}/src/vector/loader/vector_loader.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/vector/loader)
install(FILES ${PROJECT_SOURCE_DIR}/src/vector/tracer/vector_tracer.h ${PROJECT_SOURCE_DIR}/src/vector/tracer/vector_leak_detector.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/vector/tracer)
install(FILES ${PROJECT_SOURCE_DIR}/src/util/debug.h ${PROJECT_SOURCE_DIR}/src/util/global_log.h ${PROJECT_SOURCE_DIR}/src/util/compiler_util.h ${PROJECT_SOURCE_DIR}/src/util/trace_util.h ${PROJECT_SOURCE_DIR}/src/util/omni_exception.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/util)
install(FILES ${PROJECT_SOURCE_DIR}/config.h DESTINATION ${CMAKE_INSTALL_PREFIX}/)
file(GLOB DATA_TYPE_HEAD_FILES ${PROJECT_SOURCE_DIR}/src/type/*.h)
install(FILES ${DATA_TYPE_HEAD_FILES} DESTINATION ${CMAKE_INSTALL_PREFIX}/include/type)