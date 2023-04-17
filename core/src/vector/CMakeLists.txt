file(GLOB_RECURSE VECTOR_LIST ${CMAKE_CURRENT_LIST_DIR}/*.cpp)
set(VEC_TARGET ${OMNI_VECTOR_SO})

add_library(${VEC_TARGET} SHARED ${VECTOR_LIST})
target_link_libraries(${VEC_TARGET} PRIVATE -Wl,--whole-archive memory type -Wl,--no-whole-archive securec util pthread)
target_include_directories(${VEC_TARGET} PUBLIC ${SOURCE_ROOT}/src/util)

install(TARGETS ${VEC_TARGET} DESTINATION ${CMAKE_INSTALL_PREFIX})

file(GLOB VECTOR_HEAD_FILES ${SOURCE_ROOT}/src/vector/*.h)
install(FILES ${VECTOR_HEAD_FILES} DESTINATION ${CMAKE_INSTALL_PREFIX}/include/vector)
install(FILES ${SOURCE_ROOT}/src/memory/chunk.h ${SOURCE_ROOT}/src/memory/memory_pool.h ${SOURCE_ROOT}/src/memory/memory_statistic.h ${SOURCE_ROOT}/src/memory/base_allocator.h ${SOURCE_ROOT}/src/memory/simple_arena_allocator.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/memory)
install(FILES ${SOURCE_ROOT}/src/vector/loader/vector_loader.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/vector/loader)
install(FILES ${SOURCE_ROOT}/src/vector/tracer/vector_tracer.h ${SOURCE_ROOT}/src/vector/tracer/vector_leak_detector.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/vector/tracer)
install(FILES ${SOURCE_ROOT}/src/util/debug.h ${SOURCE_ROOT}/src/util/global_log.h ${SOURCE_ROOT}/src/util/compiler_util.h ${SOURCE_ROOT}/src/util/trace_util.h ${SOURCE_ROOT}/src/util/omni_exception.h ${SOURCE_ROOT}/src/util/error_code.h ${SOURCE_ROOT}/src/util/bit_map.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/util)
install(FILES ${SOURCE_ROOT}/config.h DESTINATION ${CMAKE_INSTALL_PREFIX}/)
file(GLOB DATA_TYPE_HEAD_FILES ${SOURCE_ROOT}/src/type/*.h)
install(FILES ${DATA_TYPE_HEAD_FILES} DESTINATION ${CMAKE_INSTALL_PREFIX}/include/type)