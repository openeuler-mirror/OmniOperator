file(GLOB_RECURSE OPERATOR_LIST ${CMAKE_CURRENT_LIST_DIR}/*.cpp ../simd/*.cpp util/*.cpp ../compute/*.cpp ../plannode/*.cpp)

set(OP_TARGET ${OMNI_OPERATOR_SO})
find_package(nlohmann_json 3.7.3 REQUIRED)
# compile .a file

add_library(${OP_TARGET} SHARED ${OPERATOR_LIST})

#dependent library
target_link_libraries(${OP_TARGET} PUBLIC expression ${OMNI_CODEGEN_SO} ${OMNI_VECTOR_SO})
target_include_directories(${OP_TARGET} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/simd)
install(TARGETS ${OP_TARGET} DESTINATION ${CMAKE_INSTALL_PREFIX})
install(FILES ${SOURCE_ROOT}/src/operator/operator.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/operator)
install(FILES ${SOURCE_ROOT}/src/operator/operator_factory.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/operator)
install(FILES ${SOURCE_ROOT}/src/operator/execution_context.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/operator)
install(FILES ${SOURCE_ROOT}/src/operator/memory_builder.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/operator)
install(FILES ${SOURCE_ROOT}/src/operator/status.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/operator)
install(FILES ${SOURCE_ROOT}/src/operator/config/operator_config.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/operator/config)
file(GLOB METRICS_HEAD_FILES ${SOURCE_ROOT}/src/metrics/*.h)
install(FILES ${METRICS_HEAD_FILES} DESTINATION ${CMAKE_INSTALL_PREFIX}/include/metrics)