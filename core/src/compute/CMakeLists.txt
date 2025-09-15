include_directories(${CMAKE_CURRENT_SOURCE_DIR})

file(GLOB COMPUTE_FILES ${SOURCE_ROOT}/src/compute/*.h)
install(FILES ${COMPUTE_FILES} DESTINATION ${CMAKE_INSTALL_PREFIX}/include/compute)
