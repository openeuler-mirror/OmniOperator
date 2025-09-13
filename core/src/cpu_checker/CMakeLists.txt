aux_source_directory(${CMAKE_CURRENT_LIST_DIR} CPU_CHECKER_LIST)
set(CPU_CHECKER_TARGET cpu_checker)
add_library(${CPU_CHECKER_TARGET} ${CPU_CHECKER_LIST})

# dependent include
target_link_libraries(${CPU_CHECKER_TARGET})

file(GLOB CPU_CHECKER_FILES ${SOURCE_ROOT}/src/cpu_checker/*.h)
install(FILES ${CPU_CHECKER_FILES} DESTINATION ${CMAKE_INSTALL_PREFIX}/include/cpu_checker)
