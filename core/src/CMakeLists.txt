if (DEFINED ENABLE_DT)
    if (${ENABLE_DT} STREQUAL "ON")
        message(STATUS "ENABLE_DT ON: enable -fsanitize=address -fsanitize=undefined -fsanitize-coverage=trace-pc")
        add_compile_options(-fprofile-arcs -ftest-coverage -fdump-rtl-expand)
    endif ()
endif ()

add_subdirectory(memory)
add_subdirectory(operator)
add_subdirectory(util)
add_subdirectory(vector)
add_subdirectory(codegen)
add_subdirectory(expression)
add_subdirectory(type)
add_subdirectory(cpu_checker)
add_subdirectory(udf/cplusplus)

set(OM_TARGET ${OMNI_RUNTIME_SO})

# link libraries
set(MY_LINK
        ${OMNI_CODEGEN_SO}
        ${OMNI_OPERATOR_SO}
        ${OMNI_VECTOR_SO}
        )
# SHARED mode will compile a so file
add_library(${OM_TARGET} SHARED ${CMAKE_CURRENT_LIST_DIR} ${ROOT_SRCS})

include_directories(/usr/lib/llvm-15/include)

#dependent library
target_link_libraries(${OM_TARGET}
        -Wl,--whole-archive
        ${MY_LINK}
        -Wl,--no-whole-archive
        pthread
        stdc++
        dl)

# install libomni_runtime.so
install(TARGETS ${OM_TARGET} DESTINATION ${CMAKE_INSTALL_PREFIX})
message(STATUS "Install libomni_runtime.so into ${CMAKE_INSTALL_PREFIX}")