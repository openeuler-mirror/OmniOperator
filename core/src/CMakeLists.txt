add_subdirectory(jni)
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
        jni
        memory
        util
        expression
        type
        udf
        ${OMNI_CODEGEN_SO}
        ${OMNI_OPERATOR_SO}
        ${OMNI_VECTOR_SO}
        )
# SHARED mode will compile a so file
add_library(${OM_TARGET} SHARED ${CMAKE_CURRENT_LIST_DIR} ${ROOT_SRCS})

include_directories(/usr/lib/llvm-12/include)

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
