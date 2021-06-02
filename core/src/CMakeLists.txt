add_subdirectory(jit)
add_subdirectory(jni)
add_subdirectory(memory)
add_subdirectory(operator)
add_subdirectory(util)
add_subdirectory(vector)
add_subdirectory(codegen)
add_subdirectory(common)

set(OM_TARGET omruntime)

# link libraries
set(MY_LINK
        jit
        jni
        memory
        operator
        util
        vector
        codegen
        common
        )
# SHARED mode will compile a so file
add_library(${OM_TARGET} SHARED ${CMAKE_CURRENT_LIST_DIR})
#dependent library
target_link_libraries(${OM_TARGET}
        -Wl,--whole-archive
        ${MY_LINK}
        -Wl,--no-whole-archive
        pthread
        stdc++
        dl)

# install libomruntime.so
install(TARGETS ${OM_TARGET} DESTINATION ${CMAKE_INSTALL_PREFIX})
message(STATUS "Install libomruntime.so into ${CMAKE_INSTALL_PREFIX}")

# compile lr file
function(compile_ir SOURCE_FILE)
    # get file name from path
    string(REGEX REPLACE ".+/(.+)\\..*" "\\1" SOURCE_NAME ${SOURCE_FILE})

    # compile ir
    set(COMPILE_OPTION -S -O3 -emit-llvm -fno-discard-value-names)
    execute_process(COMMAND ${CMAKE_CXX_COMPILER} ${COMPILE_OPTION} ${SOURCE_FILE})

    # install ir
    install(FILES ${PROJECT_BINARY_DIR}/${SOURCE_NAME}.ll DESTINATION ${CMAKE_INSTALL_PREFIX}/ir)
    message(STATUS "Install ${PROJECT_BINARY_DIR}/${SOURCE_NAME}.ll into ${CMAKE_INSTALL_PREFIX}/ir")
endfunction()

# add sources file, if you have new source need compile to ir file, pls add it here.
set(SOURCE_FILES ${CMAKE_CURRENT_LIST_DIR}/operator/aggregator/aggregator.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/aggregator/hash_groupby.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/sort/sort.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/pages_index.cpp
        ${CMAKE_CURRENT_LIST_DIR}/memory/memory_pool.cpp
        )
foreach (SOURCE_FILE ${SOURCE_FILES})
    compile_ir(${SOURCE_FILE})
endforeach ()