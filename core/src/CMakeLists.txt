add_subdirectory(jit)
add_subdirectory(jit_context)
add_subdirectory(jni)
add_subdirectory(memory)
add_subdirectory(operator)
add_subdirectory(util)
add_subdirectory(vector)
add_subdirectory(codegen)
add_subdirectory(expression)
add_subdirectory(type)

set(OM_TARGET omni_runtime)

# link libraries
set(MY_LINK
        jit
        jit_context
        jni
        memory
        operator
        util
        omni_vector
        codegen
        expression
        type
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

# compile IR files
set(COMPILE_OPTION -S -O2 -emit-llvm -fno-discard-value-names)
set(LLVM_INCLUDE -I/usr/lib/llvm-12/include)
set(COMPILE_INCLUDE_SOURCE -I${CMAKE_CURRENT_LIST_DIR})

# add sources file, if you have new source need compile to ir file, pls add it here.
set(IR_SOURCE_FILES ${CMAKE_CURRENT_LIST_DIR}/operator/aggregation/group_aggregation.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/aggregation/non_group_aggregation.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/sort/sort.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/pages_index.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/join/hash_builder.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/join/join_hash_table.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/join/lookup_join.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/pages_hash_strategy.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/window/window.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/window/window_function.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/window/window_partition.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/topn/topn.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/union/union.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/sort/sort_expr.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/join/hash_builder_expr.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/join/lookup_join_expr.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/window/window_expr.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/aggregation/group_aggregation_expr.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/topn/topn_expr.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/limit/limit.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/limit/distinct_limit.cpp
        )
set(IR_COMPILE_COMMANDS "")
foreach(IR_SOURCE_FILE ${IR_SOURCE_FILES})
    list(APPEND IR_COMPILE_COMMANDS COMMAND ${IR_COMPILER} ${COMPILE_OPTION} ${IR_SOURCE_FILE} ${LLVM_INCLUDE} ${COMPILE_INCLUDE_SOURCE})
endforeach()
message(STATUS "IRs Compiling...")
execute_process(${IR_COMPILE_COMMANDS})
message(STATUS "IRs Compiled")

# install IR file
file(GLOB_RECURSE LL_FILES ${PROJECT_BINARY_DIR}/*.ll)
install(FILES ${LL_FILES} DESTINATION ${CMAKE_INSTALL_PREFIX}/ir)
