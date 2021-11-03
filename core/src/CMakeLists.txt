add_subdirectory(jit)
add_subdirectory(jni)
add_subdirectory(memory)
add_subdirectory(operator)
add_subdirectory(util)
add_subdirectory(vector)
add_subdirectory(codegen)
add_subdirectory(common)

set(OM_TARGET omni_runtime)

# link libraries
set(MY_LINK
        jit
        jni
        memory
        operator
        util
        omni_vector
        codegen
        common
        )
# SHARED mode will compile a so file
add_library(${OM_TARGET} SHARED ${CMAKE_CURRENT_LIST_DIR})

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

add_library(aggregator SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/aggregation/aggregator.cpp)
add_library(group_aggregation SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/aggregation/group_aggregation.cpp)
add_library(non_group_aggregation SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/aggregation/non_group_aggregation.cpp)
add_library(sort SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/sort/sort.cpp)
add_library(pages_index SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/pages_index.cpp)
add_library(hash_builder SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/join/hash_builder.cpp)
add_library(join_hash_table SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/join/join_hash_table.cpp)
add_library(lookup_join SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/join/lookup_join.cpp)
add_library(pages_hash_strategy SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/pages_hash_strategy.cpp)
add_library(hash_util SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/hash_util.cpp)
add_library(window SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/window/window.cpp ${CMAKE_CURRENT_LIST_DIR}/operator/window/window_function.cpp)
add_library(window_function SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/window/window_function.cpp)
add_library(window_partition SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/window/window_partition.cpp)
add_library(topn SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/topn/topn.cpp)
add_library(memory_pool SHARED ${CMAKE_CURRENT_LIST_DIR}/memory/memory_pool.cpp)
add_library(projection SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/projection/projection.cpp)
add_library(operator_util SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/util/operator_util.cpp)
add_library(expr_visitor SHARED ${CMAKE_CURRENT_LIST_DIR}/common/expr_visitor.cpp)
add_library(expressions SHARED ${CMAKE_CURRENT_LIST_DIR}/common/expressions.cpp)
add_library(parserhelper SHARED ${CMAKE_CURRENT_LIST_DIR}/common/parserhelper.cpp)
add_library(parser SHARED ${CMAKE_CURRENT_LIST_DIR}/common/parser/parser.cpp)
add_library(sort_expr SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/sort/sort_expr.cpp)
add_library(hash_builder_expr SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/join/hash_builder_expr.cpp)
add_library(lookup_join_expr SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/join/lookup_join_expr.cpp)
add_library(window_expr SHARED ${CMAKE_CURRENT_LIST_DIR}/operator/window/window_expr.cpp)

set(EXTERNAL_FUNCTIONS
        ${CMAKE_CURRENT_LIST_DIR}/codegen/functions/external_func_registry.cpp
        ${CMAKE_CURRENT_LIST_DIR}/codegen/functions/externalfunctions.cpp
        ${CMAKE_CURRENT_LIST_DIR}/codegen/functions/mathfunctions.cpp
        ${CMAKE_CURRENT_LIST_DIR}/codegen/functions/stringfunctions.cpp
        )

add_library(externalfunctions SHARED ${EXTERNAL_FUNCTIONS})
set_target_properties(externalfunctions PROPERTIES
                            PREFIX external
                            OUTPUT_NAME functions)

install(TARGETS
        aggregator
        group_aggregation
        non_group_aggregation
        sort
        pages_index
        hash_builder
        join_hash_table
        lookup_join
        pages_hash_strategy
        hash_util
        window
        window_function
        window_partition
        topn
        memory_pool
        projection
        operator_util
        expr_visitor
        expressions
        parserhelper
        parser
        sort_expr
        hash_builder_expr
        lookup_join_expr
        window_expr
        DESTINATION ${CMAKE_INSTALL_PREFIX}/jit_libs)

install(TARGETS
        externalfunctions
        DESTINATION ${CMAKE_INSTALL_PREFIX})
message(STATUS "Install externalfunctions.so into ${CMAKE_INSTALL_PREFIX}")

# compile lr file
function(compile_ir SOURCE_FILE)
    # get file name from path
    string(REGEX REPLACE ".+/(.+)\\..*" "\\1" SOURCE_NAME ${SOURCE_FILE})

    # compile ir
    set(COMPILE_OPTION -S -O2 -emit-llvm -fno-discard-value-names)
    execute_process(COMMAND ${CMAKE_CXX_COMPILER} ${COMPILE_OPTION} ${SOURCE_FILE} "-I/usr/lib/llvm-12/include")

    # install ir
    install(FILES ${PROJECT_BINARY_DIR}/${SOURCE_NAME}.ll DESTINATION ${CMAKE_INSTALL_PREFIX}/ir)
    message(STATUS "Install ${PROJECT_BINARY_DIR}/${SOURCE_NAME}.ll into ${CMAKE_INSTALL_PREFIX}/ir")
endfunction()

# add sources file, if you have new source need compile to ir file, pls add it here.
set(SOURCE_FILES ${CMAKE_CURRENT_LIST_DIR}/operator/aggregation/aggregator.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/aggregation/group_aggregation.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/aggregation/non_group_aggregation.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/sort/sort.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/pages_index.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/join/hash_builder.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/join/join_hash_table.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/join/lookup_join.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/pages_hash_strategy.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/hash_util.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/window/window.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/window/window_function.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/window/window_partition.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/topn/topn.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/union/union.cpp
        ${CMAKE_CURRENT_LIST_DIR}/memory/memory_pool.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/projection/projection.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/util/operator_util.cpp
        ${CMAKE_CURRENT_LIST_DIR}/common/expr_visitor.cpp
        ${CMAKE_CURRENT_LIST_DIR}/common/expressions.cpp
        ${CMAKE_CURRENT_LIST_DIR}/common/parserhelper.cpp
        ${CMAKE_CURRENT_LIST_DIR}/common/parser/parser.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/sort/sort_expr.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/join/hash_builder_expr.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/join/lookup_join_expr.cpp
        ${CMAKE_CURRENT_LIST_DIR}/operator/window/window_expr.cpp
        )
foreach (SOURCE_FILE ${SOURCE_FILES})
    compile_ir(${SOURCE_FILE})
endforeach ()
