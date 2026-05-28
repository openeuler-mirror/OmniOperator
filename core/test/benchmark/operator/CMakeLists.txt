option(OMNI_BENCHMARK_ONLY_HASHAGG_NORMALIZEDKEY "only build hash agg normalizedkey benchmark sources" ON) #only for test

if (OMNI_BENCHMARK_ONLY_HASHAGG_NORMALIZEDKEY)
    set(BENCHMARK_OPERATOR_LIST
            ${CMAKE_CURRENT_LIST_DIR}/hash_agg_normalizedkey_serialize.cpp)
    set(BENCHMARK_OPERATOR_COMMON_LIST
            ${CMAKE_CURRENT_LIST_DIR}/common/common.cpp
            ${CMAKE_CURRENT_LIST_DIR}/common/vector_util.cpp)
else ()
    aux_source_directory(${CMAKE_CURRENT_LIST_DIR} BENCHMARK_OPERATOR_LIST)
    aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/common BENCHMARK_OPERATOR_COMMON_LIST)
    aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/tpcds BENCHMARK_OPERATOR_COMMON_LIST)
    list(REMOVE_ITEM BENCHMARK_OPERATOR_LIST ${CMAKE_CURRENT_LIST_DIR}/agg_in_hash_agg.cpp)
endif ()

set(BENCHMARK_OPERATOR_TARGET bmoperator)
find_package(benchmark REQUIRED)
set(OM_BENCHMARK_LINK benchmark::benchmark pthread gtest stdc++ dl)

add_library(${BENCHMARK_OPERATOR_TARGET} ${BENCHMARK_OPERATOR_LIST} ${BENCHMARK_OPERATOR_COMMON_LIST})

target_link_libraries(${BENCHMARK_OPERATOR_TARGET}
        ${OM_BENCHMARK_LINK}
        memory
        util
        type
        LLVM-15
        ${OMNI_OPERATOR_SO}
        ${OMNI_CODEGEN_SO}
        ${OMNI_VECTOR_SO}
        )

if (NOT OMNI_BENCHMARK_ONLY_HASHAGG_NORMALIZEDKEY)
    add_executable(groupby_hashmap_benchmark groupby_hashmap.cpp)
    target_link_libraries(groupby_hashmap_benchmark
            type
            benchmark::benchmark
            memory
            util
            pthread
    )
endif ()

add_executable(hash_agg_normalizedkey_serialize_benchmark
        ${CMAKE_CURRENT_LIST_DIR}/../omni_benchmark.cpp
        ${CMAKE_CURRENT_LIST_DIR}/common/common.cpp
        ${CMAKE_CURRENT_LIST_DIR}/common/vector_util.cpp
        hash_agg_normalizedkey_serialize.cpp)
target_link_libraries(hash_agg_normalizedkey_serialize_benchmark
        ${OM_BENCHMARK_LINK}
        memory
        util
        type
        LLVM-15
        ${OMNI_OPERATOR_SO}
        ${OMNI_CODEGEN_SO}
        ${OMNI_VECTOR_SO})