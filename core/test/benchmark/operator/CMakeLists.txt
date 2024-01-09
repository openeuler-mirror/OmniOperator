aux_source_directory(${CMAKE_CURRENT_LIST_DIR} BENCHMARK_OPERATOR_LIST)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/common BENCHMARK_OPERATOR_COMMON_LIST)
aux_source_directory(${CMAKE_CURRENT_LIST_DIR}/tpcds BENCHMARK_OPERATOR_COMMON_LIST)
list(REMOVE_ITEM BENCHMARK_OPERATOR_LIST ${CMAKE_CURRENT_LIST_DIR}/agg_in_hash_agg.cpp)

set(BENCHMARK_OPERATOR_TARGET bmoperator)

add_library(${BENCHMARK_OPERATOR_TARGET} ${BENCHMARK_OPERATOR_LIST} ${BENCHMARK_OPERATOR_COMMON_LIST})

target_link_libraries(${BENCHMARK_OPERATOR_TARGET}
        ${BM_LINK}
        memory
        util
        type
        LLVM-15
        ${OMNI_OPERATOR_SO}
        ${OMNI_CODEGEN_SO}
        ${OMNI_VECTOR_SO}
        )

add_executable(groupby_hashmap_benchmark groupby_hashmap.cpp)
find_package(benchmark REQUIRED)
target_link_libraries(groupby_hashmap_benchmark
        type
        benchmark::benchmark
        memory
        util
        pthread
)