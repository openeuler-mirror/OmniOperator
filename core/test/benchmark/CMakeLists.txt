aux_source_directory(${CMAKE_CURRENT_LIST_DIR} BENCHMARK_ROOT_SRCS)

add_subdirectory(codegen)
add_subdirectory(operator)

set(OM_BENCHMARK_TARGET ombenchmark)

find_package(benchmark REQUIRED)

set(BM_LINK benchmark::benchmark pthread gtest stdc++ dl)

function (omni_discover_benchmark path_for_discover output_variable)

    list(APPEND BENCHMARK_FILES "")
    list(APPEND BENCHMARK_FILES "${${output_variable}}")

    file(GLOB_RECURSE FILE_LIST ${path_for_discover} "*.cpp" "*.h" )
    foreach(FILE_PATH IN LISTS FILE_LIST)
        file(READ "${FILE_PATH}" FILE_CONTENT)

        string(REGEX MATCHALL "OMNI_BENCHMARK_DECLARE[-_0-9a-zA-Z]*\\s*\\(" BENCHMARK_LINES "${FILE_CONTENT}")
        set(FILE_SOURCE_HAS_BENCHMARK false)
        foreach(HIT  ${BENCHMARK_LINES})
            set(FILE_SOURCE_HAS_BENCHMARK true)
        endforeach()

        # If to include any new customer benchmarks that do not use the OMNI_BENCHAMRK_DECLARE format, but an original BENCHAMRK_REGISTER_F
        string(REGEX MATCHALL "BENCHMARK_REGISTER_F*\\s*\\(" BENCHMARK_LINES "${FILE_CONTENT}")
        foreach(HIT  ${BENCHMARK_LINES})
            set(FILE_SOURCE_HAS_BENCHMARK true)
        endforeach()


        if("${FILE_SOURCE_HAS_BENCHMARK}" STREQUAL "true")
            string(REPLACE "${CMAKE_CURRENT_LIST_DIR}" "${CMAKE_CURRENT_LIST_DIR}" EXTERNAL_HEADER ${FILE_PATH})
            list(APPEND BENCHMARK_FILES "${EXTERNAL_HEADER}")
        endif()
    endforeach()

    list(REMOVE_DUPLICATES BENCHMARK_FILES)

    set(${output_variable} ${BENCHMARK_FILES} PARENT_SCOPE)

endfunction()

omni_discover_benchmark(operator OMNI_BENCHMARK_AUTO_DISCOVER)

add_executable(${OM_BENCHMARK_TARGET} omni_benchmark.cpp ${OMNI_BENCHMARK_AUTO_DISCOVER})

target_link_libraries(${OM_BENCHMARK_TARGET} ${BM_LINK} bmoperator)

target_compile_options(${OM_BENCHMARK_TARGET} PUBLIC -g -O2 -fPIC)