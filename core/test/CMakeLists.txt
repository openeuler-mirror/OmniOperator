aux_source_directory(${CMAKE_CURRENT_LIST_DIR} TEST_ROOT_SRCS)

if (DEFINED ENABLE_DT AND ENABLE_DT STREQUAL "ON")
    add_subdirectory(dt)
else ()
    # configure
    add_subdirectory(operator)
    add_subdirectory(util)
    add_subdirectory(vector)
    add_subdirectory(memory)
    add_subdirectory(benchmark)
    add_subdirectory(parser)
    add_subdirectory(type)
    add_subdirectory(codegen)

    set(OM_TEST_TARGET omtest)
    set(MY_LINK
            optest
            testutil
            vectest
            memtest
            benchtest
            parsertest
            typetest
            codegentest
            )

    # find gtest package
    find_package(GTest REQUIRED)

    # compile a executable file
    add_executable(${OM_TEST_TARGET} ${ROOT_SRCS} ${TEST_ROOT_SRCS})
    # dependent libraries
    target_link_libraries(${OM_TEST_TARGET}
            ${GTEST_BOTH_LIBRARIES}
            ${SOURCE_LINK}
            -Wl,--whole-archive
            ${MY_LINK}
            -Wl,--no-whole-archive
            gtest
            gmock
            pthread
            stdc++
            dl
            LLVM-15)
    target_compile_options(${OM_TEST_TARGET} PUBLIC -g -O2 -fPIC)

    # dependent include
    target_include_directories(${OM_TEST_TARGET} PRIVATE {GTEST_INCLUDE_DIRS})

    # discover tests
    gtest_discover_tests(${OM_TEST_TARGET} DISCOVERY_TIMEOUT 600)
endif ()