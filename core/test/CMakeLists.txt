add_subdirectory(operator)
add_subdirectory(util)
add_subdirectory(vector)
add_subdirectory(llvm)
add_subdirectory(memory)
add_subdirectory(benchmark)
add_subdirectory(parser)

# configure
set(OM_TEST_TARGET omtest)
set(MY_LINK
        optest
        testutil
        vectest
        llvmtest
        memtest
        benchtest
        parsertest
        )

# find gtest package
find_package(GTest REQUIRED)

# compile a executable file
add_executable(${OM_TEST_TARGET} omtest.cpp)
# dependent libraries
target_link_libraries(${OM_TEST_TARGET}
        ${GTEST_BOTH_LIBRARIES}
        ${SOURCE_LINK}
        -Wl,--whole-archive
        ${MY_LINK}
        -Wl,--no-whole-archive
        gtest
        pthread
        stdc++
        dl)
target_compile_options(${OM_TEST_TARGET} PUBLIC -g -O2 -fPIC)

# dependent include
target_include_directories(${OM_TEST_TARGET} PRIVATE {GTEST_INCLUDE_DIRS})

# discover tests
gtest_discover_tests(${OM_TEST_TARGET})
