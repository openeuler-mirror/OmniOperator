# required cmake version
cmake_minimum_required(VERSION 3.14.1)

#project name
project(omni-runtime)

message(STATUS "Now processor is ${CMAKE_HOST_SYSTEM_PROCESSOR}")

# omniruntime version
set(OMNI_RUNTIME_VERSION 1.9.0)
set(OS_ARCHITECTURES aarch64)
set(OMNI_CODEGEN_SO boostkit-omniop-codegen-${OMNI_RUNTIME_VERSION}-${OS_ARCHITECTURES})
set(OMNI_OPERATOR_SO boostkit-omniop-operator-${OMNI_RUNTIME_VERSION}-${OS_ARCHITECTURES})
set(OMNI_RUNTIME_SO boostkit-omniop-runtime-${OMNI_RUNTIME_VERSION}-${OS_ARCHITECTURES})
set(OMNI_VECTOR_SO boostkit-omniop-vector-${OMNI_RUNTIME_VERSION}-${OS_ARCHITECTURES})

# configure cmake
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_COMPILER "g++")

# this is used to generate LLVM IR for jitting
set(IR_COMPILER "clang++-15")
message(STATUS "IR_COMPILER : ${IR_COMPILER}")

if (DEFINED COVERAGE)
    if (${COVERAGE} STREQUAL "ON")
        set(DEBUG_COMPILE_LEVEL "-O2 -fsanitize=address -fsanitize-recover=address,all -static-libasan")
        if (DEFINED ENABLE_DT AND ENABLE_DT STREQUAL "ON")
            set(DEBUG_COMPILE_LEVEL "-fsanitize=address -fsanitize-coverage=trace-pc")
        endif ()
    else ()
        set(DEBUG_COMPILE_LEVEL "-O0")
    endif ()
elseif(DEFINED TRACE)
    set(DEBUG_COMPILE_LEVEL "-O3 -s")
else ()
    set(DEBUG_COMPILE_LEVEL "-O0")
endif ()

find_program(CCACHE_FOUND ccache)
if (CCACHE_FOUND)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif (CCACHE_FOUND)

set(SIMD_COMPILE_LEVEL "-march=armv8-a+crc+sve -fpermissive")

#-ffast-math
#-fopt-info-vec-optimized
set(CMAKE_CXX_FLAGS_DEBUG "${DEBUG_COMPILE_LEVEL} ${SIMD_COMPILE_LEVEL} -fno-var-tracking-assignments -pipe -g -Wall -fPIC -fno-omit-frame-pointer -fno-common -fno-stack-protector -ftest-coverage -fprofile-arcs")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 ${SIMD_COMPILE_LEVEL} -fno-var-tracking-assignments -pipe -Wall -Wtrampolines -D_FORTIFY_SOURCE=2 -fPIC -finline-functions -fstack-protector-strong -s -Wl,-z,noexecstack -Wl,-z,relro,-z,now")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -finline-limit=6000 --param inline-unit-growth=300")

if(ENABLE_COMPILE_TIME_REPORT)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ftime-report")
endif()

set(CMAKE_VERBOSE_MAKEFILE OFF)

if (EXISTS $ENV{OMNI_HOME})
    set(CMAKE_INSTALL_PREFIX $ENV{OMNI_HOME}/lib)
else ()
    set(CMAKE_INSTALL_PREFIX /opt/lib)
endif ()

message(STATUS "Build by ${CMAKE_BUILD_TYPE}")
message(STATUS "OMNI_HOME is $ENV{OMNI_HOME}")
message(STATUS "JAVA_HOME is $ENV{JAVA_HOME}")

# clean all target
set(target_generated ${CMAKE_INSTALL_PREFIX}/lib${OMNI_RUNTIME_SO}.so
        ${CMAKE_INSTALL_PREFIX}/lib${OMNI_VECTOR_SO}.so
        ${CMAKE_INSTALL_PREFIX}/lib${OMNI_CODEGEN_SO}.so
        ${CMAKE_INSTALL_PREFIX}/ir
        ${CMAKE_INSTALL_PREFIX}/lib${OMNI_OPERATOR_SO}.so
        )
foreach (file ${target_generated})
    if (EXISTS ${file})
        file(REMOVE_RECURSE ${file})
        message(STATUS "Delete file or directory:${file}")
    endif ()
endforeach (file)

aux_source_directory(${CMAKE_CURRENT_LIST_DIR} ROOT_SRCS)
# for header searching
include_directories(SYSTEM core/src)
include_directories(SYSTEM core)
include_directories(${CMAKE_INSTALL_PREFIX}/lib/APL/include)

# compile library
add_subdirectory(core)

add_subdirectory(bindings/java/src/main/cpp EXCLUDE_FROM_ALL)


# options
option(DEBUG "Debug" OFF)
message(STATUS "Option DEBUG: ${DEBUG}")

option(DEBUG "Trace" OFF)
message(STATUS "Option TRACE: ${TRACE}")

option(DEBUG_OPERATOR "Using for debug into native operator code" OFF)
message(STATUS "Option DEBUG_OPERATOR: ${DEBUG_OPERATOR}")

option(DEBUG_VECTOR "Using for debug into native vector code" OFF)
message(STATUS "Option DEBUG_VECTOR: ${DEBUG_VECTOR}")

option(DEBUG_LLVM "Using for debug into JIT and codegen code" OFF)
message(STATUS "Option DEBUG_LLVM: ${DEBUG_LLVM}")

option(DISABLE_JIT "Disable OmniJit to optimize operator" OFF)
message(STATUS "Option DISABLE_JIT: ${DISABLE_JIT}")

option(COVERAGE "Enable coverage to optimize operator" OFF)
message(STATUS "Option Enable COVERAGE: ${COVERAGE}")

option(COVERAGE "Disable CPU checker to optimize operator" OFF)
message(STATUS "Option Disable CPU checker: ${DISABLE_CPU_CHECKER}")

option(COVERAGE "Enable DT" OFF)
message(STATUS "Option Enable DT: ${ENABLE_DT}")

option(ENABLE_BENCHMARK "Enable Benchmark" OFF)
message(STATUS "Option Enable Benchmark: ${ENABLE_BENCHMARK}")
