#project name
project(omni-runtime)

# required cmake version
cmake_minimum_required(VERSION 3.10)

# configure cmake
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_COMPILER "clang++-12")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fPIC -fno-limit-debug-info -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE "-g -O2 -fPIC -finline-functions")
set(CMAKE_VERBOSE_MAKEFILE OFF)

if (EXISTS $ENV{OMNI_HOME})
    set(CMAKE_INSTALL_PREFIX $ENV{OMNI_HOME}/lib)
else ()
    set(CMAKE_INSTALL_PREFIX /opt/lib)
endif ()

message(STATUS "Build by ${CMAKE_BUILD_TYPE}")

# clean all target
set(target_generated ${CMAKE_INSTALL_PREFIX}/libomni_runtime.so
        ${CMAKE_INSTALL_PREFIX}/libomni_vector.so
		${CMAKE_INSTALL_PREFIX}/libcodegen.so
        ${CMAKE_INSTALL_PREFIX}/libsecurec.so
        ${CMAKE_INSTALL_PREFIX}/ir
        ${CMAKE_INSTALL_PREFIX}/externalfunctions.so
        )
foreach (file ${target_generated})
    if (EXISTS ${file})
        file(REMOVE_RECURSE ${file})
        message(STATUS "Delete file or directory:${file}")
    endif ()
endforeach (file)

# compile library
add_subdirectory(src)
add_subdirectory(thirdparty/huawei_secure_c)

# unit test
enable_testing()
add_subdirectory(test)

# configure file
configure_file(
        "${PROJECT_SOURCE_DIR}/config.h.in"
        "${PROJECT_SOURCE_DIR}/config.h"
)

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