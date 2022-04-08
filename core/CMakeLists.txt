#project name
project(omni-runtime)

# required cmake version
cmake_minimum_required(VERSION 3.10)

message(STATUS "Now processor is ${CMAKE_HOST_SYSTEM_PROCESSOR}")
# omniruntime version
set(OMNI_RUNTIME_VERSION 1.0.0)
set(OS_ARCHITECTURES aarch64)
set(OMNI_CODEGEN_SO boostkit-omniop-codegen-${OMNI_RUNTIME_VERSION}-${OS_ARCHITECTURES})
set(OMNI_OPERATOR_SO boostkit-omniop-operator-${OMNI_RUNTIME_VERSION}-${OS_ARCHITECTURES})
set(OMNI_RUNTIME_SO boostkit-omniop-runtime-${OMNI_RUNTIME_VERSION}-${OS_ARCHITECTURES})
set(OMNI_VECTOR_SO boostkit-omniop-vector-${OMNI_RUNTIME_VERSION}-${OS_ARCHITECTURES})

# configure cmake
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_COMPILER "g++")

# this is used to generate LLVM IR for jitting
set(IR_COMPILER "clang++-12")

set(CMAKE_CXX_FLAGS_DEBUG "-O0 -pipe -g -Wall -fPIC -fno-omit-frame-pointer -fstack-check -s")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -pipe -Wall -Wtrampolines -D_FORTIFY_SOURCE=2 -O2 -fPIC -finline-functions -fstack-protector-strong -s -Wl,-z,noexecstack -Wl,-z,relro,-z,now -Wl,--disable-new-dtags,--rpath,/usr/local/lib/ -Wl,--enable-new-dtags,--rpath,/usr/local/lib/")

set(CMAKE_VERBOSE_MAKEFILE OFF)

if (EXISTS $ENV{OMNI_HOME})
    set(CMAKE_INSTALL_PREFIX $ENV{OMNI_HOME}/lib)
else ()
    set(CMAKE_INSTALL_PREFIX /opt/lib)
endif ()

message(STATUS "Build by ${CMAKE_BUILD_TYPE}")

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
include_directories(SYSTEM src)

# compile library
add_subdirectory(src)

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