#project name
project(omni-runtime)

# required cmake version
cmake_minimum_required(VERSION 3.10)

# configure cmake
set(CMAKE_CXX_COMPILER "clang++-12")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fPIC")
set(CMAKE_CXX_FLAGS_RELEASE "-g -O2 -fPIC -finline-functions")
set(CMAKE_VERBOSE_MAKEFILE OFF)
set(CMAKE_INSTALL_PREFIX /opt/lib)
message(STATUS "Build by ${CMAKE_BUILD_TYPE}")

# clean all target
set(target_generated ${CMAKE_INSTALL_PREFIX}/libomruntime.so
	             ${CMAKE_INSTALL_PREFIX}/libvector.so
	             ${CMAKE_INSTALL_PREFIX}/libsecurec.so
	             ${CMAKE_INSTALL_PREFIX}/ir
)
foreach(file ${target_generated})
       	if (EXISTS ${file})
	       	file(REMOVE_RECURSE ${file})
		message(STATUS "Delete file or directory:${file}")
        endif()
endforeach(file)
	    
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
option(DEBUG_OPERATOR "Using for debug into native operator code" OFF)
message(STATUS "Option DEBUG_OPERATOR: ${DEBUG_OPERATOR}")
option(DEBUG_LEVEL_LOW "Low debug level" OFF)
message(STATUS "Option DEBUG_LEVEL_LOW: ${DEBUG_LEVEL_LOW}")
option(DEBUG_LEVEL_HIGH "High debug level" OFF)
message(STATUS "Option DEBUG_LEVEL_HIGH: ${DEBUG_LEVEL_HIGH}")
option(DEBUG "Debug" OFF)
message(STATUS "Option DEBUG: ${DEBUG}")
