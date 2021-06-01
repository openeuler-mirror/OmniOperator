project(omni-runtime)
set(MY_TARGET omni-runtime)
set(MY_LINK operator util memory jit jni vector)

cmake_minimum_required(VERSION 3.1)
set(CMAKE_CXX_COMPILER "clang++-12")
set(DEFAULT_BUILD_TYPE "Debug")
# set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -g -O0 -Wall -g2 -ggdb")
set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -g -O3 -Wall")
set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -S -emit-llvm -O3 -fno-discard-value-names -Wall")
set(LINK_FLAGS "`llvm-config-12 --cxxflags --ldflags --libs` -rdynamic -Wl,-z,noexecstack -Wl,-z,relro -Wl,-z,now")

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

message("=========================START BUILD======================")

aux_source_directory(. ROOT_DIR)
add_subdirectory(src)

include_directories(inc)
link_directories(lib)

FIND_LIBRARY(JEMALLOC_LIB libjemalloc.so.2 /usr/lib /usr/local/lib NO_DEFAULT_PATH)
link_libraries(${JEMALLOC_LIB})
find_package(LLVM REQUIRED CONFIG)
message(STATUS "FOUND LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "FOUND LLVM ${LLVM_INCLUDE_DIRS}")
include_directories(${LLVM_INCLUDE_DIRS})
add_definitions(${LLVM_DEFINITIONS})
find_library(llvm_libs libLLVMSupport.a libLLVMCore.a libLLVMIRReader.a /usr/lib/llvm-12/lib NO_DEFAULT_PATH)
link_libraries(${llvm_libs})

# set(llvm_libs ${LLVM_AVAILABLE_LIBS})
set(EXTRA_LIBS ${EXTRA_LIBS} ${MY_LINK})
message("========================= START ADD ======================")
enable_testing()
# add_executable(${MY_TARGET} ${ROOT_DIR})
#add_library(${MY_TARGET} SHARED ${ROOT_DIR})

message(STATUS "FOUND LLVM LIB ${llvm_libs}")

#target_link_libraries(${MY_TARGET} PUBLIC ${EXTRA_LIBS} ${llvm_libs} pthread dl stdc++ edit)

add_subdirectory(test/operator)
message("=========================  END ======================")
