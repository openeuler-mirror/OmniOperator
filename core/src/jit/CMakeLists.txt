set(JIT_LIST
        jit.cpp
        config.cpp
        specialization.cpp
        compiler/llvm_compiler.cpp
        compiler/library_loader.cpp
        compiler/harden_optimizer.cpp)
set(JIT_TARGET jit)
add_library(${JIT_TARGET} ${JIT_LIST})

# from command 'llvm-config-12 --cxxflags --ldflags --libs'
target_include_directories(${JIT_TARGET} PUBLIC /usr/lib/llvm-12/include)
target_compile_options(${JIT_TARGET} PRIVATE -std=c++14 -fno-exceptions -D_GNU_SOURCE -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS)
target_link_libraries(${JIT_TARGET} PRIVATE LLVM-12 -Wl,--no-whole-archive securec)
