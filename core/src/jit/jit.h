/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_JIT_H__
#define __OMNI_JIT_H__

#include "compiler/compiler.h"
#include "config.h"
#include "context.h"

#include <vector>
#include <map>
#include <string>

namespace omniruntime {
namespace jit {
enum CompilerType {
    LLVM
};

class Jit {
public:
    explicit Jit(std::vector<Context> contexts, CompilerType compilerType = LLVM);

    ~Jit() {}

    // / Specialize templates with values/stats in Context
    // / return true if specialization succeed
    // / or false if specialization failed
    bool Specialize(const std::vector<Optimization> &optimizations = std::vector<Optimization>(),
        const std::vector<ModuleOptimization> &moduleOptimizations = std::vector<ModuleOptimization>());

    std::vector<std::string> GetAppliedOptimizations();

    uint64_t GetJitedFunction(std::string functionName, bool isNameMangled = false);

private:
    Compiler *compiler;
    Config config;
    std::vector<Context> contexts;
    void InitCompile();
};
}
}

#endif