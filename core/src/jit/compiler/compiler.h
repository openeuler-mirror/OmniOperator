/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_JIT_COMPILER_H__
#define __OMNI_JIT_COMPILER_H__

#include <string>
#include <map>
#include "jit/specialization.h"
#include "jit/config.h"

namespace omniruntime {
namespace jit {
class Compiler {
public:
    virtual ~Compiler() = default;

    virtual bool LoadModule(const std::string &templatePath) = 0;

    virtual bool SpecializeAndCompile(const std::vector<Optimization> &optimizations,
        const std::vector<ModuleOptimization> &moduleOptimizations) = 0;

    virtual void AddSpecialization(const std::string &id, const Specialization &specialization) = 0;

    virtual uint64_t GetJitedFunction(const std::string &functionName, bool isNameMangled) = 0;

protected:
    std::map<std::string, Specialization> specializations;
};
}
}

#endif