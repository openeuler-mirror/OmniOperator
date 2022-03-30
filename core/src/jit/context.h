/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_JIT_CONTEXT_H__
#define __OMNI_JIT_CONTEXT_H__

#include <map>
#include <string>
#include <vector>
#include "specialization.h"

namespace omniruntime {
namespace jit {
class Context {
public:
    Context(std::string jitTemplate, std::map<std::string, Specialization> specializations)
        : jitTemplate(jitTemplate), specializations(specializations)
    {}

    ~Context() {}

    std::string GetJitTemplate()
    {
        return this->jitTemplate;
    }

    std::map<std::string, Specialization> GetSpecializations()
    {
        return this->specializations;
    }

private:
    std::string jitTemplate;
    std::map<std::string, Specialization> specializations;
};
}
}

#endif