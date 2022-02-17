/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Math Functions Registry
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_MATH_H
#define OMNI_RUNTIME_FUNC_REGISTRY_MATH_H
#include "function.h"
#include "func_registry_base.h"

std::vector<omniruntime::Function> GetMathFunctionRegistry();
namespace omniruntime {
    class MathFunctionRegistry : public BaseFunctionRegistry {
    public:
        std::vector<Function> GetFunctions() override;
    };
}

#endif // OMNI_RUNTIME_FUNC_REGISTRY_MATH_H
