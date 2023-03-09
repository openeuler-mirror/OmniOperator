/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch math functions registry
 */
#ifndef OMNI_RUNTIME_BATCH_FUNC_REGISTRY_MATH_H
#define OMNI_RUNTIME_BATCH_FUNC_REGISTRY_MATH_H

#include "function.h"
#include "func_registry_base.h"

namespace omniruntime::codegen {
class BatchMathFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class BatchMathFunctionRegistryHalfUp : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class BatchMathFunctionRegistryDown : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};
}

#endif // OMNI_RUNTIME_BATCH_FUNC_REGISTRY_MATH_H
