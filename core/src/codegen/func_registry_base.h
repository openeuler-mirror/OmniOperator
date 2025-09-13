/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry external function
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_BASE_H
#define OMNI_RUNTIME_FUNC_REGISTRY_BASE_H

#include <vector>

namespace omniruntime::codegen {
class BaseFunctionRegistry {
public:
    virtual std::vector<Function> GetFunctions() = 0;
    virtual ~BaseFunctionRegistry() = default;
};
}

#endif // OMNI_RUNTIME_FUNC_REGISTRY_BASE_H