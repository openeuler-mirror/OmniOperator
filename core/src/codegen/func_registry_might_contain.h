/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: MightContain Function Registry
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_MIGHT_CONTAIN_H
#define OMNI_RUNTIME_FUNC_REGISTRY_MIGHT_CONTAIN_H
#include "function.h"
#include "func_registry_base.h"

namespace omniruntime::codegen {
class MightContainFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};
}

#endif // OMNI_RUNTIME_FUNC_REGISTRY_MIGHT_CONTAIN_H