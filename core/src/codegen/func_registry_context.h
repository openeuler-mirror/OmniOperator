/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Context Helper Function Registry
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_CONTEXT_H
#define OMNI_RUNTIME_FUNC_REGISTRY_CONTEXT_H
#include "function.h"
#include "func_registry_base.h"

namespace omniruntime::codegen {
class ContextFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};
}

#endif // OMNI_RUNTIME_FUNC_REGISTRY_CONTEXT_H
