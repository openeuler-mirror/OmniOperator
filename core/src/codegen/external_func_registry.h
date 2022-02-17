/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry external function
 */
#ifndef EXTERNAL_FUNC_REGISTRY_H
#define EXTERNAL_FUNC_REGISTRY_H

#include "function.h"
#include "func_registry_base.h"

namespace omniruntime {
    class ExternalFunctionRegistry : public BaseFunctionRegistry {
    public:
        std::vector<Function> GetFunctions() override;
    };
}

#endif