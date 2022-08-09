/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Batch String Function Registry
 */

#ifndef OMNI_RUNTIME_BATCH_FUNC_REGISTRY_STRING_H
#define OMNI_RUNTIME_BATCH_FUNC_REGISTRY_STRING_H
#include "function.h"
#include "func_registry_base.h"
#include "util/type_util.h"

namespace omniruntime {
    class BatchStringFunctionRegistry : public BaseFunctionRegistry {
    public:
        std::vector<Function> GetFunctions() override;
    };
}
#endif //OMNI_RUNTIME_BATCH_FUNC_REGISTRY_STRING_H
