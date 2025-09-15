/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Batch util Function Registry
 */
#ifndef OMNI_RUNTIME_BATCH_FUNC_REGISTRY_UTIL_H
#define OMNI_RUNTIME_BATCH_FUNC_REGISTRY_UTIL_H
#include "function.h"
#include "func_registry_base.h"
#include "util/type_util.h"

namespace omniruntime::codegen {
class BatchUtilFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};
}
#endif // OMNI_RUNTIME_BATCH_FUNC_REGISTRY_UTIL_H
