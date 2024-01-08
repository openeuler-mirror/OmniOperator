/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: Batch Date Time Function Registry
 */

#ifndef OMNI_RUNTIME_BATCH_FUNC_REGISTRY_DATETIME_H
#define OMNI_RUNTIME_BATCH_FUNC_REGISTRY_DATETIME_H
#include "function.h"
#include "func_registry_base.h"

namespace omniruntime::codegen {
class BatchDateTimeFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};
}
#endif // OMNI_RUNTIME_BATCH_FUNC_REGISTRY_DATETIME_H
