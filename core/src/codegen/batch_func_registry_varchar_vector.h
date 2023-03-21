/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Batch varchar vector Function Registry
 */

#ifndef OMNI_RUNTIME_BATCH_FUNC_REGISTRY_VARCHAR_VECTOR_H
#define OMNI_RUNTIME_BATCH_FUNC_REGISTRY_VARCHAR_VECTOR_H
#include "function.h"
#include "func_registry_base.h"

namespace omniruntime::codegen {
class BatchVarcharVectorFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};
}
#endif // OMNI_RUNTIME_BATCH_FUNC_REGISTRY_VARCHAR_VECTOR_H
