/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Batch Decimal Function Registry
 */

#ifndef OMNI_RUNTIME_BATCH_FUNC_REGISTRY_DECIMAL_H
#define OMNI_RUNTIME_BATCH_FUNC_REGISTRY_DECIMAL_H
#include "function.h"
#include "func_registry_base.h"

namespace omniruntime::codegen {
class BatchDecimalFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class BatchDecimalFunctionRegistryDown : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class BatchDecimalFunctionRegistryHalfUp : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class BatchDecimalFunctionRegistryReScale : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class BatchDecimalFunctionRegistryNotReScale : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};
}

#endif // OMNI_RUNTIME_BATCH_FUNC_REGISTRY_DECIMAL_H
